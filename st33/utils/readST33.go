package st33

import (
	"bytes"
	"encoding/binary"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"io"
	"os"
	"strconv"
)

type St33Reader struct {

	File        *os.File
	Size        int64
	Current     int64
	Previous    int64

}


// create a new reader instance
func NewSt33Reader(infile string ) (*St33Reader, error) {

	f,err:= os.Open(infile)
	if err == nil {
		finfo, _ := f.Stat()

		return &St33Reader{
			File:     f,
			Size:     finfo.Size(),
			Previous: 0,
			Current:  0,
		}, err
	} else {
		return nil,err
	}
}


//
// read  b bytes from the current  position
//
func (r *St33Reader) ReadAt(b []byte) (int, error){

	f := r.File
	c := r.GetCurrent()
	return f.ReadAt(b,c)
}


//
// Set position of the current record to read
//
func  (r *St33Reader)  SetCurrent(c int64) {
	r.Current = c
}


//
//   Return the location of the current record
//
func  (r *St33Reader)  GetCurrent() (int64){
	return r.Current
}

//
// Set  the location of the previous record
//
func  (r *St33Reader)  setPrevious(c int64){
	r.Previous = c
}

//
// return the location of the previous  record
//
func  (r *St33Reader)  GetPrevious() (int64){
	return r.Previous
}

//
//   read a ST33 record
//   A record should start with BDW ( 4 bytes) and RDW  ( 4 bytes)
//   BDW -RDW should = 4
//


func (r *St33Reader) Read()  ([]byte,error){

	bdw,rdw,err := r.getBDW()

	gLog.Trace.Println(bdw,rdw)
	// loop until bdw - drw == 4
	for {
		if bdw-rdw == 4 || err == io.EOF {
			break
		} else {
			r.SetCurrent(r.GetCurrent()+1)  // Look for next pair of BDW,RDW
			bdw,rdw,err = r.getBDW()
		}
	}

	// if EOF then return
	if err == io.EOF  {
		return nil,err
	} else {
		b,err := r.getRecord(int(rdw)) 	//  read  rdw bytes  at the position r.Current
		return b,err
	}
}


//
//  return Block descriptor word ( BDW) and Record Descriptor word
//
//   advance the address  by 4 + 4
func (r *St33Reader) getBDW() (uint16,uint16,error) {
	var (
		Big      			binary.ByteOrder = binary.BigEndian
		bdw                 uint16
		rdw                 uint16
	)
	byte:=  make([]byte, 2)
	_,err := r.ReadAt(byte)  // Read BDW ( first 2 bytes )

	if err == io.EOF {
		return 0,0,err
	}

	err = binary.Read(bytes.NewReader(byte), Big, &bdw)
	if bdw > 0 {
		r.SetCurrent(r.GetCurrent() + 4)  // SKIP BDW  ( 4 bytes)
		_, err = r.ReadAt(byte)           // Read RDW ( first 2 bytes)
		err = binary.Read(bytes.NewReader(byte), Big, &rdw)

	} else {
		rdw = 0
	}
	// skip RDW
	r.SetCurrent(r.GetCurrent() + 4)

	return bdw,rdw,err

}


//
//  read n bytes from the current position  position  (r.Current)

func (r *St33Reader)  getRecord(n int) ( []byte,error) {
	n = n -4   //  minus 4 bytes ( rdw length )
	byte:=  make([]byte, n)
	_,err := r.ReadAt(byte)
	r.setPrevious(r.GetCurrent())
	// set current pointer at the begining of the next BDW
	r.SetCurrent(r.GetCurrent()+ int64(n))
	gLog.Trace.Println(len(byte),r.Previous,r.Current)
	// os.Exit(100)
	return byte,err
}




func (r *St33Reader) ReadST33BLOB(v Conval)  {

	var (
		Big	= binary.BigEndian
		blobRecs uint16
		recs int = 0
	)

	buf,err := r.Read()

	if err == nil {
		recs++
		if IsST33Blob(buf,0) {   //  ST33 BLOB Record

			//   Read all the  records
			gLog.Info.Printf("BLOB ST33 - PXI ID: %s - Record number:%d - Buffer length %d",v.PxiId, recs,len(buf))
			_ = binary.Read(bytes.NewReader(buf[84:86]), Big, &blobRecs)

			for rec:= 2; rec <= int(blobRecs); rec ++ {
				if buf,err := r.Read(); err == nil {
					recs++
					gLog.Info.Printf("BLOB ST33 - PXI ID: %s - Record number:%d - Buffer length %d", v.PxiId, recs,len(buf))
				} else if err== io.EOF{
					break
				} else {
					gLog.Error.Printf("%v",err)
				}
			}

			//  Read the other  BLOB records  ( v.Records  )
			for rec := 1; rec <= int(v.Records); rec++ {
				buf,err = r.Read()
				if err == io.EOF {
					break
				}
				gLog.Info.Printf( "PXI Id %s  - Other BLOB record %d at X'%x' -  Buffer length %d",v.PxiId,rec,r.Previous,len(buf))
			}

		} else {  // Regular BLOB record
			gLog.Info.Printf("BLOB - PXI ID: %s - Number of Recs %d  - Record number:%d - Buffer length %d",v.PxiId, v.Records,recs,len(buf))
			for rec:= 2; rec <= v.Records; rec ++ {
				buf,err := r.Read()
				recs++
				gLog.Info.Printf("BLOB - PXI ID: %s - Prev address: X'%x' Cur address: X'%x' - Number of Recs %d - Record number:%d - Buffer length %d",v.PxiId, r.Previous, r.Current, v.Records,recs,len(buf))
				if err == io.EOF {
					break
				}
			}
		}
	} else {
		gLog.Error.Printf("%v",err)
	}

}


func (r *St33Reader) ReadST33Tiff( v Conval) {

	// ST33 records

	// gLog.Info.Printf("ST33 PXIID: %s - Ctrl number of records: %d - Ctrl number of pages: %d",v.PxiId,v.Records,v.Pages)

	var (
		recs      =  0
		pages     = 0
		ST33     []byte
		tiffRecs uint16
		Big	     = binary.BigEndian
		pagenum, numpages int
	)

	// compare the number of pages from the tiff record and the numer of pages from the control file
	// if they differ loop until getting the record that match or EOF
	// if they match, rewind one record  for  processing again
	for {
		if buf,err    := r.Read(); err == nil {
			ST33 = utils.Ebc2asci(buf[0:214])
			pagenum, _ = strconv.Atoi(string(ST33[17:21]))
			numpages, _ = strconv.Atoi(string(ST33[76:80]))

			if numpages == int(v.Pages) {
				//  if   match then rewind to the previous record
				r.SetCurrent(r.GetPrevious() - 8)
				break
			} else {
				gLog.Warning.Printf("PXIID %s - Control file page number %d != Image page nunber %d  at address x'%x'", v.PxiId, v.Pages, numpages, r.GetPrevious())
			}
		} else {
			// should be EOF
			break
		}
	}

	//  Read all the TIFF  Pages
	for p:= 1; p <= int(v.Pages); p++ {

		buf,err := r.Read()

		if err == io.EOF {
			break
		}
		ST33	    = utils.Ebc2asci(buf[0:214])
		pagenum,_ 	= strconv.Atoi(string(ST33[17:21]))
		recs++
		pages++
		gLog.Trace.Printf("TIFF Pages: %d-%d Record number:%d - Buffer length %d",  pages, pagenum, recs, len(buf))
		_ = binary.Read(bytes.NewReader(buf[84:86]), Big, &tiffRecs)

		for rec := 2; rec <= int(tiffRecs); rec ++ {
			recs++
			if buf, err := r.Read(); err == nil {
				gLog.Trace.Printf("TIFF Pages: %d-%d Record number:%d - Buffer length %d",  pages, pagenum, recs, len(buf))
			} else {
				break
			}
		}
	}
	//  check number of records match
	if v.Records != recs {
		gLog.Warning.Printf("PXIID %s - Records number [%d] of the control file != Records number [%d] of the data file ",v.PxiId,v.Records,recs)
		//  try to read the missing records in the data file
		missing := v.Records - recs
		for m:=1; m <= missing; m++ { // SKIP missing records

			buf,err := r.Read()
			if err == nil {
				ST33 = utils.Ebc2asci(buf[0:214])
				pagenum, _ = strconv.Atoi(string(ST33[17:21]))
				gLog.Warning.Printf("PXIID %s - Skip record number %d", v.PxiId, pagenum)
			}  else {
				gLog.Info.Printf("%v",err)
			}
		}
	} else {
		gLog.Info.Printf("PXIID: %s - Number of records: %d - Number of pages: %d ",v.PxiId,recs,pages)
	}

}


