package st33

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"io"
	"os"
	"strconv"
)

type St33Reader struct {

	File        *os.File        // File  -> File descriptor
	Buffer      *bytes.Buffer   // in memory buffer
	Size        int64           // Size of the file
	Type        string          //  type  : RAM , File
	Current     int64           //  Current address of the record to read
	Previous    int64           //  Pointer to previous recod ( BDW,RDW,data)

}


// create a new reader instance
func NewSt33Reader(infile string ) (*St33Reader, error) {

	f,err:= os.Open(infile)

	if err == nil {
		finfo, _ := f.Stat()
		size := finfo.Size()
		fmt.Println(size)
		if size > TwoGB {
			return &St33Reader{
				File:     f,
				Size:     size,
				Type:     ST33FILEReader,
				Previous: 0,
				Current:  0,
			}, err
		} else {
			if buf,err := utils.ReadBuffer(infile); err == nil {
				fmt.Println(ST33RAMReader)
				return &St33Reader{
					File:    f,
					Buffer:   buf,
					Size:     size,
					Type:     ST33RAMReader,
					Previous: 0,
					Current:  0,
				}, err
			} else {
				gLog.Error.Printf("%v",err)
				return nil,err
			}
		}
	} else {
		return nil,err
	}
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
// read  b bytes from the current  position
//
func (r *St33Reader) ReadAt(b []byte) (int, error){

	if r.Type == ST33FILEReader {
		f := r.File
		//	c := r.GetCurrent()
		c := r.Current
		return f.ReadAt(b, c)
	} else {
		return r.Buffer.Read(b)
	}
}

//
//   read a ST33 record
//   A record should start with BDW ( 4 bytes) and RDW  ( 4 bytes)
//   BDW -RDW should = 4
//

func (r *St33Reader) Read()  ([]byte,error){
	_,rdw,err := r.getBDW()
	if err != nil  {
		return nil,err
	} else {
		b,err := r.getRecord(int(rdw)) 	//  read  rdw bytes  at the position r.Current

		return b,err
	}
}

//  special Read
func (r *St33Reader) ReadST33Blob(rdw int)  ([]byte,error){
	r.Current = r.Current + 8        // skip the BDW and RDW
	b,err := r.getRecord(int(rdw)) 	//  read  rdw bytes  at the position r.Current
	gLog.Trace.Printf("rdw %d - buffer length: %d",rdw, len(b))
	return b,err
}

//
//  return Block descriptor word ( BDW) and Record Descriptor word
//
//   advance the address  by 4 + 4

func (r *St33Reader) getBDW() (uint16,uint16,error) {
	var (
		Big binary.ByteOrder = binary.BigEndian
		bdw uint16
		rdw uint16
		err error
	)

	byte := make([]byte, 4)

	seek := r.Current

	_, err = r.ReadAt(byte) // Read BDW ( first 2 bytes )

	if err == nil {
			err = binary.Read(bytes.NewReader(byte), Big, &bdw)

			r.Current += 4
	}
		// read rdw
	_, err = r.ReadAt(byte) // Read RDW ( first 2 bytes)
	err = binary.Read(bytes.NewReader(byte[0:2]), Big, &rdw)

	if err != nil {
		return bdw,rdw,err
	}

	for {
		if bdw-rdw == 4 {
			r.Current += 4 // skip RDW
			break

		} else {


			seek ++
			r.Current = seek        // Bext Byte
			_, err = r.ReadAt(byte)   // READ BDW
			err1 := binary.Read(bytes.NewReader(byte), Big, &bdw)
			r.Current += 4        // READ RDW
			_, err = r.ReadAt(byte)
			err1 = binary.Read(bytes.NewReader(byte), Big, &rdw)

			if err != nil || err1 != nil {
				break
			}
		}

	}

	return bdw,rdw,err

}


//
//  read n bytes from the current position  position  (r.Current)

func (r *St33Reader)  getRecord(n int) ( []byte,error) {

	n = n -4                                   //  minus 4 bytes ( rdw length )
	byte:=  make([]byte, n)
	_,err := r.ReadAt(byte)
	r.Previous = r.Current
	// gLog.Info.Printf("111 x'%x' %d ",r.Current,len(byte))
	r.Current += int64(n)
	// gLog.Info.Printf("222  x'%x' %d ",r.Current,len(byte))
	return byte,err
}




func (r *St33Reader) ReadST33BLOB(v Conval)  {

	var (
		Big	= binary.BigEndian
		blobRecs uint16
		blobL uint32
		imgl       uint16
		recs int = 0
	)

	buf,err := r.Read()

	if err == nil {
		recs++
		docsize := v.DocSize
		bufsize := len(buf)

		if IsST33Blob(buf,0)  {                                       //  ST33 BLOB Record

			err := CheckST33Length(&v,r,buf)
			if err != nil {
				gLog.Error.Println(err)
				gLog.Info.Printf("%s", hex.Dump(buf[0:255]))
			}

			//   Read all the  records


			_   =   binary.Read(bytes.NewReader(buf[84:86]), Big, &blobRecs)
			_	= 	binary.Read(bytes.NewReader(buf[214 : 218]), Big, &blobL)
			_   = 	binary.Read(bytes.NewReader(buf[250 : 252]), Big, &imgl)

			gLog.Info.Printf("BLOB ST33 - PXIID: %s - Record number:%d  - ST33 Blobrecs %d  - Buffer length %d",v.PxiId, recs,blobRecs,len(buf))
			gLog.Info.Printf("%s", hex.Dump(buf[0:255]))

			bufsize -= 252 //  minus header length

			for rec:= 2; rec <= int(blobRecs); rec ++ {
				if buf,err := r.Read(); err == nil {

					err1 := CheckST33Length(&v,r,buf)
					if err1 != nil {

						gLog.Error.Printf("BLOB ST33 %v",err1)
						gLog.Info.Printf("%s", hex.Dump(buf[0:255]))
						os.Exit(100)
					}

					recs++
					bufsize += len(buf)-252   // minus hedaer length
					gLog.Trace.Printf("BLOB ST33 - PXI ID: %s -  #rec %d/  Total #recs %d - Buffer length %d", v.PxiId, rec/recs,len(buf))
				} else if err== io.EOF{
					break
				} else {
					gLog.Error.Printf("%v",err)
				}
			}

			gLog.Info.Printf("BLOB ST33 - PXIID: %s - ST33 record number:%d/ St33 recs: %d - Doc size: %d  Buffer size:%d - Blob size: %d ",v.PxiId, v.Records,recs,docsize,bufsize,blobL)


			//  Read the other  BLOB records  ( v.Records  )
			for rec := 1; rec <= int(v.Records); rec++ {
				buf,err = r.Read()
				if err == io.EOF {
					break
				}
				recs++
				blobL += uint32(len(buf))
				bufsize += len(buf)
				gLog.Info.Printf( "BLOB ST33 PXIId %s  - Other BLOB record %d at X'%x' -  Buffer length %d",v.PxiId,rec,r.Previous,len(buf))
			}
			 gLog.Info.Printf("BLOB ST33 - PXIID: %s -  TOTAL record number:%d/%d - Doc size:%d  Buffer size:%d  blob size: %d ",v.PxiId, v.Records,recs,docsize,bufsize,blobL)
		} else {  // Regular BLOB record

			for rec:= 2; rec <= v.Records; rec ++ {
				if buf,err := r.Read(); err == nil {
					recs++
					bufsize += len(buf)
					gLog.Trace.Printf("BLOB - PXIID: %s - Prev address: X'%x' Cur address: X'%x' - Number of Recs %d - Record number:%d - Buffer length %d", v.PxiId, r.Previous, r.Current, v.Records, recs, len(buf))
				}
				if err == io.EOF {
					break
				}
			}
			gLog.Info.Printf("BLOB - PXIID: %s - Record number:%d/%d - Doc size %d/%d",v.PxiId, v.Records,recs,docsize,bufsize)
			if ( v.Records != recs) {
				gLog.Warning.Printf("BLOB - PXIID: %s - Record number:%d/%d - Doc size %d/%d",v.PxiId, v.Records,recs,docsize,bufsize)
			}
		}
	} else {
		gLog.Fatal.Printf("%v",err)
		os.Exit(100)

	}

}


//

//

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
			if len(buf) > 214 {
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
		// check validity of the ST33 record
		if err == nil && len(buf) > 214 {

			ST33 = utils.Ebc2asci(buf[0:214])
			long, _ := strconv.Atoi(string(ST33[0:5]))

			if long != len(buf) {
				error := fmt.Sprintf("Inavlid ST33 record %s at input byte address X'%x' - Buffer length %d != ST33 record length %d ", v.PxiId, r.Previous, len(buf), long)
				gLog.Error.Println(error)
				gLog.Info.Printf("X'%#x", buf[0:214])
				os.Exit(100)
			}
			pagenum, _ = strconv.Atoi(string(ST33[17:21]))
			recs++
			pages++
			gLog.Trace.Printf("TIFF Pages: %d-%d Record number:%d - Buffer length %d", pages, pagenum, recs, len(buf))
			_ = binary.Read(bytes.NewReader(buf[84:86]), Big, &tiffRecs)

			for rec := 2; rec <= int(tiffRecs); rec ++ {
				recs++
				if buf, err := r.Read(); err == nil {
					gLog.Trace.Printf("TIFF Pages: %d-%d Record number:%d - Buffer length %d", pages, pagenum, recs, len(buf))
				} else {
					break
				}
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
			if err == nil && len(buf)  > 214 {
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


