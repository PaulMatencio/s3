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
    /*
	File        *os.File        // File  -> File descriptor
	Buffer      *bytes.Buffer   // in memory buffer
	Size        int64           // Size of the file
	Type        string          //  type  : RAM , File
	Current     int64           //  Current address of the record to read
	Previous    int64           //  Pointer to previous recod ( BDW,RDW,data)
    */

	*utils.VBRecord
}


// create a new reader instance
/*
func NewSt33Reader(infile string ) (*St33Reader, error) {
	f,err:= os.Open(infile)
	if err == nil {
		finfo, _ := f.Stat()
		size := finfo.Size()
		gLog.Trace.Printf("Checking data file: %s - size: %d ....\n",infile, size)
			return &St33Reader{
				File:     f,
				Size:     size,
				Previous: 0,
				Current:  0,
			}, err
	} else {
		return nil,err
	}
}
*/

func NewSt33Reader(infile string ) (*St33Reader, error){
	a,err:= utils.NewVBRecord(infile)
	return &St33Reader  {
		a,
	},err
}

//
// Set position of the current record to read
//
/*
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
		f := r.File
		c := r.Current
		return f.ReadAt(b, c)
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
//  read n bytes from the current position (r.Current)

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

func (r *St33Reader) writeRecord(b[]byte,f os.File) (error){
	_,err := f.Write(b)
    return err
}

*/
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
			// gLog.Info.Printf("%s", hex.Dump(buf[0:255]))

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
			gLog.Info.Printf("BLOB ST33 - PXIID: %s -  Number of records in control file:%d/ Number of read records:%d - Doc size:%d  Buffer size:%d  blob size:%d ",v.PxiId, v.Records,recs,docsize,bufsize,blobL)

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

func (r *St33Reader) ReadST33Tiff( v Conval, ind int) (int,int,bool){

	// ST33 records

	// gLog.Info.Printf("ST33 PXIID: %s - Ctrl number of records: %d - Ctrl number of pages: %d",v.PxiId,v.Records,v.Pages)

	var (
		recs      =  0
		pages     = 0
		ST33     []byte
		tiffRecs uint16
		reci     uint16
		Big	     = binary.BigEndian
		pagenum, numpages int
		warning, errors  int
		critical bool
	)

	//
	// First compare the number of pages from data file  against  the number of pages from the control file for a document
	// if they differ skip the recod and get the  the next record  until they  match  or EOF
	// if they match, rewind one record  for processing again ( if we are lucky, we are reading the correct image
	//
	loop := 1
	for {
		if buf,err := r.Read(); err == nil {
			if len(buf) > 214 {
				ST33 = utils.Ebc2asci(buf[0:214])
				pagenum, _ = strconv.Atoi(string(ST33[17:21]))
				numpages, _ = strconv.Atoi(string(ST33[76:80]))

				if numpages == int(v.Pages)  {          //  if   match then rewind to the previous record
					r.SetCurrent(r.GetPrevious() - 8)
					break
				} else {
					loop ++
					gLog.Warning.Printf("Loop: %d - PXIID: %s/%s - Page #: %s - Ref #: %s - Total # of pages in Control file: %d != Total # of pages: %d in the image at buffer address: x'%x'", loop, v.PxiId, ST33[5:17],ST33[17:21],ST33[34:45],v.Pages, numpages, r.GetPrevious())
					gLog.Warning.Println(hex.Dump(buf[0:214]))
					warning ++
					if loop >= 6 {
						gLog.Warning.Printf("PXIID: %s - Too many errors %d  at entry %d  in file %s ",v.PxiId,loop,ind,r.File.Name())
						gLog.Error.Printf("PXIID: %s - Too many errors %d  at entry  %d  in file %s",v.PxiId,loop,ind,r.File.Name())
						critical = true
					 	return warning,errors,critical
					}
				}
			}
		} else {
			gLog.Error.Printf("Reading %s - error %v",v.PxiId,err)
			critical = true
			return warning,errors,critical
			// break  // could be end of file
		}
	}

	//
	//  From here the number of pages in data file IS EQUAL  number of page  in control file
	//
	var (
		buf []byte
		err error
	)
	// current := r.Current

	P := int(v.Pages)
	// read every page (or image)
	for p:= 1; p <= P; p++ {
		buf,err = r.Read()
		if err == io.EOF {
			break
		}
		// check validity of the ST33 record
		if err == nil && len(buf) > 214 {

			ST33 = utils.Ebc2asci(buf[0:214])
			long, _ := strconv.Atoi(string(ST33[0:5]))

			if long != len(buf) {
				error := fmt.Sprintf("Entry: %d - PXIID %d - Inavlid ST33 record @ byte address X'%x' - Read buffer length %d != ST33 record length %d ", ind, v.PxiId, r.Previous, len(buf), long)
				gLog.Error.Println(error)
				gLog.Info.Printf("X'%#x", buf[0:214])
				critical = true
				break
			}

			//
			// Un document ST33  a plusieurs pages ( Images)
			// chaque page est une image qui a plusieurs enregistrements
			// pagenum est le numero de la page ( ou image) dans le document ST33
			// pages est le  nombre de pages qui a été traités
			//    pages DOIT ETRE EGALE a pagenum
			//

			pagenum, _ = strconv.Atoi(string(ST33[17:21]))  //  Get the ST33 page number from  the image
			recs++                                          //  incr  total number of records
			pages++                                         //  the page number which is being processed
			gLog.Trace.Printf("Entry: %d - PXIID: %s  has %d pages and %d records- Current page number/page number found in image: %d/%d - current record number:%d - Length of buffer: %d - Prev/cur X'%x'/X'%x' ", ind, v.PxiId, int(v.Pages),int(v.Records) ,pages, pagenum, recs, len(buf),r.Previous,r.Current)

			//  Check the page number in the control file against the page number of the image
			//  pages =  Total number of pages ( control file)
			//  pagenum = Total number of pages ( image header)

			if pages != pagenum {
				gLog.Warning.Printf("Entry: %d - PXIID: %s - Oops ! Current page# in control file: %d < NOT EQUAL> Current page # found in the page: %d ",ind, v.PxiId,pages,pagenum)
				gLog.Warning.Println(hex.Dump(buf[0:214]))
				// stop = true
			}
			_ = binary.Read(bytes.NewReader(buf[84:86]), Big, &tiffRecs)     //  read the total number of the records for this image

			//
			// read all the records of the image
			// the number of records are from the image  header itself
			//
			for rec := 2; rec <= int(tiffRecs); rec ++ {
				recs++
				if buf, err = r.Read(); err == nil {
					ST33 = utils.Ebc2asci(buf[0:214])
					_ = 	binary.Read(bytes.NewReader(buf[25 : 27]), Big, &reci)
					gLog.Trace.Printf("Entry: %d - PXIID: %s/%s - Ref #: %s - Processing ST33 page #: %d - Page # found in the page: %d - Current record # in ST33 page: %d - Current ST33 record #: %d - length of buffer: %d - Prev/cur X'%x'/X'%x'", ind, v.PxiId, ST33[5:17],ST33[34:45],pages, pagenum, reci,recs, len(buf),r.Previous,r.Current)
				} else {
					gLog.Error.Printf("Entry: %d - PXIID: %s/%s - Read error %v",ind,v.PxiId, ST33[5:17],err)
				    critical = true
				    break
				}
			}
		}
	}

	//
	//    After reading all the pages specified in the control file and for each page all teh record specified in the header of every ST33  image,
	//    Compare the number of records in the control file against the  total number of records of the image specfied in the images
	//

	if v.Records != recs {
		gLog.Error.Printf("Entry: %d - PXIID: %s/%s - Ref #: %s - Page#: %s -  Total # of records in the control file: %d != total # of records found in the image: %d - Length of buffer:%d - Prev/cur X'%x'/X'%x'",ind, v.PxiId,ST33[5:17],ST33[34:45],ST33[17:21],v.Records,recs,len(buf),r.Previous,r.Current)
		gLog.Error.Println(hex.Dump(buf[0:214]))
		errors++
	} else {
		gLog.Info.Printf("Entry: %d - PXIID: %s - Conval index %d : Good - # of pages: %d - # of records: %d ",ind,v.PxiId,ind,pages,recs)
	}

	return warning,errors,critical

}


