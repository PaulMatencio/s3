package st33

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	// "github.com/moses/user/goLog"
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


func NewSt33Reader(infile string ) (*St33Reader, error){
	a,err:= utils.NewVBRecord(infile)
	return &St33Reader  {
		a,
	},err
}



//  special Read
func (r *St33Reader) ReadST33Blob(rdw int)  ([]byte,error){
	r.Current = r.Current + 8        // skip the BDW and RDW
	b,err := r.GetRecord(int(rdw)) 	//  read  rdw bytes  at the position r.Current
	gLog.Trace.Printf("rdw %d - buffer length: %d",rdw, len(b))
	return b,err
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

func (r *St33Reader) ReadST33Tiff( v Conval, ind int) (int,int,bool){
	// ST33 TIFF records
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
				ST33 = utils.Ebc2asci(buf[0:214])  // convert ST33 header to ASCII
				pagenum, _ = strconv.Atoi(string(ST33[17:21]))
				numpages, _ = strconv.Atoi(string(ST33[76:80]))
				// The number of pages in the control file should match the number of pages in the Data file
				if numpages == int(v.Pages)  {          //  if   match then rewind to the previous record
					r.SetCurrent(r.GetPrevious() - 8)
					break
				} else {
					// loop until the number of pages are equal
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
		}
	}

	//
	//  From here the number of pages from control file and data file ARE EQUAL
	//
	var (
		buf []byte
		err error
	)

	P := int(v.Pages)
	//
	//   for every page of a ST33 record
	//      Check Length of the record = length of the St33 record ( 5 first byte of the record)
	//      if they are not equal then EXIT
	//
	//      Check total number of pages (control file) = Total number of pages ( St33 header of data file)
	//      check if the page number being number being processed = page number ( ST33 header )
	//      check total number of records ( control file) = Total number of records ( St33 hedaer of data file)
	//
	//
	for p:= 1; p <= P; p++ {
		buf,err = r.Read()   // Read a ST33 Record
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
			// Obtenir le numero de la page courante
			pagenum, _ = strconv.Atoi(string(ST33[17:21]))  //
			recs++                                          //  Incremente le nombre total de records
			pages++                                         //  Incremente le compteur de pages pour comparer avec le numero de la page courante
			gLog.Trace.Printf("Entry: %d - PXIID: %s  has %d pages and %d records- Current page number/page number found in image: %d/%d - current record number:%d - Length of buffer: %d - Prev/cur X'%x'/X'%x' ", ind, v.PxiId, int(v.Pages),int(v.Records) ,pages, pagenum, recs, len(buf),r.Previous,r.Current)

			if pages != pagenum {
				gLog.Warning.Printf("Entry: %d - PXIID: %s - Oops ! Current page# in control file: %d < NOT EQUAL> Current page # found in the page: %d ",ind, v.PxiId,pages,pagenum)
				gLog.Warning.Println(hex.Dump(buf[0:214]))
				// stop = true
			}

			// Obtenir le nombre de records pour la page courante
			_ = binary.Read(bytes.NewReader(buf[84:86]), Big, &tiffRecs)     //  get  the total number of the records for this page

			//
			//  First record of this page has been read
			//  Continue to read all the next records of the current page
			//
			for rec := 2; rec <= int(tiffRecs); rec ++ {
				recs++
				if buf, err = r.Read(); err == nil {
					if len(buf) > 214 {
						ST33 = utils.Ebc2asci(buf[0:214])
						_ = binary.Read(bytes.NewReader(buf[25:27]), Big, &reci)
						gLog.Trace.Printf("Entry number: %d - PXIID: %s/%s - Ref #: %s - Processing ST33 page #: %d - Page # found in the page: %d - Current record # in ST33 page: %d - Current ST33 record #: %d - length of buffer: %d - Prev/cur X'%x'/X'%x'", ind, v.PxiId, ST33[5:17], ST33[34:45], pages, pagenum, reci, recs, len(buf), r.Previous, r.Current)
					} else {
						critical= true
						gLog.Error.Printf("Invalid ST33 header for PXIID %s  - entry number  %d ",v.PxiId,ind)
						break
					}
				} else {
					gLog.Error.Printf("Entry number: %d - PXIID: %s/%s - Read error %v",ind,v.PxiId, ST33[5:17],err)
				    critical = true
				    break
				}
			}
		} else {
			gLog.Error.Printf("Invalid ST33 header for PXIID %s  - entry number  %d ",v.PxiId,ind)
			critical = true
			break
		}
	}

	//
	//    After reading all the pages specified in the control file and for each page all teh record specified in the header of every ST33  image,
	//    Compare the number of records in the control file against the  total number of records of the image specfied in the images
	//

	if v.Records != recs {
		gLog.Error.Printf("Entry number: %d - PXIID: %s/%s - Ref #: %s - Page#: %s -  Total # of records in the control file: %d != total # of records found in the image: %d - Length of buffer:%d - Prev/cur X'%x'/X'%x'",ind, v.PxiId,ST33[5:17],ST33[34:45],ST33[17:21],v.Records,recs,len(buf),r.Previous,r.Current)
		gLog.Error.Println(hex.Dump(buf[0:214]))
		errors++
	} else {
		gLog.Info.Printf("Entry number: %d - PXIID: %s - Conval index %d : Good - # of pages: %d - # of records: %d ",ind,v.PxiId,ind,pages,recs)
	}

	return warning,errors,critical

}


