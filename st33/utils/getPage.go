package st33

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"strconv"
)



func GetPageV1( r *St33Reader,v Conval) (*PxiImg, int,error,error){

	var (
		err,err1 error
		// done = false
		nrec  int
		image = NewPxiImg()
	)


	//
	//  						FIX  12-04-2019
	// Before building the image , just check  consistency of control file against data file
	// First compare the number of pages from data file  against  the number of pages from the control file for a document
	// if they differ skip the recod and get the  the next record  until they  match  or EOF
	// if they match, rewind one record  for processing again ( if we are lucky, we are reading the correct image
	//

    loop:=0
	for {
		if buf,err    := r.Read(); err == nil {
			if len(buf) > 214 {
				ST33 := utils.Ebc2asci(buf[0:214])
				numpages, _ := strconv.Atoi(string(ST33[76:80]))  // get the number of pages from the st33 header
				if numpages == int(v.Pages) {
					//  compare the number of pages against the directory entrs
				    //  if match then rewind one record for BuildTiffImage and break
					r.SetCurrent(r.GetPrevious() - 8)
					// RewindST33(v,r,1)
					if loop > 0 {
						gLog.Warning.Printf("Loop: %d -  PIXID: %s - Pages are now equal [ %d = %d ] at the image  buffer at address: x'%x'",loop,v.PxiId,v.Pages, numpages, r.GetCurrent())
					}
					break
				} else {
					loop++
					gLog.Warning.Printf("Loop: %d - PXIID: %s/%s - Page #: %s - Ref #: %s - Total # of pages in control file: %d != Total # of pages: %d of the image  buffer at address: x'%x'", loop, v.PxiId,  ST33[5:17],ST33[17:21],ST33[34:45],v.Pages, numpages, r.GetPrevious())
					gLog.Warning.Println(hex.Dump(buf[0:214]))
					if loop > LOOP {
						error := fmt.Sprintf( "PXIID: %s/%s - Ref: %s - Can't get tru it - Total # of pages in control file: %d != Total number of pages: %d of the image buffer at address: x'%x' . Skip this buffer", v.PxiId,  ST33[5:17],ST33[34:45],v.Pages, numpages, r.GetPrevious())
						err = errors.New(error)
						gLog.Error.Printf("%v",err1)
						gLog.Error.Println(hex.Dump(buf[0:214]))
						return image,0,err,err1
					}
				}
			}

		} else {
			break  // could be end of file
		}
	}
	// 						end FIX  12-04-2019
    //  Build the image
	nrec,err,err1 = image.BuildTiffImage(r,v)

	return image,nrec,err,err1
}

func GetPage( r *St33Reader,v Conval) (*PxiImg, int,error,error){
	var (
		err, err1 error
		nrec  int
		image = NewPxiImg()
	)
	nrec, err, err1 = image.BuildTiffImage(r, v)
	return image,nrec,err,err1
}