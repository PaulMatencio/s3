package st33

import (
	"errors"
	"fmt"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"strconv"
)

func GetPage( v Conval, buf []byte, l int64) (PxiImg,int64,  error,error){

	var (
		err,err1 error
		done = false
		image = PxiImg{}
		pl = l
	)

	l,err = image.BuildTiffImage(buf,l)

	for ok:=true;ok;ok=!done {
		npages,_ := strconv.Atoi(string(image.NumPages))
		if npages != int(v.Pages) {
			error:= fmt.Sprintf("Skipping buffer address x'%X' for Pxi id: %s/Image id: %s - number of pages from control file( %d ) !=  number of pages from data file (%d ) ",  pl, v.PxiId, image.PxiId, v.Pages, npages)
			err1 = errors.New(error)
			gLog.Error.Printf("%s",error)
			// image = PxiImg{}
			l, err = image.BuildTiffImage(buf, l)
		} else {
			done = true
		}
	}

	return image,l,err,err1
}


func GetPageV2( r *St33Reader,v Conval) (*PxiImg, int,error,error){

	var (
		err,err1 error
		// done = false
		nrec  int
		image = NewPxiImg()
	)


	//
	// ADD 12-04-2019
	// Before building the image , just check  consistency of control file against data file
	//
	// First compare the number of pages from data file  against  the number of pages from the control file for a document
	// if they differ skip the recod and get the  the next record  until they  match  or EOF
	// if they match, rewind one record  for processing again ( if we are lucky, we are reading the correct image
	//
	for {
		if buf,err    := r.Read(); err == nil {
			if len(buf) > 214 {
				ST33 := utils.Ebc2asci(buf[0:214])
				// pagenum, _ := strconv.Atoi(string(ST33[17:21]))
				numpages, _ := strconv.Atoi(string(ST33[76:80]))

				if numpages == int(v.Pages) {  //  if   match then rewind to the previous record
					r.SetCurrent(r.GetPrevious() - 8)
					break
				} else {
					// gLog.Error.Printf("PXIID %s - Total number of pages in Control file %d != Total number of pages %d in the image at buffer address: x'%x'", v.PxiId, v.Pages, numpages, r.GetPrevious())
					error := fmt.Sprintf( "PXIID %s - Total number of pages in Control file %d != Total number of pages %d in the image at buffer address: x'%x'", v.PxiId, v.Pages, numpages, r.GetPrevious())
					err1 = errors.New(error)
				}
			}

		} else {
			break  // could be end of file
		}
	}

	// end ADD 12-04-2019




    //  Build the image
	 nrec,err = image.BuildTiffImage2(r,v)


	//
	// Check the image against the control file
	// the number of pages must match if not we are processing residual data
	// get the next image until it matches
	//

	/*
	   REMOVE  12-04-2019
	npages,_ := strconv.Atoi(string(image.NumPages))

	if npages != int(v.Pages) {
		error := fmt.Sprintf("At rddress x'%x' - Image id: %s/%s - #pages from control file( %d) != #pages from data file (%d ) ", r.GetPrevious(), v.PxiId, string(image.PxiId), v.Pages, npages)
		err1 = errors.New(error)
		gLog.Error.Printf("%s", error)
		// Dump the content of the data
		hex.Dump(r.Buffer.Bytes()[r.Previous:r.Previous+256])
		os.Exit(100)

	}
	   END  REMOVE 12-04-2019
	*/


    /*
	for ok:=true;ok;ok=!done {

		npages,_ := strconv.Atoi(string(image.NumPages))

		if npages != int(v.Pages) {

			if npages > int(v.Pages) {
				error := fmt.Sprintf("PXID %s/%s - %d pages are discarded  due to  number of pages from control file( %d ) < number of pages from data file (%d ) ", v.PxiId, image.PxiId, v.Pages, npages)
				err1 = errors.New(error)
				done = true

			} else {
				error := fmt.Sprintf("Skipping buffer address x'%X' for Pxi id: %s/Image id: %s - number of pages from control file( %d ) >   number of pages from data file (%d ) ", r.GetPrevious(), v.PxiId, image.PxiId, v.Pages, npages)
				err1 = errors.New(error)
				gLog.Error.Printf("%s", error)
				nrec, err = image.BuildTiffImage2(r, v) //  get the next image
				if err != nil || err == io.EOF {
					done = true
				}
			}
		} else {
			done = true
		}
	}
     */

	return image,nrec,err,err1
}
