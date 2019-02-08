package st33

import (
	"fmt"
	"github.com/s3/gLog"
	"strconv"
	"errors"
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


func GetPage2( r *St33Reader,v Conval) (PxiImg, int,error,error){

	var (
		err,err1 error
		done = false
		nrec  int
		image = PxiImg{}
	)
    //  Build the image
	 nrec,err = image.BuildTiffImage2(r,v)

	// Check if the image matches its  control file entry
	// the number of pages must match if not we are processing residual data
	// get the next image until it matches

	for ok:=true;ok;ok=!done {

		npages,_ := strconv.Atoi(string(image.NumPages))

		if npages != int(v.Pages) {

			error:= fmt.Sprintf("Skipping buffer address x'%X' for Pxi id: %s/Image id: %s - number of pages from control file( %d ) !=  number of pages from data file (%d ) ",  r.GetPrevious(), v.PxiId, image.PxiId, v.Pages, npages)
			err1 = errors.New(error)
			gLog.Error.Printf("%s",error)
			nrec,err = image.BuildTiffImage2(r,v)  //  get the next image

		} else {
			done = true
		}
	}

	return image,nrec,err,err1
}
