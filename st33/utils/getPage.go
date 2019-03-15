package st33

import (
	"errors"
	"fmt"
	"github.com/s3/gLog"
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
		// image = PxiImg{}
		image = NewPxiImg()
	)
    //  Build the image
	 nrec,err = image.BuildTiffImage2(r,v)


	// Check if the image matches its  control file entry
	// the number of pages must match if not we are processing residual data
	// get the next image until it matches

	npages,_ := strconv.Atoi(string(image.NumPages))

	if npages != int(v.Pages) {
		error := fmt.Sprintf("At rddress x'%x' - Image id: %s/%s - #pages from control file( %d) != #pages from data file (%d ) ", r.GetPrevious(), v.PxiId, string(image.PxiId), v.Pages, npages)
		err1 = errors.New(error)
		gLog.Error.Printf("%s", error)
		// Dump the content of the data
		// hex.Dump(r.Buffer.Bytes()[r.Previous:r.Previous+256])
		// os.Exit(100)

	}

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
