package st33

import (
	"github.com/s3/gLog"
	"strconv"
)

func GetPage( v Conval, buf []byte, l int64) (PxiImg,int64,  error){

	var (
		err error
		done = false
		image = PxiImg{}
		pl = l
	)

	l,err = image.BuildTiffImage(buf,l)

	for ok:=true;ok;ok=!done {
		npages,_ := strconv.Atoi(string(image.NumPages))
		if npages != int(v.Pages) {
			gLog.Error.Printf("Skipping page at address x'%X' - Control pxi id: %s/Image id: %s  => Control file pages number( %d ) !=  Image pages number (%d ) ",  pl, v.PxiId, image.PxiId, v.Pages, npages)
			// image = PxiImg{}
			l, err = image.BuildTiffImage(buf, l)
		} else {
			done = true
		}
	}
	return image,l,err
}
