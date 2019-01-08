
package pxi

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"io/ioutil"
	"path/filepath"
	"strings"
)

func ST33ToFiles(ifile string,  odir string, test bool)  (int,int, error){

	var (

		confile 		string
		conval			*[]Conval
		err 			error
		numdocs			int=0
		numpages		int=0
	)

	/* read  the control  file  */
	conval = &[]Conval{}
	confile = strings.Replace(ifile,DATval,CONval,1)
	if !utils.Exist(confile) {
		return 0,0,errors.New(fmt.Sprintf("Corrresponding dirval file %s does not exist for input file %s ",confile,ifile))
	} else {
		if conval,err  = BuildConvalArray(confile); err != nil {
			return 0,0,errors.New(fmt.Sprintf("Error %v  reading %s ",err,confile))
		}
	}
	conVal := *conval

	/* read ST33  file */

	abuf, err := utils.ReadBuffer(ifile)
	defer 	abuf.Reset()
	buf		:= abuf.Bytes()

	if err == nil {
		var (
			//  k 		int64
			//image 	PxiImg
			l		int64 = 0
		)
		for _, v := range conVal {

			var pathname string
			/* read the input buffer */
			lp := len(v.PxiId)
			if v.PxiId[lp-2:lp-1] == "P" {
				/* Get the document */
				s:= 0
				for p:= 0; p < int(v.Pages); p++ {
					var image = PxiImg {}
					l,err = image.BuildTiffImage(buf,l)

					// image, k = buildTiffImage(buf, l)

					//l = k
					s += image.Img.Len()

					gLog.Trace.Println("ST33 ", v.PxiId,v.Pages,   "---" , string(image.PxiId),string(image.NumPages),image.Img.Len())
					pathname = filepath.Join(odir, v.PxiId + "." +  string(image.PageNum))
					if !test {
						WriteImgToFile(pathname, image.Img)
						if p == 0 {
							if usermd, err := buildUserMeta(v); err == nil {
								pathname += ".md"
								if err:= ioutil.WriteFile(pathname,[]byte(usermd),0644); err != nil {
									gLog.Error.Printf("Error %v writing %s ",err,pathname)
								}
							}
						}
					}
					image.Img.Reset() /* reset the previous image buffer */
				}
				numpages += int(v.Pages)
				numdocs++

			} else {

				var pxiblob = pxiBlob{
					Key : v.PxiId,
					Record: v.Records,
					Blob  :	 new(bytes.Buffer),
				}

				if l,err  = pxiblob.BuildPxiBlob(buf,l); err == nil {
					// log.Printf(" Blob %s  --- size %d", KEY, pxiblob.Blob.Len())
					pathname = filepath.Join(odir, "BLOB", v.PxiId)
					if !test {
						WriteImgToFile(pathname, pxiblob.Blob)
						if usermd, err := buildUserMeta(v); err == nil {
							pathname += ".md"
							if err:= ioutil.WriteFile(pathname,[]byte(usermd),0644); err != nil {
								gLog.Error.Printf("Error %v writing %s ",err,pathname)
							}
						}// write meta data

					}
					pxiblob.Blob.Reset()  /* reset the previous blob buffer */
					numpages++
					numdocs++

				} else {
						gLog.Error.Printf("Error %v",err)
				}
			}
		}
	}

	//  lookup table

	return numpages,numdocs,err
}
