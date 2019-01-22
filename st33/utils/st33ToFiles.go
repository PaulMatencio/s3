
package st33

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"path/filepath"
	"strconv"
	"strings"
)

func ToFiles(ifile string,  odir string, bdir string, test bool)  (int,int, error){

	var (

		confile   		string
		conval			*[]Conval
		err 			error
		numdocs			int=0
		numpages		int=0
	)

	/* read  the control  file  */

	conval = &[]Conval{}
	confile = strings.Replace(ifile,DATval,CONval,1)
	if !utils.Exist(confile) {
		return 0,0,errors.New(fmt.Sprintf("Corrresponding control file %s does not exist for input data file %s ",confile,ifile))
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
			l		int64 = 0
		)
		for _, v := range conVal {

			var pathname string
			/* read the input buffer */
			lp := len(v.PxiId)
			KEY := v.PxiId;

			if v.PxiId[lp-2:lp-1] == "P" {

				s:= 0
				KEY = utils.Reverse(KEY)

				for p:= 0; p < int(v.Pages); p++ {

					if image, k, err := GetPage(v, buf, l); err == nil {
						l = k
						s += image.Img.Len()

						gLog.Trace.Printf("ST33  Key:%s numPages:%d PxiId:%s Page number:%s   ImageLength:%d", v.PxiId, v.Pages, string(image.PxiId), string(image.PageNum), image.Img.Len())

						pagenum, _ := strconv.Atoi(string(image.PageNum))
						pathname = filepath.Join(odir, KEY+"."+strconv.Itoa(pagenum))

						if !test {
							if err := utils.WriteFile(pathname, image.Img.Bytes(), 0644); err != nil {
								gLog.Error.Printf("Error %v writing image %s ", err, pathname)
							}

							if pagenum == 1 {
								if usermd, err := BuildUsermd(v); err == nil {
									pathname += ".md"
									if err = WriteUsermd(usermd,pathname); err != nil {
										gLog.Error.Printf("Error writing user metadata %v",err)
									}
								} else {
									gLog.Error.Printf("Error building user metadata %v",err)
								}
							}
						}
						image.Img.Reset() /* reset the previous image buffer */
					} else {
						gLog.Fatal.Printf("Error %v getting  page number %d", err,p+1)
					}
				}

				numpages += int(v.Pages)
				numdocs++


			} else {

				var pxiblob = pxiBlob{
					Key : utils.Reverse(KEY),
					Record: v.Records,
					Blob  :	 new(bytes.Buffer),
				}

				if l,err  = pxiblob.BuildPxiBlob(buf,l); err == nil {
					pathname = filepath.Join(bdir, pxiblob.Key)+".1"

					if !test {
						WriteImgToFile(pathname, pxiblob.Blob)
						if usermd, err := BuildUsermd(v); err == nil {
							pathname += ".md"
							if err = WriteUsermd(usermd,pathname); err != nil {
								gLog.Error.Printf("Error writing user metadata %v",err)
							}
						} else {
							gLog.Error.Printf("Error building user metadata %v",err)
						}
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

