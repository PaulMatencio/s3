
package st33

import (
	// "bytes"
	"errors"
	"fmt"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"path/filepath"
	"strconv"
	"strings"
)

func ToFiles(ifile string,  odir string, bdir string, test bool)  (int,int, int, error){

	var (

		confile   		string
		conval			*[]Conval
		err 			error
		numdocs	        int=0
		numpages		int=0
		numerrors       int=0
	)

	/* read  the control  file  */

	conval = &[]Conval{}
	confile = strings.Replace(ifile,DATval,CONval,1)
	if !utils.Exist(confile) {

		return 0,0,0,errors.New(fmt.Sprintf("Corrresponding control file %s does not exist for input data file %s ",confile,ifile))
	} else {
		if conval,err  = BuildConvalArray(confile); err != nil {
			return 0,0,0,errors.New(fmt.Sprintf("Error %v  reading %s ",err,confile))
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

					if image, k, err,_ := GetPage(v, buf, l); err == nil {
						l = k
						s += image.Img.Len()

						gLog.Trace.Printf("ST33 Key:%s - numPages:%d - PxiId:%s Page number:%s - ImageLength:%d", v.PxiId, v.Pages, string(image.PxiId), string(image.PageNum), image.Img.Len())

						pagenum, _ := strconv.Atoi(string(image.PageNum))
						pathname = filepath.Join(odir, KEY+"."+strconv.Itoa(pagenum))

						if !test {
							if err := utils.WriteFile(pathname, image.Img.Bytes(), 0644); err != nil {
								gLog.Error.Printf("Error %v writing image %s ", err, pathname)
							}

							if pagenum == 1 {
								if usermd, err := BuildUsermd(v); err == nil {
									utils.AddMoreUserMeta(usermd,ifile)
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
                /*
				var pxiblob = pxiBlob{
					Key : utils.Reverse(KEY),
					Record: v.Records,
					Blob  :	 new(bytes.Buffer),
				}
				*/
				pxiblob := NewPxiBlob(KEY,v.Records)
				gLog.Trace.Printf("Build PXI Blob %s - Key %s",KEY,pxiblob.Key)
				if l,err  = pxiblob.BuildPxiBlobV1(buf,l); err == nil {
					pathname = filepath.Join(bdir, pxiblob.Key)

					if !test {
						err = WriteImgToFile(pathname, pxiblob.Blob)
						if err != nil {
							gLog.Error.Printf("%v",err)
						}
						if usermd, err := BuildUsermd(v); err == nil {
							utils.AddMoreUserMeta(usermd,ifile)
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
						numerrors++
						numdocs ++
						gLog.Error.Printf("Error %v",err)
				}
			}
		}
	}

	//  lookup table

	return numpages,numdocs,numerrors, err
}


func ToFilesV2(ifile string,  odir string, bdir string, test bool)  (int,int, int, error){

	var (

		confile   		string
		conval			*[]Conval
		err 			error
		numdocs	        int=0
		numpages		int=0
		numerrors       int=0

	)

	/* read  the control  file  */

	conval = &[]Conval{}
	confile = strings.Replace(ifile,DATval,CONval,1)
	if !utils.Exist(confile) {
		return 0,0,0,errors.New(fmt.Sprintf("Corrresponding control file %s does not exist for input data file %s ",confile,ifile))
	} else {
		if conval,err  = BuildConvalArray(confile); err != nil {
			return 0,0,0,errors.New(fmt.Sprintf("Error %v  reading %s ",err,confile))
		}
	}
	conVal := *conval

	/* read ST33  file */
	gLog.Info.Printf("Processing input file %s - control file %s",ifile,confile)
	r,err  := NewSt33Reader(ifile)


	if err == nil {

		for _, v := range conVal {
			var (
				recs int = 0
				pages int = 0
				pathname  string
				lp = len(v.PxiId)
				KEY = v.PxiId;
			)

			gLog.Trace.Printf("Processing Key %s",v.PxiId)

			if v.PxiId[lp-2:lp-1] == "P" {  // TIFF IMAGE
				gLog.Trace.Printf("Processing ST33 Key %s Number of Pages/Records: %d/%d",v.PxiId,v.Pages,v.Records)
				s:= 0
				KEY = utils.Reverse(KEY)

				for p:= 0; p < int(v.Pages); p++ {

					if image, nrec , err, _ := GetPageV2(r,v); err == nil  {

						pages++
						recs += nrec
						if image.Img!= nil {
							s += image.Img.Len()

							gLog.Trace.Printf("ST33 Key:%s - # Pages:%d - PxiId:%s Page number:%s - ImageLength:%d", v.PxiId, v.Pages, string(image.PxiId), string(image.PageNum), image.Img.Len())
							pagenum, _ := strconv.Atoi(string(image.PageNum))
							pathname = filepath.Join(odir, KEY+"."+strconv.Itoa(pagenum))

							if err := utils.WriteFile(pathname, image.Img.Bytes(), 0644); err != nil {
								gLog.Error.Printf("Error %v writing image %s ", err, pathname)
							}

							if pagenum == 1 {
								if usermd, err := BuildUsermd(v); err == nil {
									utils.AddMoreUserMeta(usermd, ifile)
									pathname += ".md"
									if err = WriteUsermd(usermd, pathname); err != nil {
										gLog.Error.Printf("Error writing user metadata %v", err)
									}
								} else {
									gLog.Error.Printf("Error building user metadata %v", err)
								}
							}

							image.Img.Reset() /* reset the previous image buffer */
						}

					} else {
						gLog.Error.Printf("Error %v getting  page number %d", err, p+1)
						}
				}

				numpages += int(v.Pages)
				numdocs++

				//  Check if number of records of the image matche
				if v.Records != recs {
					gLog.Warning.Printf("PXIID %s - Records number [%d] of the control file != Records number [%d] of the data file ",v.PxiId,v.Records,recs)
					extrarec := v.Records - recs   // SKIP AND discard extra records
					for m:=1; m <= extrarec; m++ { // SKIP missing records
						if buf,err := r.Read(); err == nil {
							ST33 := utils.Ebc2asci(buf[0:214])
							pagenum, _ := strconv.Atoi(string(ST33[17:21]))
							gLog.Warning.Printf("PXIID %s - Skip record number %d", v.PxiId, pagenum)
						}
					}
				}
				gLog.Trace.Printf("PXIID: %s - Key %s -  #records: %d/%d - #pages: %d/%d - Total #pages: %d ",v.PxiId,utils.Reverse(v.PxiId),v.Records,recs,v.Pages,pages,numpages)

			} else if v.PxiId[lp-2:lp-1] == "B"  {            // Regular BLOB
                pxiblob := NewPxiBlob(KEY,v.Records)
				if nrec,err  := pxiblob.BuildPxiBlobV2(r,v); err == nil {
					if nrec != v.Records {                                // Check number of BLOB record
						gLog.Warning.Printf("Key %s - Control file records != Blob records",v.PxiId,v.Records,nrec)
					}
					pathname = filepath.Join(bdir, pxiblob.Key)          // Only one page
					err = WriteImgToFile(pathname, pxiblob.Blob);        // Save the image to file
					if err != nil {
						gLog.Error.Printf("%v",err)
					}
					if usermd, err := BuildUsermd(v); err == nil {       // build
						utils.AddMoreUserMeta(usermd,ifile)              // and save user metadata
						pathname += ".md"
						if err = WriteUsermd(usermd,pathname); err != nil {
							gLog.Error.Printf("Error writing user metadata %v",err)
						}
					} else {
						gLog.Error.Printf("Error building user metadata %v",err)
					}

					pxiblob.Blob.Reset()  /* reset the previous blob buffer */
					numpages++            // increment number of processed pages
				} else {
					numerrors++          // increment number of errors

					gLog.Error.Printf("Error %v",err)
				}
				numdocs++   // increment number of processed docs
			} else {
				gLog.Warning.Printf("Oop! Control file contains a wrong Key:%s",v.PxiId)
			}
		}
	} else {
		gLog.Error.Printf("%v",err)
	}

	return numpages,numdocs,numerrors, err
}
