
package st33

import (
	"io"
	"log"
	"os"
	"path/filepath"

	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/utils"
	// "path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	// "encoding/json"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"strings"
	"time"
)

type PutS3Response struct {

	Bucket  string
	Key     string
	Size    int
	Error   S3Error
}


//
//   St33 to S3    version 1
//   Best for small and  medium size input data file <  5  GB
//   Input : ToS3Request [ Infile, bucket, log bucket, .....]
//   Action :  Load Control file and input data file
//             For every entry of the control file, upload corresponding data to S3
//   Result > Number of documents and pages  uploaded, number of errors and a list of errors
//
func ToS3V1(req *ToS3Request)  (int, int, int, []S3Error) {

	var (
		infile   = req.File
		bucket   = req.Bucket
		sbucket  = req.LogBucket
		reload   = req.Reload
		confile				string
		conval				*[]Conval
		err 				error
		ErrKey,inputError	[]S3Error
		numpages,numdocs,S	int		=  0,0,0
	)

	if req.Profiling > 0 {
		go func() {
			for {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				// debug.FreeOSMemory()
				log.Println("Systenm memory:", float64(m.Sys)/1024/1024)
				log.Println("Heap allocation", float64(m.HeapAlloc)/1024/1024)
				time.Sleep(time.Duration(req.Profiling) * time.Second)
			}
		}()
	}

	/* Check the existence of the control file */
	conval = &[]Conval{}
	confile = strings.Replace(infile,DATval,CONval,1)

	if !utils.Exist(confile) {

		ErrKey = append(ErrKey,S3Error {
			"",
			errors.New(fmt.Sprintf("Corrresponding dirval file %s does not exist for input file %s ",confile,infile)),
		})
		return 0,0,0,ErrKey
	} else {

		if conval,err  = BuildConvalArray(confile); err != nil {
			ErrKey = append(ErrKey,S3Error {
				"",
				errors.New(fmt.Sprintf("Error %v  reading %s ",confile,infile)),
			})
			return 0,0,0,ErrKey
		}
	}

	conVal := *conval
	svc :=  s3.New(api.CreateSession())
	_,key := filepath.Split(infile)
	if sbucket != "" {
		getReq := datatype.GetObjRequest{
			Service: svc,
			Bucket:  sbucket,
			Key   :  key,
		}
		//
		// check if datafile is already fully loaded
		//  if req.Reload  then skip checking
		//  otherwsise check if datafile is already fully loaded
		//

		if !reload && !checkDoLoad(getReq,infile)  {
			return 0,0,0,ErrKey
		}
	}

	start0:= time.Now()
	putReq  := datatype.PutObjRequest {
		Service: svc,
		Bucket: req.Bucket,
	}


	abuf, err := utils.ReadBuffer(infile)
	defer 	abuf.Reset()
	buf		:= abuf.Bytes()
	if err == nil {

		var (
			Numdocs 	int = len(conVal)
			l		int64 = 0
		)

		gLog.Info.Printf("Number of documents to upload %d",Numdocs)

		for e, v := range conVal {
			gLog.Trace.Printf("Uploading document %s  number of pages %d",v.PxiId,v.Pages)
			lp := len(v.PxiId)
			KEY := v.PxiId;
			if v.PxiId[lp-2:lp-1] == "P" {

				KEY = utils.Reverse(KEY)

				s:= 0
				for p:= 0; p < int(v.Pages); p++ {
					// set the key of the s3 object
					putReq.Key = KEY
					// reset user metadata
					putReq.Usermd = map[string]string{}
					// extract the image
					if image,k,err,err1 := GetPage(v, buf, l); err== nil {

						l = k
						s += image.Img.Len()
						// update docsize with the actual image size
						v.DocSize = uint32(image.Img.Len())
						S += int(v.DocSize)                             // update thesize of the image
																		// build the user metadata for the first page only
						pagenum, _ := strconv.Atoi(string(image.PageNum))
						if pagenum == 1 {
							if usermd, err := BuildUsermd(v); err == nil {
								putReq.Usermd = utils.AddMoreUserMeta(usermd,infile)
							}
						}

						putReq.Key = putReq.Key + "." + strconv.Itoa(pagenum)  // Set S3 Key
						putReq.Buffer = image.Img                              // Set S3 buffer

						if _, err := writeToS3(putReq); err != nil {           // upload the image to S3
							gLog.Fatal.Printf("PutObject Key: %s  Error: %v", putReq.Key, err)
							os.Exit(100)


						}

						image.Img.Reset()   // reset the image buffer
					} else {

						if err1 != nil {
							inputError= append(inputError,S3Error{Key:v.PxiId,Err: err1})
						}
					}
				}

				numpages += int(v.Pages)   // increment number of processed pages

			} else if v.PxiId[lp-2:lp-1] == "B" {                        // It is a BLOB
				pxiblob:= NewPxiBlob(v.PxiId,v.Records)
				gLog.Trace.Println(putReq.Key,len(buf),l,v.Records)
				if l,err  = pxiblob.BuildPxiBlobV1(buf,l); err == nil {    // Build the blob
					v.DocSize= uint32(pxiblob.Blob.Len())                // Update oroginal blob size
					S += int(v.DocSize)                                  // Add Blob user metadata
					if usermd,err:= BuildUsermd(v); err == nil {
						putReq.Usermd = utils.AddMoreUserMeta(usermd,infile)
					}
					putReq.Buffer = pxiblob.Blob                         // set s3 buffer
					putReq.Bucket = bucket                               // set s3 bucket
					putReq.Key = pxiblob.Key                             // set s3 key

					if _,err:= writeToS3(putReq); err != nil {           // upload the blob
						ErrKey = append(ErrKey,S3Error{Key:putReq.Key, Err: err})
					}
					pxiblob.Blob.Reset()                                // Reset Blob structure
					numpages++                                          // increment the number of pages
				} else {
					gLog.Warning.Printf("Control file %s and data file %s do not map for key %s",confile,infile,v.PxiId)
				}
			} else {
				error := errors.New(fmt.Sprintf("Control file entry: %d contains invalid input key: %s",e,v.PxiId))
				gLog.Error.Printf("%v",error)
				ErrKey= append(ErrKey,S3Error{Key:v.PxiId,Err:error})
			}
			numdocs++  // increment number of processed documents
		}
		duration := time.Since(start0)
		status := PartiallyUploaded
		numerrupl := len(ErrKey)    // number of upload with errors
		numerrinp := len(inputError)  // input data error
		if numerrupl == 0   {
			if numerrinp == 0 {
				status = FullyUploaded
			}  else {
				status =  FullyUploaded2
			}
		}
		// build S3 response
		resp := ToS3Response {
			Time: time.Now(),
			Duration: fmt.Sprintf("%s",duration),
			Status : status,
			Docs  : numdocs,
			Pages : numpages,
			Size  : S,
			Erroru : numerrupl,
			Errori : numerrinp,

		}
		// append input data consistency to ErrKey array
		for _,v := range inputError {
			ErrKey = append(ErrKey,v)
		}

		if _,err = logIt(svc,req,&resp,&ErrKey); err != nil {
			gLog.Warning.Printf("Error logging request to %s : %v",req.LogBucket,err)
		}
		gLog.Info.Printf("Infile:%s - Number of uploaded documents/objects: %d/%d - Uploaded size: %.2f GB- Uploading time: %s  - MB/sec: %.2f ",infile,numdocs,numpages,float64(S)/float64(1024*1024*1024),duration,1000*float64(S)/float64(duration))
		abuf.Reset()
		return numpages,numdocs,S,ErrKey
	}

	gLog.Info.Printf("Number of uploaded documents %d - Number of uploaded pages %d",numdocs,numpages)
	return numpages,numdocs,S,ErrKey
}


//
//
//
//   St33 to S3    version 2
//   Best for input data file of size >  5 GB
//   Input : ToS3Request [ Infile, bucket, log bucket, .....]
//   Action :  Load control file and  get data file record  sequentially ( st33Reader)
//             For every entry of the control file, upload corresponding data to S3
//   Result > Number of documents and pages  uploaded, number of errors and a list of errors
//

func ToS3V2(req *ToS3Request)  (int, int, int, []S3Error) {

	var (
		infile   = req.File
		bucket   = req.Bucket
		sbucket  = req.LogBucket
		reload   = req.Reload
		confile				string
		conval				*[]Conval
		err 				error
		ErrKey,inputError	[]S3Error
		numpages,numdocs,S	int		=  0,0,0
	)

	if req.Profiling > 0 {
		go func() {
			for {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				// debug.FreeOSMemory()
				log.Println("Systenm memory:", float64(m.Sys)/1024/1024)
				log.Println("Heap allocation", float64(m.HeapAlloc)/1024/1024)
				time.Sleep(time.Duration(req.Profiling) * time.Second)
			}
		}()
	}

	/* Check the existence of the control file */
	conval = &[]Conval{}
	confile = strings.Replace(infile,DATval,CONval,1)

	if !utils.Exist(confile) {

		ErrKey = append(ErrKey,S3Error {
			"",
			errors.New(fmt.Sprintf("Corrresponding dirval file %s does not exist for input file %s ",confile,infile)),
		})
		return 0,0,0,ErrKey
	} else {

		if conval,err  = BuildConvalArray(confile); err != nil {
			ErrKey = append(ErrKey,S3Error {
				"",
				errors.New(fmt.Sprintf("Error %v  reading %s ",confile,infile)),
			})
			return 0,0,0,ErrKey
		}
	}

	conVal := *conval
	svc :=  s3.New(api.CreateSession())
	_,key := filepath.Split(infile)
	if sbucket != "" {
		getReq := datatype.GetObjRequest{
			Service: svc,
			Bucket:  sbucket,
			Key   :  key,
		}
		//
		// check if datafile is already fully loaded
		//  if req.Reload  then skip checking
		//  otherwsise check if datafile is already fully loaded
		//

		if !reload && !checkDoLoad(getReq,infile)  {
			return 0,0,0,ErrKey
		}
	}

	start0:= time.Now()
	putReq  := datatype.PutObjRequest {
		Service: svc,
		Bucket: req.Bucket,
	}

	/* read ST33  file */
	gLog.Info.Printf("Processing input file %s - control file %s",infile,confile)
	r,err  := NewSt33Reader(infile)

	if err == nil {
		var (
			Numdocs 	int = len(conVal)
		)
		gLog.Info.Printf("Number of documents to upload %d",Numdocs)

		for e, v := range conVal {
			var (
				recs   int =0
				pages  int = 0
				lp = len(v.PxiId)
				KEY = v.PxiId
			)

			gLog.Info.Printf("Document#: %d - Uploading Key: %s - Number of pages %d / Number of records %d",e, utils.Reverse(v.PxiId),v.Pages,v.Records)

			if v.PxiId[lp-2:lp-1] == "P" {
				// exclude This PXID
				if v.PxiId != "E1_____113F65926719P1"   {  //not in datafile

					KEY = utils.Reverse(KEY)
					s := 0
					for p := 0; p < int(v.Pages); p++ {
						// set the key of the s3 object
						putReq.Key = KEY
						putReq.Usermd = map[string]string{} // reset user metadata

						if image, nrec, err, err1 := GetPageV2(r, v); err == nil || err == io.EOF {

							pages++
							recs += nrec

							s += image.Img.Len()
							// update docsize with the actual image size
							v.DocSize = uint32(image.Img.Len())
							S += int(v.DocSize)
							// build the user metadata for the first page only
							pagenum, _ := strconv.Atoi(string(image.PageNum))

							if pagenum == 1 {
								if usermd, err := BuildUsermd(v); err == nil {
									putReq.Usermd = utils.AddMoreUserMeta(usermd, infile)
								}
							}

							// complete the request to write to S3
							putReq.Key = putReq.Key + "." + strconv.Itoa(pagenum)
							putReq.Buffer = image.Img

							if _, err := writeToS3(putReq); err != nil {
								error := errors.New(fmt.Sprintf("PutObject Key: %s  Error: %v", putReq.Key, err))
								gLog.Error.Printf("%v", error)
								ErrKey = append(ErrKey, S3Error{Key: v.PxiId, Err: error})
							}

							// reset the image
							image.Img.Reset()
						} else {
							// should never happen unless input data is corrupted
							// gLog.Fatal.Printf("%v",err)
							//  os.Exit(100)
							if err1 != nil {
								inputError = append(inputError, S3Error{Key: v.PxiId, Err: err1})
							}
						}
					}

					numpages += int(v.Pages)
			} else {
					error := errors.New(fmt.Sprintf("Skipping control key: %s ", v.PxiId))
					gLog.Error.Printf("%v", error)
					ErrKey = append(ErrKey, S3Error{Key: v.PxiId, Err: error})
				}

			} else if v.PxiId[lp-2:lp-1] == "B" {

				if v.PxiId != "E1_____114270EFD39ABL" {

				// NewPxiBlob  returns a pxiblob with  a reverse KEY
					pxiblob := NewPxiBlob(KEY, v.Records)
					if nrec, err := pxiblob.BuildPxiBlobV2(r, v); err == nil {
						if nrec != v.Records {
							// Check number of BLOB record
							error := errors.New(fmt.Sprintf("Key %s - Control file records %d != Blob records %d", v.PxiId, v.Records, nrec))
							gLog.Error.Printf("%v", error)
							ErrKey = append(ErrKey, S3Error{Key: v.PxiId, Err: error})

						}
						v.DocSize = uint32(pxiblob.Blob.Len())
						S += int(v.DocSize)
						if usermd, err := BuildUsermd(v); err == nil {
							putReq.Usermd = utils.AddMoreUserMeta(usermd, infile)
						}

						putReq.Buffer = pxiblob.Blob
						putReq.Bucket = bucket
						putReq.Key = pxiblob.Key

						if _, err := writeToS3(putReq); err != nil {

							error := errors.New(fmt.Sprintf("PutObject Key: %s  Error: %v", putReq.Key, err))
							gLog.Error.Printf("%v", error)
							ErrKey = append(ErrKey, S3Error{Key: v.PxiId, Err: error})
						}

						pxiblob.Blob.Reset()
						numpages++
					} else {
						gLog.Warning.Printf("Control file %s and data file %s do not map for key %s", confile, infile, v.PxiId)
					}
				} else {
					error := errors.New(fmt.Sprintf("Skipping control key: %s", v.PxiId))
					gLog.Error.Printf("%v", error)
					ErrKey = append(ErrKey, S3Error{Key: v.PxiId, Err: error})
				}
			} else {
				error := errors.New(fmt.Sprintf("Control file entry: %d contains invalid input key: %s",e,v.PxiId))
				gLog.Error.Printf("%v",error)
				ErrKey= append(ErrKey,S3Error{Key:v.PxiId,Err:error})
			}

			numdocs++
		}
		duration := time.Since(start0)
		status := PartiallyUploaded
		numerrupl := len(ErrKey)    // number of upload with errors
		numerrinp := len(inputError)  // input data error
		if numerrupl == 0   {
			if numerrinp == 0 {
				status = FullyUploaded
			}  else {
				status =  FullyUploaded2
			}
		}

		resp := ToS3Response {
			Time: time.Now(),
			Duration: fmt.Sprintf("%s",duration),
			Status : status,
			Docs  : numdocs,
			Pages : numpages,
			Size  : S,
			Erroru : numerrupl,
			Errori : numerrinp,
		}

		for _,v := range inputError {             // append input data consistency to ErrKey array
			ErrKey = append(ErrKey,v)
		}

		if _,err = logIt(svc,req,&resp,&ErrKey); err != nil {
			gLog.Warning.Printf("Error logging request to %s : %v",req.LogBucket,err)
		}
		gLog.Info.Printf("Infile:%s - Number of uploaded documents/objects: %d/%d - Uploaded size: %.2f GB- Uploading time: %s  - MB/sec: %.2f ",infile,numdocs,numpages,float64(S)/float64(1024*1024*1024),duration,1000*float64(S)/float64(duration))
		return numpages,numdocs,S,ErrKey
	}

	gLog.Info.Printf("Number of uploaded documents %d - Number of uploaded pages %d",numdocs,numpages)
	return numpages,numdocs,S,ErrKey
}



//
// Same a ToS3V1 but  concurrent upload data to S3
//
//

func ToS3V1Async(req *ToS3Request)  (int, int, int, []S3Error)  {

	var (
		infile   = req.File
		bucket   = req.Bucket
		sbucket  = req.LogBucket
		reload   = req.Reload
		confile				string
		conval				*[]Conval
		err 				error
		ErrKey,inputError	[]S3Error
		numpages,numdocs,E,S,S1	int		=  0,0,0,0,0
	)
	//  monitor storage and free storage if necessary
	if req.Profiling > 0 {
		go func() {
			for {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)

				heap := float64(m.HeapAlloc)/1024/1024
				log.Println("System memory MB:", float64(m.Sys)/1024/1024)
				log.Println("Heap allocation MB", heap)
				debug.FreeOSMemory()
				time.Sleep(time.Duration(req.Profiling) * time.Second)
			}
		}()
	}

	/*
		Create a  S3 session
	 */
	svc :=  s3.New(api.CreateSession())

	/* Check the existence of the control file */
	conval = &[]Conval{}
	confile = strings.Replace(infile,req.DatafilePrefix,req.CrlfilePrefix,1)

	if !utils.Exist(confile) {

		ErrKey = append(ErrKey,S3Error {
			"",
			errors.New(fmt.Sprintf("Corrresponding dirval file %s does not exist for input file %s ",confile,infile)),
		})
		return 0,0,0,ErrKey
	} else {

		if conval,err  = BuildConvalArray(confile); err != nil {
			ErrKey = append(ErrKey,S3Error {
				"",
				errors.New(fmt.Sprintf("Error %v  reading %s ",confile,infile)),
			})
			return 0,0,0,ErrKey
		}
	}

	conVal := *conval

	// check the existence of the state migration  Bucket
	// and if  data file was already uploaded
	_,key := filepath.Split(infile)
	if sbucket != "" {
		getReq := datatype.GetObjRequest{
			Service: svc,
			Bucket:  sbucket,
			Key   :  key,
		}

		//
		// check if datafile is already fully loaded
		//  if req.Reload  then skip checking
		//  otherwsise check if datafile is already fully loaded
		//

		if !reload && !checkDoLoad(getReq,infile)  {
			return 0,0,0,ErrKey
		}
	}

	// read ST33  file
	gLog.Info.Printf("Reading file ... %s",infile)
	abuf, err := utils.ReadBuffer(infile)
	defer 	abuf.Reset()
	buf		:= abuf.Bytes()
	ch := make(chan *PutS3Response)
	start0:= time.Now()

	if err == nil {
		var (
			Numdocs       = len(conVal)
			l int64 	  = 0
			p             = 0
			step          = req.Async
			stop          = false
		)

		gLog.Info.Printf("Uploading %d documents to bucket %s ...", Numdocs,bucket)
		// Set the break of the main loop
		if step > Numdocs {
			step = Numdocs
			stop = true
		}
		q:= step

		for {
			N:= 0;T :=0
			start1 := time.Now()
			for e, v := range conVal[p:q] {

				KEY := v.PxiId;
				lp := len(KEY);
				if KEY[lp-2:lp-1] == "P" {
					// s:= 0
					N += int(v.Pages)
					for p := 0; p < int(v.Pages); p++ {
						// extract tiff image
						image,k,err,err1 := GetPage(v,buf,l)
						// err1 is not null  when the control file and datafile differ
						// Extraction can continue
						if err1 != nil {
							inputError= append(inputError,S3Error{Key:v.PxiId,Err: err1})
							// fmt.Println("....",inputError)
						}
                        // Get page OK
						if err == nil {
							numpages++
							v.DocSize = uint32(image.Img.Len()) // update the document size

							go func(key string, image PxiImg, v Conval) {

								req := datatype.PutObjRequest{
									Service: svc,
									Bucket: bucket,
								}

								pagenum, _ := strconv.Atoi(string(image.PageNum))
								if pagenum == 1 {
									// Build user metadata
									if usermd, err := BuildUsermd(v); err == nil {
										req.Usermd = utils.AddMoreUserMeta(usermd,infile)
									}
								}
								// complete building  the request before writing to S3
								req.Key = utils.Reverse(key) + "." + strconv.Itoa(pagenum)
								req.Buffer = image.Img
								//  write to S3
								_,err := writeToS3(req)

								s3Error := S3Error{Key: req.Key,Err: err}

								image.Img.Reset() /* reset the previous image buffer */
								// send message to go routine listener

								ch <- &PutS3Response{bucket, req.Key, int(v.DocSize),s3Error}
							}(KEY,image, v)
						} else {
							// should never happen unless input data is corrupted
							gLog.Fatal.Printf("Error building image for Key:%s - buffer address: X'%x' ",v.PxiId,k)
							os.Exit(100)
						}
						l =k
					}
					numdocs++
				} else if KEY[lp-2:lp-1] == "B" {   // Regular Blob

					pxiblob := NewPxiBlob(v.PxiId,v.Records)

					if l, err = pxiblob.BuildPxiBlobV1(buf, l); err == nil {

						N++
						numpages++
						numdocs++
						v.DocSize = uint32(pxiblob.Blob.Len())

						go func(key string, pxiblob pxiBlob, v Conval) {

							req := datatype.PutObjRequest{
								Service: svc,
								Bucket: bucket,
								Key : pxiblob.Key,
								Buffer: pxiblob.Blob,
							}

							// build user metadata
							if usermd, err := BuildUsermd(v); err == nil {
								req.Usermd = utils.AddMoreUserMeta(usermd,infile)
							}
							// build put object request
							// Write to S3
							_,err := writeToS3(req)
							s3Error := S3Error{Key: pxiblob.Key, Err: err}
							//Reset the Blob Content
							pxiblob.Blob.Reset()
							// Send a message to go routine listener
							ch <- &PutS3Response{bucket, pxiblob.Key, int(v.DocSize),s3Error}

						}(KEY, *pxiblob, v)
					} else {
						gLog.Error.Printf("Error %v",err)
						inputError= append(inputError,S3Error{Key:v.PxiId,Err: err})
					}
				} else {
					error := errors.New(fmt.Sprintf("Control file entry: %d contains invalid input key: %s",e,v.PxiId))
					gLog.Error.Printf("%v",error)
					ErrKey= append(ErrKey,S3Error{Key:v.PxiId,Err:error})
				}

			}

			/* wait for the completion of all put objects*/

			done:= false
			S1= 0
			for ok:=true;ok;ok=!done {
				select {
				case r := <-ch:
					{
						T++
						S1 += r.Size  //  Document Size just uploaded
						S  += r.Size  // Total document size
						gLog.Trace.Printf("Upload object Key:%s - Bucket:%s - Completed:%d/%d  - Object size: %d  - Total uploaded size:%d", r.Key, r.Bucket, T,N, r.Size,S1)
						if r.Error.Err != nil {
							E++
							ErrKey = append(ErrKey, r.Error)
						}

						if T == N {
							elapsedtm := time.Since(start1)
							avgtime := float64(elapsedtm) / (float64(N) * 1000 *1000)

							gLog.Trace.Printf("%d objects were uploaded to bucket: %s - %.2f MB/sec\n", N, bucket, float64(S1)*1000/float64(elapsedtm) )
							gLog.Trace.Printf("Average object size: %d KB - avg upload time/object: %.2f ms\n", S1/(N*1024), avgtime)

							if len(ErrKey) > 0 {
								gLog.Error.Printf("\nFail to load following objects:\n")
								for _, er := range ErrKey {
									gLog.Error.Printf("Key: %s - Error: %v", er.Key, er.Err)
								}
							}

							// gLog.Trace.Printf("Infile: %s - Key:%s - Total uploaded objects:%d - Total size:%d",infile, strings.Split(r.Key,s3Client.DELIMITER)[0],N,S1)
							done = true
						}
					}
				case <-time.After(200 * time.Millisecond):
					fmt.Printf("w")
				}
			}

			if stop {
				break
			}

			if q == Numdocs {

				stop = true
				duration := time.Since(start0)
				status := PartiallyUploaded
				numerrupl := len(ErrKey)    // number of upload with errors
				numerrinp := len(inputError)  // input data error
                if numerrupl == 0   {

                	if numerrinp == 0 {
						status = FullyUploaded
					}  else {
						status =  FullyUploaded2
					}
				}

				resp := ToS3Response {
					Time: time.Now(),
					Duration: fmt.Sprintf("%s",duration),
					Status : status,
					Docs  : numdocs,
					Pages : numpages,
					Size  : S,
					Erroru : numerrupl,
					Errori : numerrinp,

				}
				// append input data consistency to ErrKey array
				for _,v := range inputError {
					ErrKey = append(ErrKey,v)
				}

				if _,err = logIt(svc,req,&resp,&ErrKey); err != nil {
					gLog.Warning.Printf("Error logging request to %s : %v",req.LogBucket,err)
				}
				gLog.Info.Printf("Infile:%s - Number of uploaded documents/objects: %d/%d - Uploaded size: %.2f GB- Uploading time: %s  - MB/sec: %.2f ",infile,numdocs,numpages,float64(S)/float64(1024*1024*1024),duration,1000*float64(S)/float64(duration))
				abuf.Reset()
				return numpages,numdocs,S,ErrKey
			}

			p += step
			q += step
			if q >= Numdocs {
				q = Numdocs
			}
			// gLog.Trace.Println("=====>",p,q,Numdocs)
		}
	}

	// Error reading data file
	ErrKey = append(ErrKey,S3Error {
		"",
		errors.New(fmt.Sprintf("Error reading data file %s %v .... ",req.File,err)),
	})
	// return without logging
	return 0,0,0,ErrKey

}


//
// Same as ToS3V2 but concurrent upload data to S3
//


func ToS3V2Async(req *ToS3Request)  (int, int, int, []S3Error)  {

	var (
		infile   = req.File
		bucket   = req.Bucket
		sbucket  = req.LogBucket
		reload   = req.Reload
		confile				string
		conval				*[]Conval
		err 				error
		ErrKey,inputError	[]S3Error
		numpages,numdocs,E,S,S1	int		=  0,0,0,0,0
	)
	//  monitor storage and free storage if necessary
	if req.Profiling > 0 {
		go func() {
			for {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)

				heap := float64(m.HeapAlloc)/1024/1024
				log.Println("System memory MB:", float64(m.Sys)/1024/1024)
				log.Println("Heap allocation MB", heap)
				debug.FreeOSMemory()
				time.Sleep(time.Duration(req.Profiling) * time.Second)
			}
		}()
	}

	/*
		Create a  S3 session
	*/
	svc :=  s3.New(api.CreateSession())

	/* Check the existence of the control file */
	conval = &[]Conval{}
	confile = strings.Replace(infile,req.DatafilePrefix,req.CrlfilePrefix,1)

	if !utils.Exist(confile) {

		ErrKey = append(ErrKey,S3Error {
			"",
			errors.New(fmt.Sprintf("Corrresponding dirval file %s does not exist for input file %s ",confile,infile)),
		})
		return 0,0,0,ErrKey
	} else {

		if conval,err  = BuildConvalArray(confile); err != nil {
			ErrKey = append(ErrKey,S3Error {
				"",
				errors.New(fmt.Sprintf("Error %v  reading %s ",confile,infile)),
			})
			return 0,0,0,ErrKey
		}
	}

	conVal := *conval

	// check the existence of the state migration  Bucket
	// and if  data file was already uploaded
	_,key := filepath.Split(infile)
	if sbucket != "" {
		getReq := datatype.GetObjRequest{
			Service: svc,
			Bucket:  sbucket,
			Key   :  key,
		}

		//
		// check if datafile is already  successfully loaded
		//  -r  or --reload will bypass the verification
		//

		if !reload && !checkDoLoad(getReq,infile)  {
			return 0,0,0,ErrKey
		}
	}

	// read ST33  input file
	gLog.Info.Printf("Reading file ... %s",infile)
	r,err := NewSt33Reader(infile)

	// Make communication channel for go routine
	ch := make(chan *PutS3Response)
	start0:= time.Now()

	if err == nil {
		var (
			Numdocs       = len(conVal)
			p             = 0
			step          = req.Async
			stop          = false
		)

		gLog.Info.Printf("Uploading %d documents to bucket %s ...", Numdocs,bucket)
		// Set the break of the main loop
		if step > Numdocs {
			step = Numdocs
			stop = true
		}
		q:= step

		for {

			var (
				N int = 0  // number of concurrent pages to be processed
				T int = 0  // number of processed pages
				start1 = time.Now()
			)

			for e, v := range conVal[p:q] {

				KEY := v.PxiId;
				lp := len(KEY);
				recs := 0  // number of records for this pxi package
				pages := 0 //  number of pages

				if KEY[lp-2:lp-1] == "P" {

					N += int(v.Pages)
					for p := 0; p < int(v.Pages); p++ {

						image,nrec,err,err1 := GetPageV2(r,v)  	// return a tiff image and its number of records
																// return err1 when the control file and datafile differ
																// but extraction will continue
						if err1 != nil {
							inputError= append(inputError,S3Error{Key:v.PxiId,Err: err1})
						}

						if err == nil /*|| err == io.EOF*/ {
							recs += nrec   // increment the number of records for this pages
							pages++        //  increment the number of pages without error.
							numpages++     //  increment the number of pages for this lot
							v.DocSize = uint32(image.Img.Len()) // update the document size (replace the oroginal value)

							go func(key string, image *PxiImg, v Conval) {

								req := datatype.PutObjRequest{                     // build a PUT OBject request
									Service: svc,
									Bucket: bucket,
								}
								// Add user metadata to page 1
								pagenum, _ := strconv.Atoi(string(image.PageNum))   // Build user metadata for page 1
								if pagenum == 1 {
									if usermd, err := BuildUsermd(v); err == nil {
										req.Usermd = utils.AddMoreUserMeta(usermd,infile)
									}
								}

								// S3 key is the reverse of the pxi id + '.' + page number
								//  PUT OBJECT
								req.Key = utils.Reverse(key) + "." + strconv.Itoa(pagenum) // add key to request
								req.Buffer = image.Img                              //   add timage
								_,err := writeToS3(req)                             //  upload the image  to S3
								//  Prepare a response Block
								s3Error := S3Error{Key: req.Key,Err: err}           // forward error
								image.Img.Reset()                                   // reset the image buffer
								ch <- &PutS3Response{bucket, req.Key, int(v.DocSize),s3Error}

							}(KEY,image, v)
						} else {
							// should never happen unless input data is corrupted
							gLog.Fatal.Printf("Error %v  building image for Key:%s - buffer address: X'%x' ",err, v.PxiId,r.Current)
							os.Exit(100)
						}

					}
					numdocs++  // increment the number of processed documents

				} else if  KEY[lp-2:lp-1] == "B"  {   // regular BLOB

					pxiblob := NewPxiBlob(v.PxiId,v.Records)                       // it is BLOB
					if nrec,err := pxiblob.BuildPxiBlobV2(r,v); err == nil {       //  Build the blob

						if nrec != v.Records { // Check number of BLOB record
						    error := errors.New(fmt.Sprintf("Pxiid %s - Control file records %d != Blob records %d",v.PxiId,v.Records,nrec))
							gLog.Warning.Printf("%v",error)
							inputError= append(inputError,S3Error{Key:pxiblob.Key,Err: error})
						}
						N++                    // Increment the number of requests
						numpages++             //  increment total number of pages
						numdocs++              //  increment total number of documents
						v.DocSize = uint32(pxiblob.Blob.Len())   // Update the original size

						go func(key string, pxiblob pxiBlob, v Conval) {

							req := datatype.PutObjRequest{
								Service: svc,
								Bucket: bucket,
								Key : pxiblob.Key,
								Buffer: pxiblob.Blob,
							}

							if usermd, err := BuildUsermd(v); err == nil {   // Add  user metadata
								req.Usermd = utils.AddMoreUserMeta(usermd,infile)
							}
							// build put object request
							// Write to S3
							_,err := writeToS3(req)
							s3Error := S3Error{Key: pxiblob.Key, Err: err}
							//Reset the Blob Content
							pxiblob.Blob.Reset()
							// Send a message to go routine listener
							ch <- &PutS3Response{bucket, pxiblob.Key, int(v.DocSize),s3Error}

						}(KEY,*pxiblob, v)
					} else {
						gLog.Error.Printf("Error %v",err)
						inputError= append(inputError,S3Error{Key:v.PxiId,Err: err})
					}
				} else {
					error := errors.New(fmt.Sprintf("Control file entry: %d contains invalid input key: %s",e,v.PxiId))
					gLog.Error.Printf("%v",error)
					ErrKey= append(ErrKey,S3Error{Key:v.PxiId,Err:error})
				}
			}

			/* wait  for goroutine signal*/

			done:= false
			S1= 0
			for ok:=true;ok;ok=!done {
				select {
				case r := <-ch:
					{
						T++           // increment the number of processed requests
						S1 += r.Size  // increment  the size of this upload
						S  += r.Size  // increment the total size
						gLog.Trace.Printf("Upload object Key:%s - Bucket:%s - Completed:%d/%d  - Object size: %d  - Total uploaded size:%d", r.Key, r.Bucket, T,N, r.Size,S1)
						if r.Error.Err != nil {
							E++
							ErrKey = append(ErrKey, r.Error)
						}

						if T == N  {   // All done
							elapsedtm := time.Since(start1)
							avgtime := float64(elapsedtm) / (float64(N) * 1000 *1000)
							gLog.Trace.Printf("%d objects were uploaded to bucket: %s - %.2f MB/sec\n", N, bucket, float64(S1)*1000/float64(elapsedtm) )
							gLog.Trace.Printf("Average object size: %d KB - avg upload time/object: %.2f ms\n", S1/(N*1024), avgtime)
							if len(ErrKey) > 0 {
								gLog.Error.Printf("\nFail to load following objects:\n")
								for _, er := range ErrKey {
									gLog.Error.Printf("Key: %s - Error: %v", er.Key, er.Err)
								}
							}
							done = true
						}
					}
				case <-time.After(200 * time.Millisecond):
					fmt.Printf("w")
				}
			}

			if stop {
				break
			}

			// log status  to a bucket
			if q == Numdocs {

				stop = true
				duration := time.Since(start0)
				status := PartiallyUploaded
				numerrupl := len(ErrKey)    // number of upload with errors
				numerrinp := len(inputError)  // input data error
				if numerrupl == 0   {         // no uploading error
					if numerrinp == 0 {       // no input data error
						status = FullyUploaded
					}  else {
						status =  FullyUploaded2
					}
				}

				resp := ToS3Response {
					Time: time.Now(),
					Duration: fmt.Sprintf("%s",duration),
					Status : status,
					Docs  : numdocs,
					Pages : numpages,
					Size  : S,
					Erroru : numerrupl,
					Errori : numerrinp,

				}
				// append input data consistency to ErrKey array
				for _,v := range inputError {
					ErrKey = append(ErrKey,v)
				}

				if _,err = logIt(svc,req,&resp,&ErrKey); err != nil {
					gLog.Warning.Printf("Error logging request to %s : %v",req.LogBucket,err)
				}
				gLog.Info.Printf("Infile:%s - Number of uploaded documents/objects: %d/%d - Uploaded size: %.2f GB- Uploading time: %s  - MB/sec: %.2f ",infile,numdocs,numpages,float64(S)/float64(1024*1024*1024),duration,1000*float64(S)/float64(duration))

				return numpages,numdocs,S,ErrKey
			}

			p += step
			q += step
			if q >= Numdocs {
				q = Numdocs
			}
			// gLog.Trace.Println("=====>",p,q,Numdocs)
		}
	}

	// Error reading data file
	ErrKey = append(ErrKey,S3Error {
		"",
		errors.New(fmt.Sprintf("Error reading data file %s %v .... ",req.File,err)),
	})
	// return without logging
	return 0,0,0,ErrKey

}

