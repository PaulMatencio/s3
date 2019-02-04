
package st33

import (
	"bytes"
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

func TooS3(infile string,  bucket  string , profiling int)  (int ,int, error){

	var (

		confile				string
		conval				*[]Conval
		err 				error
		numdocs,numpages,S	int=0,0,0
	)

	if profiling > 0 {
		go func() {
			for {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				// debug.FreeOSMemory()
				log.Println("Systenm memory:", float64(m.Sys)/1024/1024)
				log.Println("Heap allocation", float64(m.HeapAlloc)/1024/1024)
				time.Sleep(time.Duration(profiling) * time.Second)
			}
		}()
	}

	/* Check the existence of the control file */
	conval = &[]Conval{}
	confile = strings.Replace(infile,DATval,CONval,1)

	if !utils.Exist(confile) {

		return 0,0,errors.New(fmt.Sprintf("Corrresponding dirval file %s does not exist for input file %s ",confile,infile))

	} else {
		// Create an array containing the connfile
		if conval,err  = BuildConvalArray(confile); err != nil {
			return 0,0,errors.New(fmt.Sprintf("Error %v  reading %s ",err,confile))
		}
	}

	conVal := *conval

	svc :=  s3.New(api.CreateSession())

	req  := datatype.PutObjRequest {
		Service: svc,
		Bucket: bucket,
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

		for _, v := range conVal {
			gLog.Trace.Printf("Uploading document %s  number of pages %d",v.PxiId,v.Pages)
			lp := len(v.PxiId)
			KEY := v.PxiId;
			if v.PxiId[lp-2:lp-1] == "P" {

				KEY = utils.Reverse(KEY)

				s:= 0
				for p:= 0; p < int(v.Pages); p++ {
					// set the key of the s3 object
					req.Key = KEY
					// reset user metadata
					req.Usermd = map[string]string{}
					// extract the image
					if image,k,err,_ := GetPage(v, buf, l); err== nil {

						l = k
						s += image.Img.Len()
						// update docsize with the actual image size
						v.DocSize = uint32(image.Img.Len())
						S += int(v.DocSize)
						// build the user metadata for the first page only
						pagenum, _ := strconv.Atoi(string(image.PageNum))

						if pagenum == 1 {
							if usermd, err := BuildUsermd(v); err == nil {
								req.Usermd = utils.AddMoreUserMeta(usermd,infile)
							}
						}

						// complete the request to write to S3
						req.Key = req.Key + "." + strconv.Itoa(pagenum)
						req.Buffer = image.Img

						if _, err := writeToS3(req); err != nil {
							gLog.Fatal.Printf("PutObject Key: %s  Error: %v", req.Key, err)
							os.Exit(100)
						}
						// reset the image
						image.Img.Reset()
					} else {
						// should never happen unless input data is corrupted
						gLog.Fatal.Printf("%v",err)
						os.Exit(100)
					}
				}

				numpages += int(v.Pages)
				numdocs++

			} else {

				var pxiblob = pxiBlob {
					Key : utils.Reverse(KEY),
					Record: v.Records,
					Blob  :	 new(bytes.Buffer),
				}

				gLog.Trace.Println(req.Key,len(buf),l,v.Records)

				if l,err  = pxiblob.BuildPxiBlob(buf,l); err == nil {

					v.DocSize= uint32(pxiblob.Blob.Len())
					S += int(v.DocSize)
					if usermd,err:= BuildUsermd(v); err == nil {
						req.Usermd = utils.AddMoreUserMeta(usermd,infile)
					}

					req.Buffer = pxiblob.Blob
					req.Bucket = bucket
					req.Key = pxiblob.Key+".1"

					if _,err:= writeToS3(req); err != nil {
						gLog.Fatal.Printf("PutObject Key: %s  Error: %v",req.Key,err)
						os.Exit(100)
					}

					pxiblob.Blob.Reset()

					numpages++
					numdocs++
				} else {
					gLog.Warning.Printf("Control file %s and data file %s do not map for key %s",confile,infile,v.PxiId)
				}
			}
		}
	}

	gLog.Info.Printf("Number of uploaded documents %d - Number of uploaded pages %d",numdocs,numpages)
	return numpages,numdocs,err
}


// Concurrent upload of files to S3
// Input : a ST33 input file containing  tiff images and blob
// func ToS3Async(infile string,  bucket  string, profiling int,async int)  (int, int, int, []S3Error)  {
func ToS3Async(req *ToS3Request)  (int, int, int, []S3Error)  {

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
			for _, v := range conVal[p:q] {

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
				} else {

					var pxiblob = pxiBlob{
						Key:    v.PxiId,
						Record: v.Records,
						Blob:   new(bytes.Buffer),
					}

					if l, err = pxiblob.BuildPxiBlob(buf, l); err == nil {

						N++
						numpages++
						numdocs++
						v.DocSize = uint32(pxiblob.Blob.Len())

						go func(key string, pxiblob pxiBlob, v Conval) {

							req := datatype.PutObjRequest{
								Service: svc,
								Bucket: bucket,
								Key : utils.Reverse(key)+".1",
								Buffer: pxiblob.Blob,
							}

							// build user metadata
							if usermd, err := BuildUsermd(v); err == nil {
								req.Usermd = utils.AddMoreUserMeta(usermd,infile)
							}
							// build put object request
							// Write to S3 and save the return status
							_,err := writeToS3(req)
							s3Error := S3Error{Key: pxiblob.Key, Err: err}
							//Reset the Blob Content
							pxiblob.Blob.Reset()
							// Send a message to go routine listener
							ch <- &PutS3Response{bucket, pxiblob.Key, int(v.DocSize),s3Error}

						}(KEY, pxiblob, v)
					} else {
						gLog.Error.Printf("Error %v",err)
						inputError= append(inputError,S3Error{Key:v.PxiId,Err: err})
					}
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

