
package st33

import (
	"bytes"
	"log"
	"os"
	"runtime"
	"runtime/debug"

	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/utils"
	"strconv"

	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/s3Client/lib"
	"time"

	"strings"
)

type PutS3Response struct {

	Bucket  string
	Key     string
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
				debug.FreeOSMemory()
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
					image,k,_ := GetPage(v, buf, l)
					l = k
					s += image.Img.Len()
					// update docsize with the actual image size
					v.DocSize= uint32(image.Img.Len())
					S += int(v.DocSize)
					 // build the user metadata for the first page only
					pagenum, _ := strconv.Atoi(string(image.PageNum))

					if pagenum == 1 {
						if usermd, err := BuildUsermd(v); err == nil {
							req.Usermd= usermd
						}
					}

					// complete the request to write to S3
					req.Key =  req.Key + "." + strconv.Itoa(pagenum)
					req.Buffer = image.Img

					if _,err:= writeToS3(req); err != nil {
						gLog.Fatal.Printf("PutObject Key: %s  Error: %v",req.Key,err)
						os.Exit(100)
					}
					// reset the image
					image.Img.Reset()

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
						req.Usermd = usermd
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
				}
			}
		}
	}

	gLog.Info.Printf("Number of uploaded documents %d - Number of uploaded pages %d",numdocs,numpages)
	return numpages,numdocs,err
}


func ToS3Async(infile string,  bucket  string, profiling int)  (int, int, []S3Error)  {

	var (

		confile				string
		conval				*[]Conval
		err 				error
		ErrKey				[]S3Error
		numpages,numdocs,E,S,S1	int		=  0,0,0,0,0
	)

	if err != nil {
		ErrKey[0]= S3Error {
			"",
			err,
		}
		return 0,0,ErrKey
	}

	/*
		Create a  S3 session
	 */
	svc :=  s3.New(api.CreateSession())


	/* Check the existence of the control file */
	conval = &[]Conval{}
	confile = strings.Replace(infile,DATval,CONval,1)

	if !utils.Exist(confile) {
		ErrKey[0]= S3Error {
			"",
			errors.New(fmt.Sprintf("Corrresponding dirval file %s does not exist for input file %s ",confile,infile)),
		}
		return 0,0,ErrKey
	} else {

		if conval,err  = BuildConvalArray(confile); err != nil {
			ErrKey[0]= S3Error {
				"",
				errors.New(fmt.Sprintf("Error %v  reading %s ",confile,infile)),
			}
			return 0,0,ErrKey
		}
	}

	conVal := *conval
	/* read ST33  file */
	abuf, err := utils.ReadBuffer(infile)
	defer 	abuf.Reset()
	buf		:= abuf.Bytes()
	ch := make(chan *PutS3Response)
	start0:= time.Now()

	if err == nil {
		var (
			Numdocs int   = len(conVal)
			l       int64 = 0
		)

		gLog.Info.Printf("number of documents to upload %d", Numdocs)

		// Set the break of the main loop
		p := 0; step := 40; stop := false
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
				lp := len(KEY);S1= 0
				if KEY[lp-2:lp-1] == "P" {
					// s:= 0
					N += int(v.Pages)
					for p := 0; p < int(v.Pages); p++ {
						// extract tiff image
						image,k,err := GetPage(v,buf,l)

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
									if usermd, err := BuildUsermd(v); err == nil {
										req.Usermd = usermd
									}
								}
								// complete building  the request before writing to S3
								req.Key = utils.Reverse(key) + "." + strconv.Itoa(pagenum)
								req.Buffer = image.Img

								S += int(v.DocSize)
								S1 += int(v.DocSize)

								_,err := writeToS3(req)

								s3Error := S3Error{Key: req.Key, Err: err}

								image.Img.Reset() /* reset the previous image buffer */
								// send message to go routine listener

								ch <- &PutS3Response{bucket, req.Key, s3Error}
							}(KEY,image, v)
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

							S += int(v.DocSize)
							S1 += int(v.DocSize)
							// build user metadata
							if usermd, err := BuildUsermd(v); err == nil {
								req.Usermd = usermd
							}
							// build put object request
							// Write to S3 and save the return status
							_,err := writeToS3(req)
							s3Error := S3Error{Key: pxiblob.Key, Err: err}
							//Reset the Blob Content
							pxiblob.Blob.Reset()
							// Send a message to go routine listener
							ch <- &PutS3Response{bucket, pxiblob.Key, s3Error}

						}(KEY, pxiblob, v)
					}else {
						gLog.Error.Printf("Error %v",err)
					}
				}

			}

			/* wait for the completion of all put objects*/

			done:= false
			for ok:=true;ok;ok=!done {
				select {
				case r := <-ch:
					{
						T++
						gLog.Trace.Printf("Upload object Key:%s - Bucket:%s - Completed:%d - %d", r.Key, r.Bucket, N, T)
						if r.Error.Err != nil {
							E++
							ErrKey = append(ErrKey, r.Error)
						}

						if T == N {

							elapsedtm := time.Since(start1)
							avgtime := float64(elapsedtm) / (float64(N) * 1000000)
							gLog.Trace.Printf("%d objects loaded to bucket %s (%d bytes uploaded in %s)\n", N, bucket, S, elapsedtm)
							gLog.Trace.Printf("Average object size:%d bytes - average time(ms) per object:%4.3f\n", S/N, avgtime)

							if len(ErrKey) > 0 {
								gLog.Error.Printf("\nFail to load following objects:\n")
								for _, er := range ErrKey {
									gLog.Error.Printf("Key:%s - Error:%v", er.Key, er.Err)
								}
							}


							gLog.Trace.Printf("Infile: %s - Key:%s - Total uploaded objects:%d - Total size:%d",infile, strings.Split(r.Key,s3Client.DELIMITER)[0],N,S1)
							done = true

						}
					}
				case <-time.After(50 * time.Millisecond):
					fmt.Printf("w")
				}
			}

			if stop {
				break
			}
			p += step;q += step
			if q > Numdocs {
				q = Numdocs
				stop = true
				gLog.Info.Printf("Infile:%s - Number of uploaded documents/objects:%d/%d - Total Size:%d - Total elapsed time:%s",infile,numdocs,numpages,S,time.Since(start0))
				abuf.Reset()
				return numpages,numdocs,ErrKey
			}
			gLog.Trace.Println("=====>",p,q)
		}
	}

	//  lookup table
	gLog.Info.Printf("Infile: %s - Number of uploaded documents/objects:%d/%d - Total Size :%d  - Total elapsed time:%s",infile,numdocs, numpages,S,time.Since(start0))
	return  numpages,numdocs,ErrKey
}


func writeToS3( r datatype.PutObjRequest) (*s3.PutObjectOutput,error){
	gLog.Trace.Println("Write to ", r.Bucket, r.Key,r.Buffer.Len())
	return api.PutObject2(r)
}

/*
func writeToS3( r datatype.PutObjRequest, test bool) error{
	var err error
	gLog.Trace.Println("Write to ", r.Bucket, r.Key)
	if (!test) {
		if _, err := api.PutObject2(r); err != nil {
			gLog.Error.Printf("Put key %s error %v", r.Key, err)
		}
	}
	return err
}
*/
