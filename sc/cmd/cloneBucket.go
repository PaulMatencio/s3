package cmd

import (
	"github.com/spf13/viper"
	"sync"
	"time"
	"github.com/s3/gLog"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
)


var (
	clBucket = "Command to clone a bucket "
	clBucketCmd = &cobra.Command{
		Use:   "cloneBucket",
		Short: clBucket,
		Long: ``,
		Run: cloneBucket,
	}
	srcBucket,tgtBucket string
	fromDate string
)

func init() {
	RootCmd.AddCommand(clBucketCmd)
	RootCmd.MarkFlagRequired("bucket")
	initCbFlags(clBucketCmd)
}

func initCbFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&srcBucket,"srcBucket","s","","the name of the source bucket")
	cmd.Flags().StringVarP(&tgtBucket,"tgtBucket","t","","the name of the target bucket")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maximum number of keys to be processed ")
	cmd.Flags().StringVarP(&marker,"marker","M","","start key processing from marker")
	cmd.Flags().IntVarP(&maxLoop,"maxLoop","",1,"maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&fromDate,"fromDate","","2000-01-01T00:00:00Z","clone objects with last modified from <yyyy-mm-ddThh:mm:ss>")
	// cmd.Flags().StringVarP(&delimiter,"delimiter","d","","key delimiter")
}

func cloneBucket(cmd *cobra.Command,args []string) {

	var (
		start        = utils.LumberPrefix(cmd)
		N= 0
		retryNumber int = 0
		waitTime time.Duration = 0
		total,size  int64 = 0,0
		frDate time.Time


   )

	if len(srcBucket) == 0 {
		if  len( viper.GetString("clone.source.bucket")) == 0 {
			gLog.Warning.Printf("%s", "missing source bucket")
			utils.Return(start)
			return
		} else {
			srcBucket = viper.GetString("clone.source.bucket")
		}
	}

	if len(tgtBucket) == 0 {
		if  len(viper.GetString("clone.target.bucket")) == 0 {
			gLog.Warning.Printf("%s", "missing target bucket")
			utils.Return(start)
			return
		} else {
			tgtBucket = viper.GetString("clone.target.bucket")
		}
	}

	source := datatype.CreateSession{
		Region : viper.GetString("clone.source.region"),
		EndPoint : viper.GetString("clone.source.url"),
		AccessKey : viper.GetString("clone.source.access_key_id"),
		SecretKey : viper.GetString("clone.source.secret_access_key"),
	}

	target := datatype.CreateSession{
		Region : viper.GetString("clone.target.region"),
		EndPoint : viper.GetString("clone.target.url"),
		AccessKey : viper.GetString("clone.target.access_key_id"),
		SecretKey : viper.GetString("clone.target.secret_access_key"),
	}

	if target.EndPoint == source.EndPoint {
		if tgtBucket== srcBucket{
			gLog.Warning.Printf("Reject because source bucket: %s == target bucket: %s",srcBucket,tgtBucket)
			return
		}
	}

	if frDate, err = time.Parse(time.RFC3339, fromDate); err != nil {
		gLog.Error.Printf("Wrong date format %s", toDate)
		return
	}


	waitTime = utils.GetWaitTime(*viper.GetViper());
	retryNumber =utils.GetRetryNumber(*viper.GetViper())
	gLog.Info.Printf("Cloning from date %v",frDate)
	gLog.Info.Printf("Source: %v - Target: %v ",source,target)
	gLog.Info.Printf("Retry Options:  Waitime: %v  Retrynumber: %d ",waitTime,retryNumber)

	list := datatype.ListObjRequest{
		Service : s3.New(api.CreateSession2(source)),
		Bucket: srcBucket,
		Prefix : prefix,
		MaxKey : maxKey,
		Marker : marker,
	}
	svc3 := s3.New(api.CreateSession2(target))
	for {
		var (
			nextmarker string
			result     *s3.ListObjectsOutput
			err        error
		)
		N++
		if result, err = api.ListObject(list); err == nil {
			if l := len(result.Contents); l > 0 {
				var wg1 sync.WaitGroup
				//wg1.Add(len(result.Contents))
				for _, v := range result.Contents {
					// gLog.Trace.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size ,v.LastModified)
					if (v.LastModified.After(frDate)) {

						svc := list.Service
						wg1.Add(1)
						total += 1
						size += *v.Size
						get := datatype.GetObjRequest{
							Service: svc,
							Bucket:  list.Bucket,
							Key:     *v.Key,
						}
						go func(request datatype.GetObjRequest) {

							defer wg1.Done()

							for r1 := 0; r1 <= retryNumber; r1++ {
								robj := getObj(request)
								if robj.Err == nil {
									// write object
									// meta:= robj.Metadata
									put := datatype.PutObjRequest3{
										Service:  svc3,
										Bucket:   tgtBucket,
										Key:      robj.Key,
										Buffer:   robj.Body,
										Metadata: robj.Metadata,
									}
									gLog.Trace.Printf("Key %s  - Bucket %s - size: %d ", put.Key, put.Bucket, put.Buffer.Len())
									utils.PrintMetadata(put.Metadata)

									for r2 := 0; r2 <= retryNumber; r2++ {
										r, err := api.PutObject3(put)
										if err == nil {
											gLog.Trace.Printf("Etag: %s - Version id: %s ", r.ETag, r.VersionId)
											break /* break r2*/
										} else {
											gLog.Error.Printf("PutObj: %s - Error: %v - Retry: %d", robj.Key, err, r2)
											time.Sleep(waitTime * time.Millisecond)
										}
									}
									break /*  break r1 */
								} else {
									gLog.Error.Printf("GetObj: %s - Error: %v - Retry: %d", robj.Key, err, r1)
									time.Sleep(waitTime * time.Millisecond)
								}
							}

						}(get)
					}
				}
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
				}
				wg1.Wait()
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}
		if !*result.IsTruncated {

			gLog.Info.Printf("Total number of objects cloned: %d /size(KB): %.2f",total,float64(size/(1024.0)))
			return
		} else {
			list.Marker = nextmarker
		}
		if maxLoop != 0 && N > maxLoop {
			gLog.Info.Printf("Total number of objects cloned: %d / size(KB): %.2f",total,float64(size/(1024.0)))
			return
		}
	}
}

func getObj(request datatype.GetObjRequest) (datatype.Robj){

	robj := datatype.Robj{
		Key : request.Key,
	}
	if result,err := api.GetObject(request); err == nil {
		b, err := utils.ReadObject(result.Body)
		if err == nil {
			// gLog.Info.Printf("Key: %s  - ETag: %s  - Content length: %d - Object lenght: %d",key,*result.ETag,*result.ContentLength,b.Len())
			robj.Body= b
			robj.Metadata =result.Metadata
		}
		robj.Err = err
	} else {
		robj.Err = err
	}
	return robj
}

