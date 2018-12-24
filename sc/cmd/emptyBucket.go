
package cmd

import (
	"fmt"
	// "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"time"
)


var (
	ebshort = "Command to delete multiple objects"
	eBucketCmd = &cobra.Command{
		Use:   "deleteObjects",
		Short: ebshort,
		Long: ``,
		Hidden: true,
		Run: deleteObjects,
	}

	ebCmd = &cobra.Command{
		Use:   "rmo",
		Short: ebshort,
		Long: ``,
		Run: deleteObjects,
	}

	ebCmd1 = &cobra.Command{
		Use:   "dmo",
		Short: ebshort,
		Long: ``,
		Run: deleteObjects,
	}
)


func init() {

	rootCmd.AddCommand(eBucketCmd)
	rootCmd.AddCommand(ebCmd)
	rootCmd.AddCommand(ebCmd1)
	rootCmd.MarkFlagRequired("bucket")

	initLoFlags(eBucketCmd)
	initLoFlags(ebCmd)
	initLoFlags(ebCmd1)
}

func deleteObjects(cmd *cobra.Command,args []string) {

	var (
		start= time.Now()
		N,T = 0,0
	)

	type  Rd struct {
		Key string
		Result   *s3.DeleteObjectOutput
		Err error
	}

	utils.LumberPrefix(cmd)

	if len(bucket) == 0 {

		log.Warn(missingBucket)
		utils.Return(start)
		return
	}

	req := datatype.ListObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Prefix : prefix,
		MaxKey : maxKey,
		Marker : marker,
	}
	ch:= make(chan *Rd)
	var (
		nextmarker string
		result  *s3.ListObjectsOutput
		err error
		rd Rd
		l  int
	)
	for {

		if result, err = api.ListObject(req); err == nil {

			if l = len(result.Contents); l > 0 {

				N = len(result.Contents)
				T = 0

				for _, v := range result.Contents {
					//lumber.Info("Key: %s - Size: %d ", *v.Key, *v.Size)
					//  delete the object
					del := datatype.DeleteObjRequest{
						Service: req.Service,
						Bucket:  req.Bucket,
						Key:     *v.Key,
					}
					go func(request datatype.DeleteObjRequest) {
						rd.Result, rd.Err = api.DeleteObjects(del)
						rd.Key = del.Key
						ch <- &rd

					}(del)

				}

				done:= false
				for ok:=true;ok;ok=!done {
					select {
					case rd := <-ch:
						T++
						if rd.Err != nil {
							log.Error("Error %v deleting %s", rd.Err, rd.Key)
						} else {
							// lumber.Trace("Key %s is deleted", rd.Key)
						}
						if T == N {
							//utils.Return(start)
							log.Info("Deleting .... %d objects ",N)
							done = true
						}
					case <-time.After(50 * time.Millisecond):
						fmt.Printf("w")
					}
				}

			}  else {
				log.Info("Bucket %s is empty", bucket)
			}
		} else {
			log.Error("ListObjects err %v",err)
			break
		}

		if *result.IsTruncated {

			nextmarker = *result.Contents[l-1].Key
			log.Info("Truncated %v  - Next marker : %s ", *result.IsTruncated, nextmarker)

		}

		if loop && *result.IsTruncated {
			req.Marker = nextmarker

		} else {
			break
		}
	}

	utils.Return(start)
}

