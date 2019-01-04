package cmd

import (
	"fmt"
	// "github.com/golang/gLog"
	"github.com/s3/gLog"
	"path/filepath"
	// "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"time"
)


var (
	getObjsShort = "Command to download multiple objects from a bucket"
	getobjectsCmd = &cobra.Command{
		Use:   "getObjects",
		Short: getObjsShort,
		Long: ``,
		Hidden: true,
		Run: downloadObjects,
	}

	getobjsCmd = &cobra.Command{
		Use:   "getObjs",
		Short: getObjsShort,
		Long: ``,
		Run: downloadObjects,
	}
)



func init() {

	rootCmd.AddCommand(getobjectsCmd)
	rootCmd.AddCommand(getobjsCmd)
	rootCmd.MarkFlagRequired("bucket")

	initLoFlags(getobjectsCmd)
	initLoFlags(getobjsCmd)
	getobjectsCmd.Flags().StringVarP(&odir,"odir","o","","the output directory relative to the home directory")
	getobjsCmd.Flags().StringVarP(&odir,"odir","o","","the output directory relative to the home directory")

}


func downloadObjects(cmd *cobra.Command,args []string) {

	var (
		start= utils.LumberPrefix(cmd)
		N,T = 0,0
		total int64 = 0
	)

	if len(bucket) == 0 {

		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	if len(odir) >0 {
		pdir = filepath.Join(utils.GetHomeDir(),odir)
		utils.MakeDir(pdir)
	}

	req := datatype.ListObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Prefix : prefix,
		MaxKey : maxKey,
		Marker : marker,
	}
	ch:= make(chan *datatype.Ro)

	var (
		nextmarker string
		result  *s3.ListObjectsOutput
		err error
		l  int
	)

	svc  := s3.New(api.CreateSession()) /* create a new service for downloading object*/

	for {

		if result, err = api.ListObject(req); err == nil {

			if l = len(result.Contents); l > 0 {

				N = len(result.Contents)
				total += int64(N)
				T = 0

				for _, v := range result.Contents {

					get := datatype.GetObjRequest{
						// Service: req.Service,
						Service: svc,
						Bucket:  req.Bucket,
						Key:     *v.Key,
					}
					go func(request datatype.GetObjRequest) {

						rd := datatype.Ro{
							Key : get.Key,
						}
						rd.Result, rd.Err = api.GetObjects(get)

						ch <- &rd

					}(get)

				}

				done:= false

				for ok:=true;ok;ok=!done {
					select {

					case rd := <-ch:
						T++
						procGetResult(rd)

						if T == N {
							gLog.Info.Printf("Getting %d objects ",N)
							done = true
						}

					case <-time.After(50 * time.Millisecond):
						fmt.Printf("w")
					}
				}

			}
		} else {
			gLog.Error.Printf("ListObjects err %v",err)
			break
		}

		if *result.IsTruncated {

			nextmarker = *result.Contents[l-1].Key
			gLog.Warning.Printf("Truncated %v  - Next marker : %s ", *result.IsTruncated, nextmarker)

		}

		if loop && *result.IsTruncated {
			req.Marker = nextmarker

		} else {
			gLog.Info.Printf("Total number of objects downloaded: %d",total)
			break
		}
	}

	utils.Return(start)
}

