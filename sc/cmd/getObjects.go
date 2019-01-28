package cmd

import (
	"fmt"
	"runtime"
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
	getObjsShort = "Command to download concurrently nultiple  objects and their metadata to a given directory"
	getobjectsCmd = &cobra.Command{
		Use:   "getObjects",
		Short: getObjsShort,
		Long: ``,
		Hidden: true,
		Run: getObjects,
	}

	getobjsCmd = &cobra.Command{
		Use:   "getObjs",
		Short: getObjsShort,
		Long: ``,
		Run: getObjects,
	}
)



func init() {

	RootCmd.AddCommand(getobjectsCmd)
	RootCmd.AddCommand(getobjsCmd)
	RootCmd.MarkFlagRequired("bucket")

	initLoFlags(getobjectsCmd)
	initLoFlags(getobjsCmd)
	getobjectsCmd.Flags().StringVarP(&odir,"odir","O","","the output directory relative to the home directory you'd like to save")
	getobjsCmd.Flags().StringVarP(&odir,"odir","O","","the output directory relative to the home directory you's like to save")

}


func getObjects(cmd *cobra.Command,args []string) {

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

	//

	if profiling >0  {
		go func() {
			for {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				// debug.FreeOSMemory()
				gLog.Info.Printf("PROFILING: System memory %d MB",float64(m.Sys) / 1024 / 1024)
				gLog.Info.Printf("PROFILING: Heap allocation %d MB",float64(m.HeapAlloc) / 1024 / 1024)
				gLog.Info.Printf("PROFILING: Total allocation %d MB",float64(m.TotalAlloc) / 1024 / 1024)
				time.Sleep(time.Duration(profiling) * time.Second)

			}
		}()
	}

	req := datatype.ListObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Prefix : prefix,
		MaxKey : maxKey,
		Marker : marker,
	}
	// ch:= make(chan *datatype.Ro)
	ch:= make(chan int)
	var (
		nextmarker string
		result  *s3.ListObjectsOutput
		err error
		l  int
	)

	// svc  := s3.New(api.CreateSession()) /* create a new service for downloading object*/

	for {

		if result, err = api.ListObject(req); err == nil {

			if l = len(result.Contents); l > 0 {

				N = len(result.Contents)
				total += int64(N)
				T = 0

				for _, v := range result.Contents {

					get := datatype.GetObjRequest{
						Service: req.Service,
						// Service: svc,
						Bucket:  req.Bucket,
						Key:     *v.Key,
					}
					go func(request datatype.GetObjRequest) {

						ro := datatype.Ro{
							Key : get.Key,
						}
						ro.Result, ro.Err = api.GetObject(get)
						procGetResult(&ro)
						get = datatype.GetObjRequest{} // reset the get structure for GC
						// ch <- &ro
						ch <- 1
					}(get)
				}

				done:= false

				for ok:=true;ok;ok=!done {
					select {
					// case rg := <-ch:
					case  <-ch:
						T++
						// procGetResult(rg)

						if T == N {
							gLog.Info.Printf("%d objects are downloaded from bucket %s",N,bucket)
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

