package cmd

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"path/filepath"
	"time"
)


var (
	gmshort = "Command to retieve some of the metadata of specific or every object in the bucket"
	gmetaCmd = &cobra.Command{
		Use:   "statObjects",
		Short: gmshort,
		Long: ``,
		Hidden: true,
		Run: statObjects,
	}

	gmCmd = &cobra.Command{
		Use:   "statObjs",
		Short: gmshort,
		Long: ``,
		Run: statObjects,
	}

	hmCmd = &cobra.Command{
		Use:   "headObjs",
		Short: gmshort,
		Long: ``,
		Run: statObjects,
	}

)
/*
type  Rd struct {
	Key string
	Result   *s3.HeadObjectOutput
	Err error
}
*/

func init() {

	RootCmd.AddCommand(gmetaCmd)
	RootCmd.AddCommand(gmCmd)
	RootCmd.AddCommand(hmCmd)
	RootCmd.MarkFlagRequired("bucket")

	initLoFlags(gmetaCmd)
	initLoFlags(gmCmd)
	initLoFlags(hmCmd)
	gmetaCmd.Flags().StringVarP(&odir,"odir","O","","the output directory relative to the home directory")
	gmCmd.Flags().StringVarP(&odir,"odir","O","","the output directory relative to the home directory")
}


func statObjects(cmd *cobra.Command,args []string) {

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
	ch:= make(chan *datatype.Rh)
	var (
		nextmarker string
		result  *s3.ListObjectsOutput
		err error
		// rd Rd
		l  int
	)

    svc  := s3.New(api.CreateSession()) // create another service point for  getting metadata

	for {

		if result, err = api.ListObject(req); err == nil {

			if l = len(result.Contents); l > 0 {

				N = len(result.Contents)
				total += int64(N)
				T = 0

				for _, v := range result.Contents {

					head := datatype.StatObjRequest{
						// Service: req.Service,
						Service: svc,
						Bucket:  req.Bucket,
						Key:     *v.Key,
					}
					go func(request datatype.StatObjRequest) {

						rd := datatype.Rh{
							Key : head.Key,
						}
						rd.Result, rd.Err = api.StatObject(head)

						ch <- &rd

					}(head)

				}

				done:= false

				for ok:=true;ok;ok=!done {

					select {
					case rd := <-ch:
						T++
						procStatResult(rd)
						if T == N {
							gLog.Info.Printf("Getting metadata of %d objects ",N)
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
			gLog.Info.Printf("Total number of objects returned: %d",total)
			break
		}
	}

	utils.Return(start)
}

