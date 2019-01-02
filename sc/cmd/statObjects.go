package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	// "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"time"
)


var (
	gmshort = "Command to retieve multiple objects metadata"
	gmetaCmd = &cobra.Command{
		Use:   "statobjects",
		Short: gmshort,
		Long: ``,
		Hidden: true,
		Run: statObjects,
	}

	gmCmd = &cobra.Command{
		Use:   "statobjs",
		Short: gmshort,
		Long: ``,
		Run: statObjects,
	}



)

type  Rd struct {
	Key string
	Result   *s3.HeadObjectOutput
	Err error
}

func init() {

	rootCmd.AddCommand(gmetaCmd)
	rootCmd.AddCommand(gmCmd)
	rootCmd.MarkFlagRequired("bucket")

	initLoFlags(gmetaCmd)
	initLoFlags(gmCmd)
	gmetaCmd.Flags().StringVarP(&odir,"odir","o","","the output directory relative to the home directory")
	gmCmd.Flags().StringVarP(&odir,"odir","o","","the output directory relative to the home directory")
}


func statObjects(cmd *cobra.Command,args []string) {

	var (
		start= time.Now()
		N,T = 0,0
	)

	utils.LumberPrefix(cmd)

	if len(bucket) == 0 {

		log.Warn(missingBucket)
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
	ch:= make(chan *Rd)
	var (
		nextmarker string
		result  *s3.ListObjectsOutput
		err error
		// rd Rd
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
					head := datatype.StatObjRequest{
						Service: req.Service,
						Bucket:  req.Bucket,
						Key:     *v.Key,
					}
					go func(request datatype.StatObjRequest) {
						rd := Rd{
							Key : head.Key,
						}
						rd.Result, rd.Err = api.StatObjects(head)
						// rd.Key = head.Key
						//head = datatype.StatObjRequest{}
						ch <- &rd

					}(head)

				}

				done:= false

				for ok:=true;ok;ok=!done {
					select {
					case rd := <-ch:
						T++
						if rd.Err != nil {
							log.Error("Error %v getting %s metadata", rd.Err, rd.Key)
						} else {
							// formatting metadata
							if len(odir) == 0 {
								printUserMeta(rd)
							} else {
								writeUserMetadata(rd)
							}
							rd = &Rd{}
						}
						if T == N {
							//utils.Return(start)
							log.Info("Getting metadata of %d objects ",N)
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

func printUserMeta(rd *Rd) {
	meta := rd.Result.Metadata
	for k,v := range meta {
		log.Info("Key %s - Metadata (k=v) %s=%s",key, k,*v)
	}

	if usermd,err  := utils.GetUserMeta(meta); err == nil {
		log.Info("key:%s - User Metadata: %s", usermd)
	}
}

func writeUserMetadata(rd  *Rd ) {

	var (
		usermd string
		err    error

		pathname = filepath.Join(pdir,strings.Replace(rd.Key,string(os.PathSeparator),"_",-1)+".md")
		)

	if usermd,err  = utils.GetUserMeta(rd.Result.Metadata); err == nil {
		if err:= ioutil.WriteFile(pathname,[]byte(usermd),0644); err != nil {
			log.Error("Error %v writing %s ",err,pathname)
		}
	}

}