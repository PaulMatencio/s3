package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/gLog"
	"github.com/spf13/viper"

	// "github.com/golang/gLog"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
)

// listObjectCmd represents the listObject command
var (
	loshort = "Command to list multiple objects of a given bucket"
	listObjectCmd = &cobra.Command{
		Use:   "lsObjs",
		Short: loshort,
		Long: ``,
		// Hidden: true,
		Run: listObject,
	}

	loCmd = &cobra.Command{
		Use:   "lo",
		Short: loshort,
		Hidden: true,
		Long: ``,
		Run: listObject,
	}
)

var (
	prefix string
	maxKey  int64
	marker  string
	maxLoop int
	delimiter string
	loop,full,R  bool
)

func initLoFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker,"marker","M","","start processing from this key")
	cmd.Flags().StringVarP(&delimiter,"delimiter","d","","key delimiter")
	cmd.Flags().BoolVarP(&loop,"loop","L",false,"loop until all keys are processed")
	// cmd.Flags().BoolVarP(&,"maxLoop","",false,"maximum number of loop")
	cmd.Flags().BoolVarP(&full,"fullKey","F",false,"given prefix is a full documemt key")

	cmd.Flags().BoolVarP(&R,"reverse","R",false,"Reverse the prefix")

}

func init() {

	RootCmd.AddCommand(listObjectCmd)
	RootCmd.AddCommand(loCmd)
	RootCmd.MarkFlagRequired("bucket")
	initLoFlags(listObjectCmd)
	initLoFlags(loCmd)
}

func listObject(cmd *cobra.Command,args []string) {
	var (
		start = utils.LumberPrefix(cmd)
		total int64 = 0
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}
	if full {
		bucket = bucket +"-"+fmt.Sprintf("%02d",utils.HashKey(prefix,bucketNumber))
	}

	if R {
		prefix = utils.Reverse(prefix)
	}
	req := datatype.ListObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Prefix : prefix,
		MaxKey : maxKey,
		Marker : marker,
		Delimiter: delimiter,
	}
	for {
		var (
			nextmarker string
			result  *s3.ListObjectsOutput
			err error
		)
		if result, err = api.ListObject(req); err == nil {

			if l := len(result.Contents); l > 0 {
				total += int64(l)
				for _, v := range result.Contents {
					gLog.Info.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size,v.LastModified)
				}

				if *result.IsTruncated {

					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Truncated %v  - Next marker : %s ", *result.IsTruncated, nextmarker)
				}


			}
			/*else {
				gLog.Warning.Printf("List returns no object from %s", bucket)
			}
			*/
		} else {
			gLog.Error.Printf("%v", err)
			break
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

func ListObjRepStatus(cmd *cobra.Command,args []string) {
	var url string
	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}
	if url = utils.GetLevelDBUrl(*viper.GetViper()); len(url) == 0 {
		gLog.Warning.Printf("levelDB url is missing")
		return
	}
	var (
		nextMarker string
		req        = datatype.ListObjLdbRequest{
			Url:       url,
			Bucket:    bucket,
			Prefix:    prefix,
			MaxKey:    maxKey,
			Marker:    marker,
			Delimiter: delimiter,
		}
		s3Meta = datatype.S3Metadata{}
		N = 0
	)
	for {
		if result, err := api.ListObjectLdb(req); err != nil {
			if err != nil {
				gLog.Error.Println(err)
			} else {
				gLog.Info.Println("Result is empty")
			}
		} else {

			if err = json.Unmarshal([]byte(result.Contents), &s3Meta); err == nil {
				//gLog.Info.Println("Key:",s3Meta.Contents[0].Key,s3Meta.Contents[0].Value.XAmzMetaUsermd)
				//num := len(s3Meta.Contentss3Meta.Contents)
				// l := len(s3Meta.Contents)
				for _, c := range s3Meta.Contents {
					//m := &s3Meta.Contents[i].Value.XAmzMetaUsermd
					repInfo := &c.Value.ReplicationInfo
					lastModified := &c.Value.LastModified
					gLog.Info.Printf("Key: %s - Last Modified %v  - replication info status  %v ", c.Key, repInfo,lastModified)
				}
				N++
			} else {
					gLog.Info.Println(err)
			}

			if !s3Meta.IsTruncated {
				return
			} else {
				marker = nextMarker
				gLog.Info.Printf("marker %s", marker)
			}
			if N >= maxLoop {
				return
			}
		}
	}
}