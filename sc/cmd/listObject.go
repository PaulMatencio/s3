
package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jcelliott/lumber"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"time"
)

// listObjectCmd represents the listObject command
var (
	loshort = "Command to list the objects of a bucket"
	listObjectCmd = &cobra.Command{
		Use:   "listObj",
		Short: loshort,
		Long: ``,
		Run: listObject,
	}

	loCmd = &cobra.Command{
		Use:   "lo",
		Short: loshort,
		Long: ``,
		Hidden: true,
		Run: listObject,
	}
)

var (
	prefix string
	maxKey  int64
	marker  string
	delimiter string
)

func initLoFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maxmimum number of keys")
	cmd.Flags().StringVarP(&marker,"marker","M","","list starts with this key")
	cmd.Flags().StringVarP(&delimiter,"delimiter","d","","list delimiter")
}

func init() {

	rootCmd.AddCommand(listObjectCmd)
	rootCmd.AddCommand(loCmd)
	rootCmd.MarkFlagRequired("bucket")
	initLoFlags(listObjectCmd)
	initLoFlags(loCmd)
}

func listObject(cmd *cobra.Command,args []string) {
	start:= time.Now()
	utils.LumberPrefix(cmd)

	if len(bucket) == 0 {
		lumber.Warn(missingBucket)
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

	if result,err := api.ListObject(req); err == nil {

		if l := len(result.Contents); l > 0 {

			for _, v := range result.Contents {
				lumber.Info("Key: %s - Size: %d ", *v.Key, *v.Size)
			}

			if *result.IsTruncated {

				nextmarker := *result.Contents[l-1].Key
				lumber.Info("Truncated %v  - Next marker : %s ",*result.IsTruncated, nextmarker)
			}

		} else {
			lumber.Info("List returns no object from %s",bucket)
		}
	} else {
		lumber.Error("%v",err)
	}
}