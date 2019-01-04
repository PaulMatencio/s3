
package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/gLog"

	// "github.com/golang/gLog"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
)

// listObjectCmd represents the listObject command
var (
	loshort = "Command to list the objects in a specific bucket"
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
	delimiter string
	loop  bool
)

func initLoFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maxmimum number of keys")
	cmd.Flags().StringVarP(&marker,"marker","M","","list starts with this key")
	cmd.Flags().StringVarP(&delimiter,"delimiter","d","","list delimiter")
	cmd.Flags().BoolVarP(&loop,"loop","L",false,"loop with next marker")
}

func init() {

	rootCmd.AddCommand(listObjectCmd)
	rootCmd.AddCommand(loCmd)
	rootCmd.MarkFlagRequired("bucket")
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
					gLog.Info.Printf("Key: %s - Size: %d ", *v.Key, *v.Size)
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