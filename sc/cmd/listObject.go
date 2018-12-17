
package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/jcelliott/lumber"
	"github.com/s3/api"
	"github.com/spf13/cobra"
)

// listObjectCmd represents the listObject command
var listObjectCmd = &cobra.Command{
	Use:   "listObject",
	Short: "list Objects",
	Long: ``,
	Run: listObject,
}

var (
	prefix string
	limit  int64
)

func init() {
	rootCmd.AddCommand(listObjectCmd)
	listObjectCmd.Flags().StringVar(&bucket,"b","","the bucket name")
	listObjectCmd.Flags().StringVar(&prefix,"prefix","","key prefix")
	listObjectCmd.Flags().Int64Var(&limit,"limit",100,"limit the maxmimum number of objects")
}

func listObject(cmd *cobra.Command,args []string) {

	lumber.Prefix(cmd.Name())

	if len(bucket) == 0 {
		lumber.Warn("Missing bucket - please provide the bucket for objects you'd like to list")
	}

	svc := s3.New(api.CreateSession())

	input := &s3.ListObjectsInput{

		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
		MaxKeys: aws.Int64(limit),
	}
	// svc.ListObjectsRequest(input)
	if result, err := svc.ListObjects(input); err == nil {
		for _, v := range result.Contents {
			lumber.Info("Key: %s - Size: %d ", *v.Key, *v.Size)
		}
	} else {
		lumber.Error("%v",err)
	}
	lumber.Prefix("[sc]")
}