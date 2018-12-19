
package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/jcelliott/lumber"
	"github.com/s3/api"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
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
)

func initLoFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maxmimum number of keys")

}

func init() {

	rootCmd.AddCommand(listObjectCmd)
	rootCmd.AddCommand(loCmd)
	rootCmd.MarkFlagRequired("bucket")
	initLoFlags(listObjectCmd)
	initLoFlags(loCmd)
}

func listObject(cmd *cobra.Command,args []string) {

	utils.LumberPrefix(cmd)

	if len(bucket) == 0 {
		lumber.Warn(missingBucket)
		utils.Return()
		return
	}

	svc := s3.New(api.CreateSession())

	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
		MaxKeys: aws.Int64(maxKey),
	}

	// svc.ListObjectsRequest(input)

	if result, err := svc.ListObjects(input); err == nil {
		for _, v := range result.Contents {
			lumber.Info("Key: %s - Size: %d ", *v.Key, *v.Size)
		}
	} else {
		lumber.Error("%v",err)
	}

}