

package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jcelliott/lumber"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
)

// listBucketCmd represents the listBucket command
var (
	lbshort = "Command to list all your buckets"
	listBucketCmd = &cobra.Command {
		Use:   "listBucket",
		Short: lbshort,
		Long: ``,
		Run: listBucket,
	}
	lbCmd = &cobra.Command {
		Use:   "lb",
		Short: lbshort,
		Long: ``,
		Hidden: true,
		Run: listBucket,
	}
)



func init() {

	rootCmd.AddCommand(listBucketCmd)
	rootCmd.AddCommand(lbCmd)
}

func listBucket(cmd *cobra.Command,args []string) {

	utils.LumberPrefix(cmd)

	req := datatype.ListBucketRequest{
		Service:  s3.New(api.CreateSession()),
	}
	if result,err := api.ListBuckets(req); err != nil {
		lumber.Error("%v",err)
	} else {
		lumber.Info("Ownerof the bucket: %s", result.Owner)
		for _, v := range result.Buckets {
			lumber.Info("Bucket Name: %s - Creation date: %s", *v.Name, v.CreationDate)
		}
	}

}