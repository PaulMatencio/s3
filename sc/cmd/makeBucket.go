
package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jcelliott/lumber"
	"github.com/s3/api"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
)

// makeBucketCmd represents the makeBucket command
var (
	mbshort = "Command to create a bucket"
	makeBucketCmd = &cobra.Command{
		Use:   "makeBucket",
		Short: mbshort,
		Long: ``,
		Run:makeBucket,
	}

	createBucketCmd = &cobra.Command{
		Use:   "createBucket",
		Short: mbshort,
		Long: ``,
		Run:makeBucket,
	}
	mbCmd = &cobra.Command{
		Use:   "mb",
		Short: mbshort,
		Long: ``,
		Hidden: true,
		Run:makeBucket,
	}

	cbCmd = &cobra.Command{
		Use:   "cb",
		Short: mbshort,
		Long: ``,
		Hidden: true,
		Run:makeBucket,
	}

)

func initMbFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name to get the object")
}

func init() {

	rootCmd.AddCommand(makeBucketCmd)
	rootCmd.AddCommand(mbCmd)
	initMbFlags(makeBucketCmd)
	initMbFlags(createBucketCmd)
	initMbFlags(mbCmd)
	initMbFlags(cbCmd)

}

func makeBucket(cmd *cobra.Command,args []string) (){

	utils.LumberPrefix(cmd)

	if len(bucket) == 0 {
		lumber.Warn("Missing bucket - please provide the bucket you 'd like to make")
		utils.Return()
		return
	}

	svc := s3.New(api.CreateSession())

	if _,err := api.MakeBucket(svc,bucket); err != nil {

		lumber.Error("Create Bucket fails [%v]",err)
	}

}