
package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jcelliott/lumber"
	"github.com/s3/api"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
)

// deleteBucketCmd represents the deleteBucket command
var (
	dbshort =  "Command to delete a bucket"

	deleteBucketCmd = &cobra.Command{
		Use:   "deleteBucket",
		Short: dbshort,
		Long: ``,
		Run:deleteBucket,
	}

	removeBucketCmd = &cobra.Command{
		Use:   "removeBucket",
		Short: dbshort,
		Long: ``,
		Run:deleteBucket,
	}
	rbCmd = &cobra.Command{
		Use:   "db",
		Short: dbshort,
		Long: ``,
		Run:deleteBucket,
	}

	dbCmd = &cobra.Command{
		Use:   "rb",
		Short: dbshort,
		Long: ``,
		Run:deleteBucket,
	}

)

func initRbFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name to get the object")
}

func init() {

	rootCmd.AddCommand(deleteBucketCmd)
	rootCmd.AddCommand(dbCmd)
	rootCmd.AddCommand(removeBucketCmd)
	rootCmd.AddCommand(rbCmd)
	initRbFlags(deleteBucketCmd)
	initRbFlags(removeBucketCmd)
	initRbFlags(rbCmd)
	initRbFlags(dbCmd)

}

func deleteBucket(cmd *cobra.Command,args []string) (){

	utils.LumberPrefix(cmd)
	if len(bucket) == 0 {
		lumber.Warn("Missing bucket - please provide the bucket you'd like to delete")
		return
	}
	svc := s3.New(api.CreateSession())
	if _,err := api.DeleteBucket(svc,bucket); err != nil {
		lumber.Error("Delete bucket fails  [%v]",err)
	}

}