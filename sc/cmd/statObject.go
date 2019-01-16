package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
)

// getObjectCmd represents the getObject command
var (
	soshort = "Command to retrieve an object metadata"

	statObjectCmd = &cobra.Command {
		Use:   "statObj",
		Short: soshort,
		Long: ``,
		// Hidden: true,
		Run: statObject,
	}

	headObjCmd = &cobra.Command {
		Use:   "headObj",
		Short: soshort,
		Long: ``,
		// Hidden: true,
		Run: statObject,
	}

	statObjCmd = &cobra.Command {
		Use:   "ho",
		Short: soshort,
		Hidden: true,
		Long: ``,
		Run: statObject,
	}

)

func initHoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&key,"key","k","","the  key of the object you'd like to check")
	cmd.Flags().StringVarP(&odir,"odir","O","","the output directory relative to the home directory")
}

func init() {

	RootCmd.AddCommand(statObjectCmd)
	RootCmd.AddCommand(statObjCmd)
	RootCmd.AddCommand(headObjCmd)
	RootCmd.MarkFlagRequired("bucket")
	RootCmd.MarkFlagRequired("key")
	initHoFlags(statObjectCmd)
	initHoFlags(statObjCmd)
	initHoFlags(headObjCmd)


}


//  statObject utilizes the api to get object

func statObject(cmd *cobra.Command,args []string) {

	// handle any missing args
	utils.LumberPrefix(cmd)

	switch {

	case len(bucket) == 0:
		gLog.Warning.Printf("%s",missingBucket)
		return

	case len(key) == 0:
		gLog.Warning.Printf("%s",missingKey)
		return
	}

	var (

		req = datatype.StatObjRequest{
			Service:  s3.New(api.CreateSession()),
			Bucket: bucket,
			Key: key,
		}
		result, err = api.StatObject(req)
	)

	/* handle error */

	if err != nil {
		procS3Error(err)

	} else {

		procS3Meta(key, result.Metadata)
	}

}



