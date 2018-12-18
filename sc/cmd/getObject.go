
package cmd

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jcelliott/lumber"
	"github.com/s3/api"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
)

// getObjectCmd represents the getObject command
var (
	goshort = "Command to retrieve an object"
	getObjectCmd = &cobra.Command {
		Use:   "getObject",
		Short: goshort,
		Long: ``,
		Run: getObject,
	}

	getObjCmd = &cobra.Command {
		Use:   "go",
		Short: goshort,
		Long: ``,
		Run: getObject,
	}

)

func initGoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name to get the object")
	cmd.Flags().StringVarP(&key,"key","k","","the  key of the object")
}

func init() {

	rootCmd.AddCommand(getObjectCmd)
	rootCmd.AddCommand(getObjCmd)
	rootCmd.MarkFlagRequired("bucket")
	rootCmd.MarkFlagRequired("key")
	initGoFlags(getObjectCmd)
	initGoFlags(getObjCmd)


}


//  getObject utilizes the api to get object

func getObject(cmd *cobra.Command,args []string) {

	// handle any missing args
	utils.LumberPrefix(cmd)

	switch {

	case len(bucket) == 0:
		lumber.Warn("Missing bucket - please provide the bucket for object you'd like to get")
		return

	case len(key) == 0:
		lumber.Warn("Missing key - please provide the key for object you'd like to get")
		return
	}

	svc := s3.New(api.CreateSession())

    result, err := api.GetObjects(svc, bucket, key)

	/* handle error */

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				lumber.Warn("Error: [%v]  Error: [%v]",s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				lumber.Error("error [%v]",aerr.Error())
			}
		} else {
			lumber.Error("[%v]",err.Error())
		}
		return
	}


	lumber.Info("Key %s ETag: %s  Content-Length:%d  Meta [%v]",key,*result.ETag,*result.ContentLength,result.Metadata)
	for k,v := range result.Metadata {
		lumber.Info("Key %s - Metadata %s : %s",key, k,*v)
	}

}

