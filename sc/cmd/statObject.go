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
	soshort = "Command to retrieve an object metadata"

	statObjectCmd = &cobra.Command {
		Use:   "statObj",
		Short: soshort,
		Long: ``,
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
		Long: ``,
		Hidden: true,
		Run: statObject,
	}

)

func initHoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name to get the object")
	cmd.Flags().StringVarP(&key,"key","k","","the  key of the object")
}

func init() {

	rootCmd.AddCommand(statObjectCmd)
	rootCmd.AddCommand(statObjCmd)
	rootCmd.AddCommand(headObjCmd)
	rootCmd.MarkFlagRequired("bucket")
	rootCmd.MarkFlagRequired("key")
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
		lumber.Warn(missingBucket)
		return

	case len(key) == 0:
		lumber.Warn(missingKey)
		return
	}

	var (
		svc = s3.New(api.CreateSession())
		result, err = api.StatObjects(svc, bucket, key)
	)

	/* handle error */

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				lumber.Warn("Error: [%v] -  Error: [%v]",s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				lumber.Error("error [%v]",aerr.Error())
			}
		} else {
			lumber.Error("[%v]",err.Error())
		}
		return
	}


	lumber.Info("Key %s - ETag: %s - Content length:%d - Meta [%v]",key,*result.ETag,*result.ContentLength,result.Metadata)
	for k,v := range result.Metadata {
		lumber.Info("Key %s - Metadata (k=v) %s=%s",key, k,*v)
	}

	if usermd,err  := utils.GetuserMeta(result.Metadata); err == nil {
		lumber.Info("key:%s - User Metadata: %s", usermd)
	}


}

