
package cmd

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jcelliott/lumber"
	"github.com/s3/api"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

// getObjectCmd represents the getObject command
var (
	goshort = "Command to retrieve an object"
	output   string
	getObjectCmd = &cobra.Command {
		Use:   "getObj",
		Short: goshort,
		Long: ``,
		Run: getObject,
	}

	getObjCmd = &cobra.Command {
		Use:   "go",
		Short: goshort,
		Long: ``,
		Hidden:true,
		Run: getObject,
	}

	fgetObjCmd = &cobra.Command {
		Use:   "fGetObj",
		Short: goshort,
		Long: ``,
		Run: fGetObject,
	}
)

func initGoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name to get the object")
	cmd.Flags().StringVarP(&key,"key","k","","the  key of the object")
}

func initPoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name to get the object")
	cmd.Flags().StringVarP(&key,"key","k","","the  key of the object")
	cmd.Flags().StringVarP(&output,"output","o","","the ouput directory you'like to save")

}

func init() {

	rootCmd.AddCommand(getObjectCmd)
	rootCmd.AddCommand(getObjCmd)
	rootCmd.AddCommand(fgetObjCmd)
	rootCmd.MarkFlagRequired("bucket")
	rootCmd.MarkFlagRequired("key")
	initGoFlags(getObjectCmd)
	initGoFlags(getObjCmd)
	initPoFlags(fgetObjCmd)


}


//  getObject utilizes the api to get object

func getObject(cmd *cobra.Command,args []string) {
	var usermd string
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

	// create an S3 session
	svc := s3.New(api.CreateSession())
	// get the object
    result, err := api.GetObjects(svc, bucket, key)

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
	} else {
		usermd, err = utils.GetuserMeta(result.Metadata)
		lumber.Info("Key: %s - User meta: %s ",key, usermd)
		b, err := utils.ReadObject(result.Body)
		if err == nil {
			lumber.Info("Key: %s  - ETag: %s  - Content length: %d - Object lenght: %d",key,*result.ETag,*result.ContentLength,b.Len())
		}

	}

}

func fGetObject(cmd *cobra.Command,args []string) {

	var (

		err  error
		result *s3.GetObjectOutput
		usermd string

	)
	// handle any missing args
	utils.LumberPrefix(cmd)


	switch {

	case len(bucket) == 0:
		lumber.Warn("Missing bucket - please provide the bucket for object you'd like to get")
		return

	case len(key) == 0:
		lumber.Warn("Missing key - please provide the key for object you'd like to get")
		return

	case len(output) == 0:
		lumber.Warn("Missing output directory - please provide the output directory you'd like to save the object")
		return
	}

    // Make the output directory if it does not exist

	if _,err := os.Stat(output); os.IsNotExist(err) {
		os.MkdirAll(output,0755)
	}
	pathname := output + string(os.PathSeparator) + strings.Replace(key,string(os.PathSeparator),"_",-1)
	//  create an S3 session
	svc := s3.New(api.CreateSession())
	// get the object
	result, err = api.GetObjects(svc, bucket, key);
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
	} else {
		usermd, err = utils.GetuserMeta(result.Metadata)
		lumber.Info("Object: %s - User meta: %s ",key,usermd)
		if err = saveObject(result,pathname); err == nil {
			lumber.Info("Object %s is downloaded to %s",key,pathname)
		} else {
			lumber.Error("Saving %s Error %v ",key,err)
		}
	}
}



func saveObject(result *s3.GetObjectOutput, pathname string) (error) {

	var (
		err error
		f  *os.File
	)
	if f,err = os.Create(pathname); err == nil {
		 _,err = io.Copy(f, result.Body);
	}
	return err
}


func writeObj(b *bytes.Buffer) {

	pathname := output + string(os.PathSeparator) + strings.Replace(key,string(os.PathSeparator),"_",-1)
	if err:= ioutil.WriteFile(pathname,b.Bytes(),0644); err == nil {
		lumber.Info("Object %s is downloaded to %s",key,pathname)
	} else {
		lumber.Info("Error %v downloading object %s",err,key)
	}
}