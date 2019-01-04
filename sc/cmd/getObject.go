package cmd

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/glog"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strings"
)

// getObjectCmd represents the getObject command
var (
	goshort = "Command to get an object"
	// odir   string
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
		Hidden: true,
		Run: getObject,
	}

	fgetObjCmd = &cobra.Command {
		Use:   "fgetObj",
		Short: "Command to download an objet to a file",
		Long: ``,
		Run: fGetObject,
	}
)

func initGoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name to get the object")
	cmd.Flags().StringVarP(&key,"key","k","","the  key of the object")
}

func initFgoFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name to get the object")
	cmd.Flags().StringVarP(&key,"key","k","","the  key of the object")
	cmd.Flags().StringVarP(&odir,"odir","o","","the ouput directory you'like to save")

}

func init() {

	rootCmd.AddCommand(getObjectCmd)
	rootCmd.AddCommand(getObjCmd)
	rootCmd.AddCommand(fgetObjCmd)
	rootCmd.MarkFlagRequired("bucket")
	rootCmd.MarkFlagRequired("key")
	initGoFlags(getObjectCmd)
	initGoFlags(getObjCmd)
	initFgoFlags(fgetObjCmd)


}


//  getObject utilizes the api to get object

func getObject(cmd *cobra.Command,args []string) {
	var usermd string
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


	req := datatype.GetObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Key : key,
	}
	result, err := api.GetObjects(req)

	if err != nil {

		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				gLog.Warning.Printf("Error: [%v]  Error: [%v]",s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				gLog.Error.Printf("error [%v]",aerr.Error())
			}
		} else {
			gLog.Error.Printf("[%v]",err.Error())
		}
	} else {
		usermd, err = utils.GetUserMeta(result.Metadata)
		gLog.Info.Printf("Key: %s - User meta: %s ",key, usermd)
		b, err := utils.ReadObject(result.Body)
		if err == nil {
			gLog.Info.Printf("Key: %s  - ETag: %s  - Content length: %d - Object lenght: %d",key,*result.ETag,*result.ContentLength,b.Len())
		}
	}
}

func fGetObject(cmd *cobra.Command,args []string) {

	var (

		err  error
		result *s3.GetObjectOutput
		usermd string
		start = utils.LumberPrefix(cmd)

	)
	// handle any missing args



	switch {

	case len(bucket) == 0:
		glog.Warningf("%s",missingBucket)
		return

	case len(key) == 0:
		glog.Warningf("%s",missingKey)
		return

	case len(odir) == 0:
		glog.Warningf("%s",missingOutputFolder)
		return
	}

	// Make the output directory if it does not exist
	pdir = filepath.Join(utils.GetHomeDir(),odir)
	utils.MakeDir(pdir)

	pathname := filepath.Join(pdir,strings.Replace(key,string(os.PathSeparator),"_",-1))

	//  build a request
	req := datatype.GetObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Key : key,
	}
	// get the object
	result, err = api.GetObjects(req);



	if err != nil {

		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				gLog.Warning.Printf("Error: [%v]  Error: [%v]",s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				gLog.Error.Printf("error [%v]",aerr.Error())
			}
		} else {
			gLog.Error.Printf("[%v]",err.Error())
		}
	} else {
		usermd, err = utils.GetUserMeta(result.Metadata)
		gLog.Info.Printf("Object: %s - User meta: %s ",key,usermd)
		if err = utils.SaveObject(result,pathname); err == nil {
			gLog.Info.Printf("Object %s is downloaded to %s",key,pathname)
		} else {
			gLog.Error.Printf("Saving %s Error %v ",key,err)
		}
	}
	utils.Return(start)
}







