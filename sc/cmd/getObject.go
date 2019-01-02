package cmd

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jcelliott/lumber"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// getObjectCmd represents the getObject command
var (
	goshort = "Command to retrieve an object"
	// odir   string
	getObjectCmd = &cobra.Command {
		Use:   "getobj",
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
		Short: "Command to download an objet ",
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
		lumber.Warn(missingBucket)
		return

	case len(key) == 0:
		lumber.Warn(missingKey)
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
				lumber.Warn("Error: [%v]  Error: [%v]",s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				lumber.Error("error [%v]",aerr.Error())
			}
		} else {
			lumber.Error("[%v]",err.Error())
		}
	} else {
		usermd, err = utils.GetUserMeta(result.Metadata)
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
		log.Warn(missingBucket)
		return

	case len(key) == 0:
		log.Warn(missingKey)
		return

	case len(odir) == 0:
		log.Warn(missingOutputFolder)
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
				log.Warn("Error: [%v]  Error: [%v]",s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				log.Error("error [%v]",aerr.Error())
			}
		} else {
			log.Error("[%v]",err.Error())
		}
	} else {
		usermd, err = utils.GetUserMeta(result.Metadata)
		log.Info("Object: %s - User meta: %s ",key,usermd)
		if err = saveObject(result,pathname); err == nil {
			log.Info("Object %s is downloaded to %s",key,pathname)
		} else {
			log.Error("Saving %s Error %v ",key,err)
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

	pathname := pdir + string(os.PathSeparator) + strings.Replace(key,string(os.PathSeparator),"_",-1)
	if err:= ioutil.WriteFile(pathname,b.Bytes(),0644); err == nil {
		log.Info("Object %s is downloaded to %s",key,pathname)
	} else {
		log.Info("Error %v downloading object %s",err,key)
	}
}

