

package cmd

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/gLog"
	"github.com/jcelliott/lumber"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
)

// putObjectCmd represents the putObject command
var (
	pfshort = "Command to upload a file to a bucket"
	poshort = "Command to upload a buffer to a bucket"
	datafile,metafile string
	fPutObjectCmd = &cobra.Command{
		Use:   "fPutObject",
		Short: pfshort,
		Long: ``,
		Hidden: true,
		Run: fPutObject,
	}
	putObjectCmd = &cobra.Command{
		Use:   "putObj",
		Short: poshort,
		Long: ``,
		Run: putObject,
	}

	fPoCmd = &cobra.Command{
		Use:   "fputObj",
		Short: pfshort,
		Long: ``,
		Run: fPutObject,
	}
	poCmd = &cobra.Command{
		Use:   "po",
		Short: poshort,
		Long: ``,
		Hidden: true,
		Run: putObject,
	}
)

func initPfFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name")
	cmd.Flags().StringVarP(&datafile,"datafile","f","","the data file to upload")
	// cmd.Flags().StringVarP(&metafile,"metafile","m","","the meta file to upload")
}

func init() {

	rootCmd.AddCommand(fPutObjectCmd)
	rootCmd.AddCommand(fPoCmd)
	//rootCmd.AddCommand(putObjectCmd)
	initPfFlags(fPutObjectCmd)
	initPfFlags(fPoCmd)
}


func fPutObject(cmd *cobra.Command, args []string) {

	var (
	start = utils.LumberPrefix(cmd)
	meta []byte
	err error
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	if len(datafile) == 0 {
		gLog.Warning.Printf("%s",missingInputFile)
		utils.Return(start)
		return
	}

	/*
	if len(metafile) == 0 {
		lumber.Warn(missingMetaFile)
		utils.Return(start)
		return
	}
	*/
	cwd,_:= os.Getwd()
	datafile = filepath.Join(cwd,datafile)
	metafile = datafile+".md"
	_,key := filepath.Split(datafile)


	if  meta,err  = utils.ReadFile(metafile); err != nil || len(meta) == 0 {
		gLog.Warning.Printf("no user metadata %s",metafile)
	}

	gLog.Info.Printf("%s %s",meta,metafile)

	req:= datatype.FputObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Key: key,
		Inputfile: datafile,
		Meta : meta,

	}

	if result,err := api.FputObjects(req); err == nil {
		lumber.Info("Successfuly upload file %s to  Bucket %s  - Etag : %s", datafile,bucket,*result.ETag)
	} else {
		lumber.Error("fail to upload %s - error: %v",datafile,err)
	}

	utils.Return(start)

}

func putObject(cmd *cobra.Command, args []string) {

	var (
		//buffer *bytes.Buffer
		start = utils.LumberPrefix(cmd)
		meta []byte
		data []byte

	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	if len(datafile) == 0 {
		gLog.Warning.Printf("%s",missingInputFile)
		utils.Return(start)
		return
	}

	cwd,_:= os.Getwd()
	datafile = filepath.Join(cwd,datafile)
	metafile = datafile+".md"
	_,key := filepath.Split(datafile)


	// read Meta file into meta []byte
	if  meta,err  := utils.ReadFile(metafile); err != nil || len(meta) == 0 {
		gLog.Info.Printf("no user metadata  %s",metafile)
	}

	// Read data file into data []byte

	if  data,err  := utils.ReadFile(datafile); err != nil || len(data) == 0 {
		gLog.Error.Printf("Error %v reading %s",err,datafile)
		utils.Return(start)
	}

	req:= datatype.PutObjRequest{

		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Key: key,
		Buffer: bytes.NewBuffer(data), // convert []byte into *bytes.Buffer
		Meta : meta,

	}

	if result,err := api.PutObjects(req); err == nil {
		gLog.Info.Printf("Successfuly upload file %s to  Bucket %s  - Etag : %s  - Expiration: %s ", datafile,bucket,*result.ETag,*result.Expiration)
	} else {
		gLog.Error.Printf("fail to upload %s - error: %v",datafile,err)
	}

	utils.Return(start)

}

