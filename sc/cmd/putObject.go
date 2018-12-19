

package cmd

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jcelliott/lumber"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"path/filepath"
)

// putObjectCmd represents the putObject command
var (
	poshort = "Command to upload an object"
	datafile,metafile string
	fPutObjectCmd = &cobra.Command{
		Use:   "fPutObj",
		Short: poshort,
		Long: ``,
		Run: fPutObject,
	}
	putObjectCmd = &cobra.Command{
		Use:   "putObj",
		Short: poshort,
		Long: ``,
		Run: putObject,
	}

	fPoCmd = &cobra.Command{
		Use:   "fPo",
		Short: poshort,
		Long: ``,
		Hidden: true,
		Run: fPutObject,
	}
	poCmd = &cobra.Command{
		Use:   "po",
		Short: poshort,
		Long: ``,
		Run: putObject,
	}
)

func initPoFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name")
	cmd.Flags().StringVarP(&datafile,"datafile","f","","the data file to upload")
	cmd.Flags().StringVarP(&metafile,"metafile","m","","the meta file to upload")
}

func init() {

	rootCmd.AddCommand(fPutObjectCmd)
	rootCmd.AddCommand(fPoCmd)
	initPoFlags(fPutObjectCmd)
	initPoFlags(fPoCmd)
}


func fPutObject(cmd *cobra.Command, args []string) {
	start := utils.LumberPrefix(cmd)

	if len(bucket) == 0 {
		lumber.Warn(missingBucket)
		utils.Return(start)
		return
	}

	if len(datafile) == 0 {
		lumber.Warn(missingInputFile)
		utils.Return(start)
		return
	}
	if len(datafile) == 0 {
		lumber.Warn(missingMetaFile)
		utils.Return(start)
		return
	}

	dir,key := filepath.Split(datafile)

	/* todo */
	meta := []byte(dir)


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
	var buffer *bytes.Buffer
	start := utils.LumberPrefix(cmd)

	if len(bucket) == 0 {
		lumber.Warn(missingBucket)
		utils.Return(start)
		return
	}

	if len(datafile) == 0 {
		lumber.Warn(missingInputFile)
		utils.Return(start)
		return
	}

	dir,key := filepath.Split(datafile)

	/* todo  meta*/

	/* todo */
	meta := []byte(dir)


	req:= datatype.PutObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Key: key,
		Buffer: buffer,
		Meta : meta,
	}

	if result,err := api.PutObjects(req); err == nil {
		lumber.Info("Successfuly upload file %s to  Bucket %s  - Etag : %s  - Expiration: %s ", datafile,bucket,*result.ETag,*result.Expiration)
	} else {
		lumber.Error("fail to upload %s - error: %v",datafile,err)
	}
	utils.Return(start)

}
