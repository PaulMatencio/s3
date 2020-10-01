package cmd

import (
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"github.com/s3/utils"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
	"time"
)

var (
	getMulipartcmd = &cobra.Command{
		Use:   "getMultipart",
		Short: "Command to retrieve an object in multi parts from S3",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			getMultipart(cmd, args)
		},
	}
	outfile string
)


func initGMPFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "The name of the bucket")
	cmd.Flags().StringVarP(&key, "key", "k", "", "Object key")
	cmd.Flags().StringVarP(&odir,"odir","O","","the ouput directory relative to the working (or Home ir omitted)  directory you'like to save")
	cmd.Flags().Int64VarP(&maxPartSize, "maxPartSize", "m", MinPartSize, "Maximum part size(MB)")
	cmd.Flags().IntVarP(&partNumber, "partNumber", "p", 5, "Part numner")
	cmd.Flags().IntVarP(&maxCon, "maxCon", "M", 5, "Maximum concurrent parts download , 0 => all parts")
}

func init() {
	RootCmd.AddCommand(getMulipartcmd)
	RootCmd.MarkFlagRequired("bucket")
	initGMPFlags(getMulipartcmd)
}

func getMultipart(cmd *cobra.Command, args []string) {

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}
	var (
		svc = s3.New(api.CreateSession())
	)

	if len(odir) == 0 {
		gLog.Warning.Printf("%s", missingOutputFolder)
		odir,_ = os.UserHomeDir()
	} else {
		if sep := strings.Split(odir, string(os.PathSeparator)); len(sep) == 1 {
			cwd, _ := os.Getwd()
			odir = filepath.Join(cwd, odir)
		}
		if !utils.Exist(odir){
			utils.MakeDir(odir)
		}
	}
	gLog.Info.Printf("output folder %s",odir)
	outfile = filepath.Join(odir, key)

	maxPartSize = maxPartSize*1024*1024  // convert into bytes
	if maxPartSize < MinPartSize {
		gLog.Warning.Printf("max part size %d < min part size %d", maxPartSize,MinPartSize)
		maxPartSize = MinPartSize
		gLog.Warning.Printf("min part size %d will be used for max part size",maxPartSize)
	}
	gLog.Info.Printf("Downloading key %s", key)
	// Create a downloader with the s3 client and custom options

	start:= time.Now()
	req := datatype.GetMultipartObjRequest{

		Service:        svc,
		Bucket:         bucket,
		Key:            key,
		PartNumber:     int64(partNumber),
		PartSize:       maxPartSize,
		Concurrency:    maxCon,
		OutputFilePath: outfile,
	}

	if n, err := api.GetMultipart(req); err == nil {
		gLog.Info.Printf("Downloaded %s to folder %s - size %d - Elapsed time %v ",key,odir, n,time.Since(start))
	} else {
		gLog.Error.Printf("%v", err)
	}
}
