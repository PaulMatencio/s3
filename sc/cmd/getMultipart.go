package cmd

import (
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"github.com/aws/aws-sdk-go/service/s3"
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
)

func initGMPFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the bucket")
	cmd.Flags().StringVarP(&key, "key", "k", "", "object key")
	cmd.Flags().StringVarP(&odir, "odir", "o", "", "the output directory relative to the home directory you'd like to save")
	cmd.Flags().Int64VarP(&maxPartSize, "maxPartSize", "m", MinPartSize, "Maximum part size")
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
		partnumber int64 = 50
		svc = s3.New(api.CreateSession())
	)

	if len(odir) == 0 {
		gLog.Warning.Printf("%s", missingOutputFolder)
		odir,_ = os.UserHomeDir()
		gLog.Warning.Printf("user home directory %s will be used for output folder",odir)

	}

	if maxPartSize < MinPartSize {
		gLog.Warning.Printf("Minimum maxPartize is %d", MinPartSize)
		maxPartSize = MinPartSize
		gLog.Warning.Printf("%d will be used for max Part Size",maxPartSize)
	}
	gLog.Info.Printf("Downloading key %s", key)
	// Create a downloader with the s3 client and custom options

	outfile := filepath.Join(odir, key)
	start:= time.Now()
	req := datatype.GetMultipartObjRequest{
		Service:        svc,
		Bucket:         bucket,
		Key:            key,
		PartNumber:     partnumber,
		PartSize:       partSize,
		Concurrency:    maxCon,
		OutputFilePath: outfile,
	}

	if n, err := api.GetMultipart(req); err == nil {
		gLog.Info.Printf("Downloaded %s to folder %s - size %d - Elapsed time %v ",key,odir, n,time.Since(start))
	} else {
		gLog.Error.Printf("%v", err)
	}
}
