// Copyright © 2019 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"github.com/s3/gLog"
	"github.com/s3/st33/utils"
	"github.com/spf13/cobra"
)

// st33ToS3Cmd represents the st33ToS3 command
var (
	async bool

	toS3Cmd = &cobra.Command{
	Use:   "toS3",
	Short: "Command to extract ST33 file containing Tiff Images and Blob and upload to S3",
	Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		toS3Func(cmd,args)
	},
}
	)

func initT3Flags(cmd *cobra.Command) {

	// cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&ifile,"ifile","i","","the input data file")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket ")
	cmd.Flags().BoolVarP(&async,"async","a",false,"upload asynchronously ")
}

func init() {
	RootCmd.AddCommand(toS3Cmd)
	initT3Flags(toS3Cmd)
}

func toS3Func(cmd *cobra.Command, args []string) {

	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}
	if len(bucket) == 0 {
		gLog.Info.Printf("%s",missingBucket)
		return
	}
	gLog.Info.Printf("Processing input file %s",ifile)
	if !async {
		numpages,numdocs,err := st33.TooS3(ifile, bucket,profiling)
		gLog.Info.Printf("%d documents/ %d pages were processed - error ",numdocs,numpages,err)

	} else {
		st33.ToS3Async(ifile,bucket,profiling)
	}

}

