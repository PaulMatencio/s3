// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
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
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jcelliott/lumber"
	"github.com/s3/api"
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
	cmd.Flags().StringVarP(&datafile,"datafile","d","","the data file to upload")
	cmd.Flags().StringVarP(&metafile,"metafile","m","","the meta file to upload")
}

func init() {

	rootCmd.AddCommand(fPutObjectCmd)
	rootCmd.AddCommand(fPoCmd)
	initPoFlags(fPutObjectCmd)
	initPoFlags(fPoCmd)
}


func fPutObject(cmd *cobra.Command, args []string) {
	utils.LumberPrefix(cmd)

	if len(bucket) == 0 {
		lumber.Warn(missingBucket)
		utils.Return()
		return
	}

	if len(datafile) == 0 {
		lumber.Warn(missingInputFile)
		utils.Return()
		return
	}
	if len(datafile) == 0 {
		lumber.Warn(missingMetaFile)
		utils.Return()
		return
	}

	dir,key := filepath.Split(datafile)

	/* todo */
	usermd := dir
	meta := make(map[string]*string)
	meta["usermd"]= &usermd

	svc := s3.New(api.CreateSession())

	api.FputObjects(svc,bucket,key,datafile,meta)

	utils.Return()

}

func putObject(cmd *cobra.Command, args []string) {
	var buffer *bytes.Buffer
	utils.LumberPrefix(cmd)

	if len(bucket) == 0 {
		lumber.Warn(missingBucket)
		utils.Return()
		return
	}

	if len(datafile) == 0 {
		lumber.Warn(missingInputFile)
		utils.Return()
		return
	}

	dir,key := filepath.Split(datafile)

	/* todo  meta*/
	usermd := dir
	meta := make(map[string]*string)
	meta["usermd"]= &usermd

	/*todo  data */

	svc := s3.New(api.CreateSession())


	api.PutObjects(svc,bucket,key,buffer,meta)

	utils.Return()

}
