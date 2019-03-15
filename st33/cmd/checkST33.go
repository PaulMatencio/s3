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
	"os"
)

// checkST33Cmd represents the checkST33 command
var (
	checkST33Cmd = &cobra.Command {
		Use:   "chkST33",
		Short: "Command to check ST33 data file consistency vs control file",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
				checkST33(cmd,args)
		},
	}
	cfile string
)


func initCdFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&ifile,"data-file","i","","the St33 data file ")
	cmd.Flags().StringVarP(&cfile,"control-file","c","","the St33 control file ")
	// cmd.Flags().StringVarP(&idir,"input-directory","d","","the name of the directory")

}

func init() {
	RootCmd.AddCommand(checkST33Cmd)
	initCdFlags(checkST33Cmd)

}


func checkST33(cmd *cobra.Command, args []string) {


	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}

	if len(cfile) == 0 {
		gLog.Info.Printf("%s",missingCtrlFile)
		return
	}

	/*
	if len(idir) == 0 {
		gLog.Info.Printf("%s",missingInputFolder)
		return
	}
	*/


	// ctrlFile := filepath.Join(idir,cfile)

	r,err  := st33.NewSt33Reader(ifile)

	if err != nil {
		gLog.Fatal.Printf("%v",err)
		os.Exit(100)
	}

	if c,err:=  st33.BuildConvalArray(cfile); err == nil {
		for _, v := range *c {

			lp := len(v.PxiId)
			typ := v.PxiId[lp-2:lp-1]

			if typ == "B" {  // BLOB record
				r.ReadST33BLOB(v)
			} else  if typ == "P" {
				if v.PxiId  != "E1_____113F65926719P1" {       // Exclude PXIID for IPXI.lot029 INTG
					r.ReadST33Tiff(v)
				}
			} else {
				gLog.Warning.Printf("%s 's document code is %s",v.PxiId, typ)
			}
		}

	} else {
		gLog.Error.Println(err)
	}

}


