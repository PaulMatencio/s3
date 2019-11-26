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
	"fmt"
	"github.com/s3/gLog"
	st33 "github.com/s3/st33/utils"
	"github.com/spf13/cobra"
)

// hashKeyCmd represents the hashKey command
var  (
	    preKey string
	    modulo int
	    start, end int
		hashKeyCmd = &cobra.Command{
		Use:   "hashKey",
		Short: "hash a given key with modulo",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			hashFile(cmd,args)
		},
	}
)

func inithaFlags(cmd *cobra.Command) {

	/*cmd.Flags().StringVarP(&key,"key","k","","The key to  be hashed") */
	cmd.Flags().IntVarP(&modulo,"modulo","m",16,"Modulo")
	cmd.Flags().StringVarP(&ifile,"ifile","i","","the corresponding control file that was used to migrate the data file")




}
func init() {
	RootCmd.AddCommand(hashKeyCmd)
	inithaFlags(hashKeyCmd)
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// hashKeyCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// hashKeyCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}



func hash(key string, modulo int) {
	 v := 0;
	 for k :=0; k < len(key); k++ {
	 	v += int(key[k])
	 }
	 v1 := v % modulo
	 fmt.Println(key, v1)

}

func hashFile(cmd *cobra.Command, args []string) {

	if len(ifile) == 0 {
		gLog.Info.Printf("%s", missingInputFile)
		return
	}

	if c, err := st33.BuildConvalArray(ifile); err == nil {

		for _, v := range *c {
			hash(v.PxiId, modulo)
		}
	}
}