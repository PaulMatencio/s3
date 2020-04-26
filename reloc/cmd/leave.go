// Copyright © 2020 NAME HERE <EMAIL ADDRESS>
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
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// leaveCmd represents the leave command
var (
	fromIP, toIP , fn, ft , root string
	find bool
	leaveCmd = &cobra.Command{
	Use:   "leave",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		Reloc(cmd)
	},
}
)

func init() {
	rootCmd.AddCommand(leaveCmd)
	initReloc(leaveCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// leaveCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// leaveCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func initReloc(cmd *cobra.Command){
	cmd.Flags().StringVarP(&fromIP, "fromIP", "f", "","from ip address")
	cmd.Flags().StringVarP(&toIP, "toIP", "t", "","from ip address")
	cmd.Flags().StringVarP(&root, "root", "R", "","root directory")
	cmd.Flags().StringVarP(&fn, "fileName", "N", "","file name")
	cmd.Flags().StringVarP(&ft, "fileType", "T", "","file type")
	cmd.Flags().BoolVarP(&find, "find", "F", false,"find string fromIP")
}

func Reloc(cmd *cobra.Command) {

	if len(fromIP) == 0 {
		if fromIP = viper.GetString("move.fromIP");len(fromIP) == 0 {
			gLog.Info.Println("From IP address is missing")
			os.Exit(100)
		}
	}

	if len(toIP) == 0 {
		if toIP = viper.GetString("move.toIP");len(toIP) == 0 {
			gLog.Info.Println("To IP address is missing")
			os.Exit(100)
		}
	}

	if len(root) == 0 {
		if root = viper.GetString("files.root");len(root) == 0 {
			gLog.Info.Printf("Current directory, %s will be used as root\n",".")
			root= "."
		}
	}

	if len(fn) == 0 {
		if fn = viper.GetString("files.fn");len(fn) == 0 {
			gLog.Info.Printf("File name is missing, %s  will be used as file name\n","*")
			fn="*"
		}
	}

	if len(ft) == 0 {
		if ft = viper.GetString("files.ft");len(fn) == 0 {
			gLog.Info.Printf("File type is missing, %s  will be used as file type\n","conf")
			ft = "conf"
		}
	}

	if err := filepath.Walk(root, replace); err != nil {
		gLog.Error.Printf("Replace file with error %v\n",err)
	}

}

func replace(path string, fi os.FileInfo, err error) error {
	var (
		matched bool
		re = regexp.MustCompile(fromIP)
	)
	if err != nil {
		return err
	}
	if !!fi.IsDir() {
		return nil //
	}
	pattern:= fn + "." + ft
	gLog.Trace.Println(pattern)
	now := time.Now()
    backup := fmt.Sprintf("%4d-%02d-%02d:%02d:%02d:%02d",now.Year(),now.Month(),now.Day(),now.Hour(),now.Minute(),now.Second())
	if matched, err = filepath.Match(pattern, fi.Name()); err != nil {
		return err
	}

	if matched {
		backup = path + "-" + backup
		if read, err := ioutil.ReadFile(path); err == nil {
			if !find {
				if err = ioutil.WriteFile(backup, []byte(read), 0644); err == nil {
					gLog.Trace.Printf("Replacing string %s by string %s in file %s\n", fromIP, toIP, path)
					newContents := strings.Replace(string(read), fromIP, toIP, -1)
					return ioutil.WriteFile(path, []byte(newContents), 0)
				} else {
					return err
				}
			} else {
				gLog.Trace.Printf("Searching string %s in file %s ", fromIP, path)
				gLog.Info.Printf("Find %q in %s\n",re.Find([]byte(read)),path)
			}
		} else {
			return err
		}
	}
	return nil
}

