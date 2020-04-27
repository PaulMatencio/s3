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
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// leaveCmd represents the leave command
var (
	fromIP, toIP , fn, ft , root string
	FT []string
	Pattern []string
	find , check bool
	leaveCmd = &cobra.Command{
	Use:   "remove",
	Short: "Remove a storage node from Scality Ring",
	Long: ` 
    Remove find -i <string> or find -i <string> and replace by -t <string> in
    files from a given directory tree ( --root directory)
    
    Every file that matches the --fileName and --fileType arguments will be processed
    Both generic (*) FileName and FileType are accepted  
	
            
    Examples 

      (1) Find  IP addresses in every file of type conf,xml or yaml in the 
      /etc directory tree
      reloc remove -R /etc -F -t 10.12.202.10 -f 10.14.204.10 -N "*"  -T "conf|xml|yaml" 
                                                                                         
      (2) Find  and Replace IP addresses in every file of type conf,xml or yaml in the 
      /etc directory tree
      reloc remove -R /etc -t 10.12.202.10 -f 10.14.204.10 -N "*"  -T "conf|xml|yaml"

      (3) Configuration file yaml(default location $HOME/.reloc.yaml )
	  
      The rguments in the configuration file are overridden by input arguments	
      
      $HOME/.reloc.yaml
      move:
        fromIP: 10.12.202.10
        toIP: 10.15.203.10
      files:
        root: "/etc"
        fn: "*"
        ft: "conf|yaml|yml|xml|py"
 
      `,
	Run: func(cmd *cobra.Command, args []string) {
		Reloc(cmd)
	},
}
)

func init() {
	rootCmd.AddCommand(leaveCmd)
	initReloc(leaveCmd)
}

func initReloc(cmd *cobra.Command){
	cmd.Flags().StringVarP(&fromIP, "fromIP", "f", "","from ip address")
	cmd.Flags().StringVarP(&toIP, "toIP", "t", "","from ip address")
	cmd.Flags().StringVarP(&root, "root", "R", "","root directory")
	cmd.Flags().StringVarP(&fn, "fileName", "N", "","file name")
	cmd.Flags().StringVarP(&ft, "fileType", "T", "","file types separed by | ")
	cmd.Flags().BoolVarP(&find, "find", "F", false,"find string fromIP")
	cmd.Flags().BoolVarP(&find, "check", "C", true,"check valid IP address")
}

func Reloc(cmd *cobra.Command) {

	if len(fromIP) == 0 {
		if fromIP = viper.GetString("move.fromIP");len(fromIP) == 0 {
			gLog.Info.Println("From IP address is missing")
			os.Exit(100)
		}
	}
	if net.ParseIP(fromIP).To4()== nil  {
		gLog.Warning.Printf("%s is not a valid IPv4 address",fromIP)
	}

	if len(toIP) == 0 {
		if toIP = viper.GetString("move.toIP");len(toIP) == 0 {
			gLog.Info.Println("To IP address is missing")
			os.Exit(100)
		}
	}
	if net.ParseIP(toIP).To4()== nil  {
		gLog.Warning.Printf("%s is not a valid IPv4 address",toIP)
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
		if ft = viper.GetString("files.ft"); len(fn) == 0 {
			gLog.Info.Printf("File type is missing, %s  will be used as file type\n", "conf")
			ft = "conf"
		}
	}
	FT = strings.Split(ft, "|")
	if err := filepath.Walk(root, findReplace); err != nil {
		gLog.Error.Printf("Replace file with error %v\n",err)
	}

}


func findReplace(path string, fi os.FileInfo, err error) error {
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
	pattern:= fn
	for _,t :=range FT {
		if len(t) >0 {
			pattern += "."+t
			Pattern = append(Pattern,t)
		}
	}

	//gLog.Trace.Println(pattern)
	now := time.Now()

    matched = false
    Matched := make([]bool,len(Pattern))
	for i,p:= range Pattern {
		p = fn + "." + p
		if Matched[i], err = filepath.Match(p, fi.Name()); err != nil {
			return err
		} else {
			matched = Matched[i]
			if matched {
				break
			}
		}
	}

	if matched {
		backup := path + "-" + fmt.Sprintf("%4d-%02d-%02d:%02d:%02d:%02d",now.Year(),now.Month(),now.Day(),now.Hour(),now.Minute(),now.Second())
		if read, err := ioutil.ReadFile(path); err == nil {
			// Find string fromIP
			if r:= re.Find([]byte(read));len(r) > 0 {
			 	if !find {
			 		// Replace string fromIP by string toIP
			 		//  backup old file before replace
					 if err = ioutil.WriteFile(backup, []byte(read), 0644); err == nil {
						 gLog.Info.Printf("Replacing string %s by string %s in file %s\n", fromIP, toIP, path)
						 newContents := strings.Replace(string(read), fromIP, toIP, -1)
						 return ioutil.WriteFile(path, []byte(newContents), 0)
					 } else {
						 return err
					 }
				 } else {
					 gLog.Trace.Printf("Searching string %s in file %s ", fromIP, path)
					 gLog.Info.Printf("Find %q in %s\n", r, path)
				 }
			}
		} else {
			return err
		}
	}
	return nil
}

