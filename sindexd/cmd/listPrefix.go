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
	"encoding/json"
	hostpool "github.com/bitly/go-hostpool"
	directory "github.com/moses/directory/lib"
	sindexd "github.com/moses/sindexd/lib"
	"github.com/s3/gLog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strings"
	"sync"
	"time"

	// "strings"
)

// listPrefixCmd represents the listPrefix command
var (
	sindexUrl,iIndex,prefix,marker  string
	response   *directory.HttpResponse
	Nextmarker = true
	delimiter = ""
	maxKey int

	listPrefixCmd = &cobra.Command{
		Use:   "listPrefix",
		Short: "List Scality Sindexd entries with prefix",
		Long: `List Scality Sindexd entries with prefix: 
            There are 5 set of tables: 
            PN => Publication number tables
            PD => Publication date tables
            NP => Cite NPL table
            XX => New loaded document id table	
            BN => Legacy BNS id table`,
		Run: func(cmd *cobra.Command, args []string) {
			listPrefix(cmd,args)
		},
	}
)

func initLpFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&sindexUrl,"sindexd","s","","sindexd endpoint <url:port>")
	cmd.Flags().StringVarP(&prefixs,"prefix","p","","prefix of the key separted by a comma")
	// cmd.Flags().StringVarP(&iIndex,"iIndex","i","","Index table <PN>/<PD>")
	cmd.Flags().StringVarP(&iIndex,"iIndex","i","","Index table <PN>/<PD>/<BN>/<NP>/<OM>/<OB>")
	cmd.Flags().StringVarP(&marker, "marker", "k", "","Start with this Marker (Key) for the Get Prefix ")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "","Delimiter of the list for the Get Prefix ")
	cmd.Flags().IntVarP(&maxKey,"maxKey","m",100,"Limit number of keys to be processed concurrently")
	cmd.Flags().IntVarP(&loop,"loop","L",1,"Number of loop using the next marker if there is one")
}

func init() {
	RootCmd.AddCommand(listPrefixCmd)
	initLpFlags(listPrefixCmd)
}


func listPrefix(cmd *cobra.Command,args []string) {

	if len(sindexUrl) == 0 {
		sindexUrl = viper.GetString("sindexd.url")
	}

	if len(iIndex) == 0 {
		iIndex = "PN"
	}


	// indSpecs := directory.GetIndexSpec(iIndex)

	gLog.Info.Println(sindexUrl, prefixs)
	prefixa = strings.Split(prefixs,",")

	sindexd.Delimiter = delimiter
	// sindexd.Host = append(sindexd.Host, sindexUrl)
	sindexd.Host = strings.Split(sindexUrl,",")
	sindexd.HP = hostpool.NewEpsilonGreedy(sindexd.Host, 0, &hostpool.LinearEpsilonValueCalculator{})

	start := time.Now()
	if len(prefixa) > 0 {
		var wg sync.WaitGroup
		wg.Add(len(prefixa))
		for _, prefix := range prefixa {
			go func(prefix string ) {
				defer wg.Done()
				gLog.Info.Println(prefix, bucket)
				  listPref(prefix);
			}(prefix)
		}
		wg.Wait()
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
}

func listPref (prefix string)  {

	indSpecs := directory.GetIndexSpec(iIndex)
	if pref := strings.Split(prefix,"/"); pref[0] == "NP" {
		pref[0]= "XP"
		prefix =""
		for _,v := range pref {
			prefix += v+"/"
		}
		iIndex = "NP"
	}
	gLog.Info.Printf("Prefix: %s - start with this key: %s - Index: %s - Index specification: %v", prefix,marker,iIndex,&indSpecs)

	n:= 0
	for Nextmarker {
		if response = directory.GetSerialPrefix(iIndex, prefix, delimiter, marker, maxKey, indSpecs); response.Err == nil {
			n++
			resp := response.Response
			for k, v := range resp.Fetched {
				if v1, err:= json.Marshal(v); err == nil {
					gLog.Info.Printf("%s %v", k, string(v1))

				} else {
					gLog.Error.Printf("Error %v -  Marshalling %v",err, v)
				}
			}
			if len(resp.Next_marker) == 0  || (n >= loop && loop != 0) {
				Nextmarker = false
			} else {
				marker = resp.Next_marker
				gLog.Info.Printf("Next marker => %s", marker)
			}

		} else {
			gLog.Error.Printf("%v",response.Err)
			Nextmarker = false
		}
	}
}

func GetNewKeys(prefix string, marker string, maxKey int) {

	var (
		index    = "XX"
		keys     []string
		indSpecs = directory.GetIndexSpec(index) // Get the index Specifiaction for XX ( new loaded document
	)
	if response = directory.GetSerialPrefix(index, prefix, delimiter, marker, maxKey, indSpecs); response.Err == nil {

		resp := response.Response
		for k, _ := range resp.Fetched {
			keys = append(keys, k)
		}
	}
}


