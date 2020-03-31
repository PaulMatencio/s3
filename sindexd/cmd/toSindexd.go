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
	"github.com/s3/gLog"
	"github.com/spf13/viper"
	"os"
	"strings"

	hostpool "github.com/bitly/go-hostpool"
	directory "github.com/moses/directory/lib"
	sindexd "github.com/moses/sindexd/lib"
	"github.com/spf13/cobra"
)


var (
	toSindexUrl string
	toSindexdCmd = &cobra.Command{
		Use:   "toSindexd",
		Short: "Copy sindexd tables to sindexd tables",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			toSindexd(cmd,args)
		},
	}
)

func init() {
	RootCmd.AddCommand(toSindexdCmd)
	initCopyFlags(toSindexdCmd)
}

func initCopyFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&sindexUrl,"sindexd","s","","sindexd endpoint <url:port>")
	cmd.Flags().StringVarP(&toSindexUrl,"toSindexd","t","","target sindexd endpoint <url:port>")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","the prefix of the key")
	cmd.Flags().StringVarP(&iIndex,"iIndex","i","","Index table <PN>/<PD>")
	cmd.Flags().StringVarP(&marker, "marker", "k", "","Start with this Marker (Key) for the Get Prefix ")
	cmd.Flags().IntVarP(&maxKey,"maxKey","m",100,"maxmimum number of keys to be processed concurrently")


}

func toSindexd(cmd *cobra.Command,args []string) {

	if len(sindexUrl) == 0 {
		if sindexUrl = viper.GetString("sindexd.url"); len(sindexUrl) ==0 {
			gLog.Info.Println("%s", "missing source  sindexed URL");
			os.Exit(2)
		}
	}
	if len(toSindexUrl) == 0 {
		if toSindexUrl = viper.GetString("toSindexd.url"); len(toSindexUrl) == 0 {
			gLog.Info.Println("%s", "missing target sindexed URL");
			os.Exit(2)
		}
	}
	if len(iIndex) == 0 {
		iIndex = "PN"
	}
	// indSpecs := directory.GetIndexSpec(iIndex)

	if len(prefix) == 0 {
		gLog.Info.Println("%s", missingPrefix);
		os.Exit(2)
	}


	sindexd.Delimiter = delimiter
	// sindexd.Host = append(sindexd.Host, sindexUrl)
	sindexd.Host = strings.Split(sindexUrl,",")
	sindexd.TargetHost = strings.Split(toSindexUrl,",")
	sindexd.HP = hostpool.NewEpsilonGreedy(sindexd.Host, 0, &hostpool.LinearEpsilonValueCalculator{})
	sindexd.TargetHP = hostpool.NewEpsilonGreedy(sindexd.TargetHost, 0, &hostpool.LinearEpsilonValueCalculator{})
	bkupSindexd()

}

func bkupSindexd ()  {
	/*
		List prefix sindexd table ( "PD")
	    for each key,value  {
	       map[key] = value
	       key1 = key  -  YYYY/MM/DD  = Publication number
	       map[key1] =value
	    }
	    add map[key] to target PD  tables
	    add map[key1] to target PN tables

	 */
	indSpecs := directory.GetIndexSpec("PD")
	indSpecs1 := directory.GetIndexSpec("PN")
	num := 0
	keyObj := make(map[string]string)
	keyObj1 := make(map[string]string)
	/*
		Loop until Next marker is false
	 */
	for Nextmarker {
		if response = directory.GetSerialPrefix(iIndex, prefix, delimiter, marker, maxKey, indSpecs); response.Err == nil {
			resp := response.Response
			for k, v := range resp.Fetched {
				keys := strings.Split(k,"/")
				k1 := keys[0]
				for i := 4; i < len(keys); i++ {
					k1 += "/"+keys[i]
				}
				if v1, err:= json.Marshal(v); err == nil {
					vs := string(v1)
					gLog.Info.Println(k, vs)
					gLog.Info.Println(k1,vs)
					keyObj[k] = vs
					keyObj1[k]= vs
				}
			}
			/*
				add keys to tge PD sindexd tables
			    add keys to the PN sindexd tables
				Exit if any error ot sindexd status != 200
			*/
			if r := directory.AddSerialPrefix1(sindexd.TargetHP,prefix,indSpecs,keyObj); r.Err == nil {
				if r.Response.Status == 200 {
					if r1 := directory.AddSerialPrefix1(sindexd.TargetHP, prefix, indSpecs1, keyObj1); r1.Err != nil {
						gLog.Error.Printf("Error: %v  adding key after marker %s to %s", r1.Err, marker,indSpecs1)
						os.Exit(100)
					} else {
						if r1.Response.Status != 200 {
							gLog.Error.Printf("Sindexd status: %v adding key after marker %s to %s", r.Response.Status, marker, indSpecs1)
							os.Exit(100)
						}
					}
				}  else {
					gLog.Error.Printf("Sindexd status: %v adding key after marker %s to %s", r.Response.Status, marker, indSpecs)
					os.Exit(100)
				}
			} else {
				gLog.Error.Printf("Error: %v adding key after marker %s to %s",r.Err,marker,indSpecs)
				os.Exit(100)
			}

			// Reuse the MAP storage rather then let the Garbage free the unused storage
			// this may  create overhead without real benefit
			for k := range keyObj{ delete(keyObj,k)}
			for k := range keyObj1{ delete(keyObj1,k)}

			if len(resp.Next_marker) == 0 {
				Nextmarker = false
			} else {
				marker = resp.Next_marker
				num++
				if num == 10 {
					Nextmarker = false
				}
				gLog.Info.Printf("Next marker => %s", marker)
			}

		} else {
			gLog.Error.Printf("Error: %v getting prefix %s",response.Err,prefix)
			Nextmarker = false
		}
	}
}
