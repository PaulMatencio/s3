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
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/api"
	"github.com/s3/gLog"
	"github.com/spf13/viper"
	"os"

	"github.com/spf13/cobra"
	directory "github.com/moses/directory/lib"
	sindexd "github.com/moses/sindexd/lib"
	hostpool "github.com/bitly/go-hostpool"
)

// toS3Cmd represents the toS3 command
var toS3Cmd = &cobra.Command{
	Use:   "toS3",
	Short: "Migrate Scality sindexd entries to S3 ",
	Long: `Migtate Scality sindexd entries to S3: 

     See Usage for the description of the other -- flags. See Examples below for full and incremental migration below 
     --index [PN|PD|NP|OM|XX-PN|XX-PD|XX-BN]
     Explanation of the --index flag 
        PN or PD => Full migration of Publication number and Publication data tables for a given country
        BN => Full migration of the Legacy BNS id table for a given country	
        NP => Full migration of Cite NPL table
        OM => Full migration of publication number and publication date tables for other countries
        OB => Full migration of legacy BNS tables for other countries
        XX-PN => Incremental migration of publication number tables ( every or specific country, XP and Cite NPL inclusive )
        XX-PD => Incremental migration of publication number tables ( every or specific country, XP and Cite NPL inclusive)
        XX-BN => Incremental migrattion of legacy BNS tables ( every country ,XP inclusive)

        Note : XP table contains the indexes of the Non Patent Literature documents
               Cite NPL  contains the indexes of the Cite NPL documents 
               
        Examples

        - Full migration
                    
           - There are 3 indexes tables per large country ( check the documentation for such countries)
				
                sindexd toS3  -i PN  -p US -m 500 ( Publication number and Publication date indexes for the US )
                sindexd toS3  -i BN  -p US -m 500 ( legacy BNS indexes for the US )
                sindexd toS3  -i PN  -p US -m 500 -k <Key1> ( From key1 to migrate from a specific key)
					
           - Small countries indexes are grouped in one table named "OTHER"

                sindexd toS3  -i OM -m 500 ( Publication number and Publication Date for the small countries)
                sindexd toS3 -i OB -m 500 ( Legacy BNS index the small countries)
					
           - Cite NPL table 
                sindexd toSindexd -i NP -m 1000
     	
        - Incremental migration		
                
                sindexd toS3-i XX-PN  -p 20200403  -m 500 ( publication number for every country of April 3,2020 )
                sindexd toS3 -i XX-PN  -p 20200403/US  ( publication number for for US  of April 3,2020)
                sindexd toS3 -i XX-PD  -p 20200403  -m 500 ( publication date for every country of April 3,2020 )
                sindexd toS3 -i XX-BN  -p 20200403/US  ( legacy BNS id for US of April 3,020 )
                sindexd toS3 -i XX-BN  -p 20200403/US  ( legacy BNS id for US of April 3,2020 )

         Note : XX-PN, includes Cite NPL publication number
                XX-PD  includes Cite NPL publication date
                XX-BN  There is no Cite NPL publication number or publication date
 				`,
	Run: func(cmd *cobra.Command, args []string) {
		migrateToS3(cmd,args)
	},
}

func init() {
	RootCmd.AddCommand(toS3Cmd)
	initToS3Flags(toS3Cmd)
}

func initToS3Flags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&sindexUrl,"sindexd","s","","sindexd endpoint <url:port>")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","the prefix of the key")
	cmd.Flags().StringVarP(&iIndex,"iIndex","i","","Index table [PN|PD|BN|NP|OM|OB]")
	cmd.Flags().StringVarP(&marker, "marker", "k", "","Start with this Marker (Key) for the Get Prefix ")
	cmd.Flags().IntVarP(&maxKey,"maxKey","m",100,"maxmimum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the S3  bucket")

}

func migrateToS3(cmd *cobra.Command,args []string) {

	if len(sindexUrl) == 0 {
		sindexUrl = viper.GetString("sindexd.url")
	}
	if len(iIndex) == 0 {
		iIndex = "PN"
	}
	// indSpecs := directory.GetIndexSpec(iIndex)

	if len(prefix) == 0 {
		gLog.Info.Println("%s", missingPrefix);
		os.Exit(2)
	}

	if len(bucket) == 0 {
		if bucket = viper.GetString("s3.bucket"); len(bucket) == 0 {
			gLog.Info.Println("%s", missingBucket);
			os.Exit(2)
		}
		bucket = bucket+"-"+strings.ToLower(iIndex)
	}

	sindexd.Delimiter = delimiter
	// sindexd.Host = append(sindexd.Host, sindexUrl)
	sindexd.Host = strings.Split(sindexUrl,",")
	sindexd.HP = hostpool.NewEpsilonGreedy(sindexd.Host, 0, &hostpool.LinearEpsilonValueCalculator{})

	migToS3(prefix)

}

func migToS3 (prefix string)  {
	buck:= ""
	indSpecs := directory.GetIndexSpec(iIndex)
	svc      := s3.New(api.CreateSession())
	num := 0

	for Nextmarker {
		if response = directory.GetSerialPrefix(iIndex, prefix, delimiter, marker, maxKey, indSpecs); response.Err == nil {
			resp := response.Response

			var wg sync.WaitGroup
			wg.Add(len(resp.Fetched))

			for k, v := range resp.Fetched {
				if v1, err:= json.Marshal(v); err == nil {
					/* hash the country code which should be equal to prefix */
					cc := strings.Split(k,"/")[0]

					if cc == "XP" {
						buck=bucket+"-05"
					} else {
						buck = bucket + "-" + fmt.Sprintf("%02d", utils.HashKey(cc, bucketNumber))
					}
					go func(svc *s3.S3,k string,buck string,value []byte) {
						defer wg.Done()

						if r,err := writeToS3(svc, k, buck,v1); err == nil {
							gLog.Trace.Println(*r.ETag)
						} else {
							gLog.Error.Printf("Error %v  - Writing %s",err,k)
						}
						// gLog.Info.Println(k,buck)
					} (svc,k,buck,v1)

				} else {
					gLog.Error.Printf("Error %v - Marshalling %s:%v",err, k,v)
					wg.Done()
				}
			}
			wg.Wait()
			if len(resp.Next_marker) == 0 {
				Nextmarker = false
			} else {
				marker = resp.Next_marker
				num++
				gLog.Info.Printf("Next marker => %s %d", marker,num)
			}

		} else {
			// gLog.Error.Printf("%v",response.Err)
			gLog.Error.Printf("Error: %v getting prefix %s",response.Err,prefix)
			Nextmarker = false
		}
	}
}



func writeToS3(svc  *s3.S3 ,key string, bucket string, meta []byte) (*s3.PutObjectOutput,error) {

	//gLog.Info.Printf("Writing key:%s - meta:%v to bucket:%s", key, meta, bucket)
    data := make([]byte,0,0)  // empty byte array
    gLog.Trace.Println(bucket,key)
	req:= datatype.PutObjRequest{
		Service : svc,
		Bucket: bucket,
		Key: key,
		Buffer: bytes.NewBuffer(data), // convert []byte into *bytes.Buffer
		Meta : meta,
	}
	return api.PutObject(req)

}






