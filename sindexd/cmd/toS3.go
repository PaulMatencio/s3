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
	"time"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/api"
	"github.com/s3/gLog"
	"github.com/spf13/viper"
	"os"
	hostpool "github.com/bitly/go-hostpool"
	directory "github.com/moses/directory/lib"
	sindexd "github.com/moses/sindexd/lib"
	"github.com/spf13/cobra"
)

var (
	toS3Cmd = &cobra.Command{
	Use:   "toS3",
	Short: "Migrate Scality sindexd entries to S3 metadata",
	Long: `Migtate Scality sindexd entries to S3 metadata: 

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
     
     --prefix [CC|XP|WO|EP] 
         CC is a country code such as US,CN,JP,GB,DE,KR,ES,FR,IT,NO,TW,SU,BG etc ...
         XP is the code for Non Patent literature, Cite NPL  inclusive
         WO is the code for WIPO 
         EP is the code for European patents 

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
	// bucket_pd, bucket_pn string
	maxLoop int
)

func init() {
	rootCmd.AddCommand(toS3Cmd)
	initToS3Flags(toS3Cmd)
}

func initToS3Flags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&sindexUrl,"sindexd","s","","sindexd endpoint <url:port>")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","the prefix of the key")
	cmd.Flags().StringVarP(&iIndex,"iIndex","i","","Index table [PN|PD|BN|NP|OM|OB|XX-PN|XX-PD|XX-BN]")
	cmd.Flags().StringVarP(&marker, "marker", "k", "","Start with this Marker (Key) for the Get Prefix ")
	cmd.Flags().IntVarP(&maxKey,"maxKey","m",100,"maximum number of keys to be processed concurrently")
	cmd.Flags().IntVarP(&maxLoop,"maxLoop","",1,"maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the S3  bucket")
	cmd.Flags().BoolVarP(&check,"check","v",false,"Check mode")
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
		if iIndex == "PN" || iIndex == "PD" || iIndex == "BN" || iIndex == "XX"{
			gLog.Info.Printf("%s", missingPrefix);
			os.Exit(2)
		}
	}

	if len(bucket) == 0 {
		if bucket = viper.GetString("s3.bucket"); len(bucket) == 0 {
			gLog.Info.Println("%s", missingBucket);
			os.Exit(2)
		}
	}

	sindexd.Delimiter = delimiter
	sindexd.Host = strings.Split(sindexUrl,",")
	sindexd.HP = hostpool.NewEpsilonGreedy(sindexd.Host, 0, &hostpool.LinearEpsilonValueCalculator{})

	switch (iIndex) {
		case "PN","PD":  /* pubication date or publication number */
			migToS3("PD")     // Read sindexd pub date tables and write to both  s3 bucket-pd and bucket-pn
		case "BN":  migToS3b(index)
		case "OM","NP":  /* all other countries or Cite NPL table */
			migToS3(iIndex)
		case "OB":
			migToS3b(iIndex)
		case "XX-PN": /* incremental Publication nunber todo */
		case "XX-PD": /* incremental Publication date todo */
		case "XX-BN": /* incremental todo */
		default:
			gLog.Info.Printf("%s", "invalid index table : [PN|PD|BN|OM|OB]");
			os.Exit(2)
	}
}


func migToS3 (index string)  {

	indSpecs := directory.GetIndexSpec("PD")
	indSpecs1 := directory.GetIndexSpec("PN")
	svc      := s3.New(api.CreateSession())
	num := 0

	switch (index) {
	case  "OM" :
		prefix = ""
		i := indSpecs["OTHER"]
		i1 := indSpecs1["OTHER"]
		if i == nil || i1 == nil {
			gLog.Error.Printf("No OTHER entry in PD or PN Index spcification tables")
			os.Exit(2)
		}
		gLog.Info.Printf("Indexd specification PN: %v  - PD %v", *i1, *i)
	case "NP" :
		prefix ="XP"
		i := indSpecs["NP"]
		i1 := indSpecs1["NP"]
		if i == nil || i1 == nil {
			gLog.Error.Printf("No OTHER entry in PD or PN Index spcification tables")
			os.Exit(2)
		}
		gLog.Info.Printf("Indexd specification PN: %v  - PD %v", *i1, *i)
	default:
		/*  just continue */
	}
	gLog.Info.Printf("Index: %s - Prefix: %s - Start with key %s ",index,prefix,marker)
	/*
		Loop on  the get prefix
	     for each key value retuned by the list prefix
	     	write ( publication date key, value) to bucket-pd
	     	write ( publication number key, vale ) to bucket-pn
	 */
	for Nextmarker {
		if response = directory.GetSerialPrefix(index, prefix, delimiter, marker, maxKey, indSpecs); response.Err == nil {
			resp := response.Response
			var wg sync.WaitGroup
			wg.Add(len(resp.Fetched))

			for k, v := range resp.Fetched {
				/*   Publication date
				     key  format  CC/YYYY/MM/DD/NNNNNNNNNN/KC ( no KC for Cite NPL )
				*/
				if v1, err:= json.Marshal(v); err == nil {
					cc := strings.Split(k,"/")[0]
					go func(svc *s3.S3,k string,cc string,value []byte,check bool) {
						defer wg.Done()
						var (
							buck  = setBucketName(cc,bucket,"pd")
							buck1 = setBucketName(cc,bucket,"pn")
							keys  = strings.Split(k,"/")
							k1    = keys[0]
						)
						/*
						   extract  publication number key
						   from the publication date key
						*/
						for i := 4; i < len(keys); i++ {
							k1 += "/"+keys[i]
						}
						/*
							write to S3 buckets of not run in check mode
						 */
						if !check {
							if r, err := writeToS3(svc,  buck,k, value); err == nil {
								gLog.Trace.Println(buck,*r.ETag,*r)
								if r,err := writeToS3(svc,buck1,k1,value); err == nil {
									gLog.Trace.Println(buck1,*r.ETag)
								} else {
									gLog.Error.Printf("Error %v - Writing key %s to bucket %s", err, k1,buck1)
								}
							} else {
								gLog.Error.Printf("Error %v  - Writing key %s to bucket %s", err, k,buck)
							}
						} else {
							gLog.Trace.Printf("Check mode: Writing key/vakue %s/%s - to bucket %s",k,value,buck)
							gLog.Trace.Printf("Check mode: Writing key/value %s/%s - to bucket %s",k1,value,buck1)
						}
					} (svc,k,cc,v1,check)

				} else {
					gLog.Error.Printf("Error %v - Marshalling %s:%v",err, k,v)
					wg.Done()
				}
			}
			// Wait for all the go routine to be cpmpleted
			wg.Wait()

			if len(resp.Next_marker) == 0 {
				Nextmarker = false
			} else {
				num++
				marker = resp.Next_marker
				gLog.Info.Printf("Next marker => %s %d", marker, num)
				// stop if number of iteration > maxLoop
				if maxLoop != 0 && num > maxLoop {
					Nextmarker = false
				}
			}
		} else {
			gLog.Error.Printf("Error: %v getting prefix %s",response.Err,prefix)
			Nextmarker = false
		}
	}
}

func migToS3b (index string)  {

	indSpecs := directory.GetIndexSpec(index)
	svc      := s3.New(api.CreateSession())
	num := 0
	switch (index) {
	case  "OB" :
		prefix = ""
		i := indSpecs["OTHER"]
		if i == nil  {
			gLog.Error.Printf("No OTHER entry in PD or PN Index spcification tables")
			os.Exit(2)
		}
		gLog.Info.Printf("Indexd specification BN: %v"  ,  *i)

	default:
	}
	gLog.Info.Printf("Index: %s - Prefix: %s - Start with key %s ",index,prefix,marker)
	/*
			Loop on  the get prefix
		     for each key value retuned by the list prefix
		     	write ( publication date key, value) to bucket-pd
		     	write ( publication number key, vale ) to bucket-pn
	*/
	for Nextmarker {
		if response = directory.GetSerialPrefix(index, prefix, delimiter, marker, maxKey, indSpecs); response.Err == nil {
			resp := response.Response
			var wg sync.WaitGroup
			wg.Add(len(resp.Fetched))
			for k, v := range resp.Fetched {
				if v1, err:= json.Marshal(v); err == nil {
					cc := strings.Split(k,"/")[0]
					go func(svc *s3.S3,k string,cc string,value []byte,check bool) {
						defer wg.Done()
						var (
							buck  = setBucketName(cc,bucket,index)
						)
						/*
							write to S3 buckets of not run in check mode
						*/
						if !check {
							if r, err := writeToS3(svc,  buck,k, value); err == nil {
								gLog.Trace.Println(buck,*r.ETag,*r)
							} else {
								gLog.Error.Printf("Error %v  - Writing key %s to bucket %s", err, k,buck)
							}
						} else {
							gLog.Trace.Printf("Check mode: Writing key/vakue %s/%s - to bucket %s",k,value,buck)
						}
					} (svc,k,cc,v1,check)

				} else {
					gLog.Error.Printf("Error %v - Marshalling %s:%v",err, k,v)
					wg.Done()
				}
			}
			// Wait for all the go routine to be cpmpleted
			wg.Wait()

			if len(resp.Next_marker) == 0 {
				Nextmarker = false
			} else {
				marker = resp.Next_marker
				num++
				gLog.Info.Printf("Next marker => %s %d", marker,num)
			}
		} else {
			gLog.Error.Printf("Error: %v getting prefix %s",response.Err,prefix)
			Nextmarker = false
		}
	}
}

func writeToS3(svc  *s3.S3 , bucket string,  key string,meta []byte) (*s3.PutObjectOutput,error) {
    var (
    	data = make([]byte,0,0)  // empty byte array
    	err error
    	r  *s3.PutObjectOutput
    )
    gLog.Trace.Println(key,bucket)
	req:= datatype.PutObjRequest{
		Service : svc,
		Bucket: bucket,
		Key: key,
		Buffer: bytes.NewBuffer(data), // convert []byte into *bytes.Buffer
		Meta : meta,
	}
	// write to S3
	// if error not nil retry
	for i:= 0; i <= retryNumber; i++ {
		 if r,err = api.PutObject(req); err == nil {
			break
		 } else {
		 	 gLog.Error.Printf("Error: %v - number of retries: %d" , err, i )
		 	 /* wait time between 2 retries */
			 time.Sleep(waitTime * time.Millisecond)
		 }
	}
	return r,err
}

func setBucketName(cc string, bucket string, index string)  (string){
	buck := bucket + "-" + strings.ToLower(index)
	if cc == "XP" {
		buck  = buck+"-05"
	} else {
		/* hash the country code which should be equal to the prefix */
		buck = buck + "-" + fmt.Sprintf("%02d", utils.HashKey(cc, bucketNumber))
	}
	return buck
}





