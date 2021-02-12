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
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	hostpool "github.com/bitly/go-hostpool"
	directory "github.com/moses/directory/lib"
	sindexd "github.com/moses/sindexd/lib"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
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
			migrateToS3(cmd, args)
		},
	}
	// bucket_pd, bucket_pn string
	maxLoop int
	redo bool
)

type addRequest struct {
	Service  *s3.S3
	Bucket    string
	Index     string
	Resp      *sindexd.Response
	Check     bool

}

type delRequest struct {
	Service  *s3.S3
	Bucket    string
	Index     string
	Key       string
}

func init() {
	rootCmd.AddCommand(toS3Cmd)
	initToS3Flags(toS3Cmd)
}

func initToS3Flags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&sindexUrl, "sindexd", "s", "", "sindexd endpoint <url:port>")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "the prefix of the key")
	cmd.Flags().StringVarP(&iIndex, "iIndex", "i", "", "Index table [PN|PD|BN|NP|OM|OB|XX-PN|XX-PD|XX-BN]")
	cmd.Flags().StringVarP(&marker, "marker", "k", "", "Start with this Marker (Key) for the Get Prefix ")
	cmd.Flags().IntVarP(&maxKey, "maxKey", "m", 100, "maximum number of keys to be processed concurrently")
	cmd.Flags().IntVarP(&maxLoop, "maxLoop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the prefix of the S3  bucket names")
	cmd.Flags().BoolVarP(&check, "check", "v", false, "Check mode")
	cmd.Flags().BoolVarP(&check, "redo", "r", false, "Redo the migration of the indexd-id")
}

/*
	Set bucket suffix based the type of index ( pn|pd|bn ) and on the hash code of the Country code
    moses-meta -> moses-meta-pn-xx   pn:publication number indexd xx:hashKey(country code)
    XP -> 05

 */
func setBucketName(cc string, bucket string, index string) string {
	buck := bucket + "-" + strings.ToLower(index)
	if cc == "XP" {
		buck = buck + "-05"
	} else {
		/* hash the country code which should be equal to the prefix */
		buck = buck + "-" + fmt.Sprintf("%02d", utils.HashKey(cc, bucketNumber))
	}
	return buck
}


/*
	Write to S3  ( only meta data, empty object

 */
func writeToS3(svc *s3.S3, bucket string, key string, meta []byte) (*s3.PutObjectOutput, error) {
	var (
		data = make([]byte, 0, 0) // empty byte array
		err  error
		r    *s3.PutObjectOutput
	)
	gLog.Trace.Printf("Writting key %s to bucket %s\n",key , bucket)
	req := datatype.PutObjRequest{
		Service: svc,
		Bucket:  bucket,
		Key:     key,
		Buffer:  bytes.NewBuffer(data), // convert []byte into *bytes.Buffer
		Meta:    meta,
	}

	// write to S3
	// if error not nil retry

	for i := 1; i <= retryNumber; i++ {
		if r, err = api.PutObject(req); err == nil {
			break
		} else {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchBucket:
					gLog.Error.Printf("Error: %v", err)
					break
				}
			}
			gLog.Error.Printf("Error: %v - number of retries: %d", err, i)
			/* wait time between 2 retries */
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return r, err
}

/*
     migration  of sindexd tables to S3
*/

func migrateToS3(cmd *cobra.Command, args []string) {

	if len(sindexUrl) == 0 {
		sindexUrl = viper.GetString("sindexd.url")
	}
	if len(iIndex) == 0 {
		iIndex = "PN"
	}

	// indSpecs := directory.GetIndexSpec(iIndex)
	if len(prefix) == 0 {
		if iIndex == "PN" || iIndex == "PD" || iIndex == "BN" || iIndex == "XX" {
			gLog.Info.Printf("%s", missingPrefix)
			os.Exit(2)
		}
	}

	if len(bucket) == 0 {
		if bucket = viper.GetString("s3.bucket"); len(bucket) == 0 {
			gLog.Info.Println("%s", missingBucket)
			os.Exit(2)
		}
	}

	sindexd.Delimiter = delimiter
	sindexd.Host = strings.Split(sindexUrl, ",")
	sindexd.HP = hostpool.NewEpsilonGreedy(sindexd.Host, 0, &hostpool.LinearEpsilonValueCalculator{})

	switch iIndex {
	case "PN", "PD": /* pubication date or publication number */
		migToS3("PD") // Read sindexd pub date tables and write to both  s3 bucket-pd and bucket-pn
	case "BN":
		migToS3b(iIndex)
	case "OM", "NP": /* all other countries or Cite NPL table */
		migToS3(iIndex)
	case "OB":
		migToS3b(iIndex)
	case "XX-PN":
		incToS3("XX", "PN")
	case "XX-PD":
		incToS3("XX", "PD")
	case "XX-BN": /* incremental todo */
		incToS3("XX", "BN")
	default:
		gLog.Info.Printf("%s", "invalid index table : [PN|PD|BN|OM|OB|XX-PN|XX-PD|XX-BN]")
		os.Exit(2)
	}
}

/*
	Full  migration  tof  pn and pd sindexd tables to S3
*/

func migToS3(index string) {
	var (
	indSpecs = directory.GetIndexSpec("PD")
	indSpecs1 = directory.GetIndexSpec("PN")

	tos3 = datatype.CreateSession{
		EndPoint:  viper.GetString("toS3.url"),
		Region:    viper.GetString("toS3.region"),
		AccessKey: viper.GetString("toS3.access_key_id"),
		SecretKey: viper.GetString("toS3.secret_access_key"),
	}
	svc = s3.New(api.CreateSession2(tos3))
	num,total,totalc = 0,0,0 // num =  number of loop , total = total number of k
	mu  sync.Mutex   // mu = mutex for counter totalc, mu1 mutex for counter total
	)

	switch index {
	case "OM":
		prefix = ""
		i := indSpecs["OTHER"]
		i1 := indSpecs1["OTHER"]
		if i == nil || i1 == nil {
			gLog.Error.Printf("No OTHER entry in PD or PN Index spcification tables")
			os.Exit(2)
		}
		gLog.Info.Printf("Indexd specification PN: %v  - PD %v", *i1, *i)
	case "NP":
		prefix = "XP"
		i := indSpecs["NP"]
		i1 := indSpecs1["NP"]
		if i == nil || i1 == nil {
			gLog.Error.Printf("No NP entry in PD or PN Index spcification tables")
			os.Exit(2)
		}
		gLog.Info.Printf("Indexd specification PN: %v  - PD %v", *i1, *i)
	default:
		/*  just continue */
	}
	gLog.Info.Printf("Index: %s - Prefix: %s - Start with key %s ", index, prefix, marker)
	/*
			Loop on  the get prefix
		     for each key value retuned by the list prefix
		     	write ( publication date key, value) to bucket-pd
		     	write ( publication number key, vale ) to bucket-pn
	*/
	start := time.Now()
	for Nextmarker {

		if response = directory.GetSerialPrefix(index, prefix, delimiter, marker, maxKey, indSpecs); response.Err == nil {
			resp := response.Response
			var wg sync.WaitGroup
			wg.Add(len(resp.Fetched))

			for k, v := range resp.Fetched {

				/*
					Publication date
				    key  format  CC/YYYY/MM/DD/NNNNNNNNNN/KC ( no KC for Cite NPL )
				*/

				if v1, err := json.Marshal(v); err == nil {
					cc := strings.Split(k, "/")[0]
					total ++
					go func(svc *s3.S3, k string, cc string, value []byte, check bool) {
						defer wg.Done()
						var (
							buck  = setBucketName(cc, bucket, "pd")
							buck1 = setBucketName(cc, bucket, "pn")
							keys  = strings.Split(k, "/")
							k1    = keys[0]
						)
						/*
						   extract  publication number key
						   from the publication date key
						*/
						for i := 4; i < len(keys); i++ {
							k1 += "/" + keys[i]
						}
						/*
							write to S3 buckets of not run in check mode
						*/
						if !check {
							// if redo , bypass write to S3  if the object already existed
							if redo {
								stat := datatype.StatObjRequest{Service: svc, Bucket: buck, Key: k}
								if _, err := api.StatObject(stat); err == nil {
									gLog.Trace.Printf("Object %s already existed in the target Bucket %s", k, buck)
									mu.Lock()
									totalc++
									mu.Unlock()
									return
								} else {
									if  err.(awserr.Error).Code() == s3.ErrCodeNoSuchBucket {
										gLog.Error.Printf("Bucket %s is not found - error %v", buck, err)
										return
									}
								}
							}
							//  write toS3
							if r, err := writeToS3(svc, buck, k, value); err == nil {
								gLog.Trace.Println(buck, *r.ETag, *r)
								if r, err := writeToS3(svc, buck1, k1, value); err == nil {
									gLog.Trace.Println(buck1, *r.ETag)
								} else {
									gLog.Error.Printf("Error %v - Writing key %s to bucket %s", err, k1, buck1)
								}
							} else {
								gLog.Error.Printf("Error %v  - Writing key %s to bucket %s", err, k, buck)
							}
						} else {
							gLog.Trace.Printf("Check mode: Writing key/vakue %s/%s - to bucket %s", k, value, buck)
							gLog.Trace.Printf("Check mode: Writing key/value %s/%s - to bucket %s", k1, value, buck1)
						}
					}(svc, k, cc, v1, check)

				} else {
					gLog.Error.Printf("Error %v - Marshalling %s:%v", err, k, v)
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
				if maxLoop != 0 && num >= maxLoop {
					Nextmarker = false
				}
			}
		} else {
			gLog.Error.Printf("Error: %v getting prefix %s", response.Err, prefix)
			Nextmarker = false
		}
	}
	gLog.Info.Printf("Index/Prefix: %s/%s -Total number of migrated objects %d - Total number of skipped objects %d - Duration %v",index,prefix,total,totalc,time.Since(start))
}

/*
	Full  migration  of bn  sindexd tables to S3
*/


func migToS3b(index string) {
	var (
		indSpecs = directory.GetIndexSpec(index)
		svc = s3.New(api.CreateSession())
		num,total,totalc = 0,0,0 // num =  number of loop , total = total number of k
		mu       sync.Mutex   // mu = mutex for counter totalc, mu1 mutex for counter total
	)
	switch index {
	case "OB":
		prefix = ""
		i := indSpecs["OTHER"]
		if i == nil {
			gLog.Error.Printf("No OTHER entry in PD or PN Index spcification tables")
			os.Exit(2)
		}
		gLog.Info.Printf("Indexd specification BN: %v", *i)
	default:
	}
	gLog.Info.Printf("Index: %s - Prefix: %s - Start with key %s ", index, prefix, marker)

	/*
			Loop on  the get prefix
		     for each key value retuned by the list prefix
		     	write ( publication date key, value) to bucket-pd
		     	write ( publication number key, vale ) to bucket-pn

	*/

	start := time.Now()
	for Nextmarker {
		if response = directory.GetSerialPrefix(index, prefix, delimiter, marker, maxKey, indSpecs); response.Err == nil {
			resp := response.Response
			var wg sync.WaitGroup
			wg.Add(len(resp.Fetched))

			for k, v := range resp.Fetched {
				if v1, err := json.Marshal(v); err == nil {
					total ++
					cc := strings.Split(k, "/")[0]
					go func(svc *s3.S3, k string, cc string, value []byte, check bool) {
						defer wg.Done()
						var (
							buck = setBucketName(cc, bucket, index)
						)
						/*
							write to S3 buckets of not run in check mode
						*/
						if !check {
							// if redo , bypass write to S3  if the object already existed
							if redo {
								stat := datatype.StatObjRequest{Service: svc, Bucket: buck, Key: k}
								if _, err := api.StatObject(stat); err == nil {
									gLog.Trace.Printf("Object %s already existed in the target Bucket %s", k, buck)
									mu.Lock()
									totalc ++
									mu.Unlock()
									return
								} else {
									if  err.(awserr.Error).Code() == s3.ErrCodeNoSuchBucket {
										gLog.Error.Printf("Bucket %s is not found - error %v", buck, err)
										return
									}
								}
							}
							// write to S3
							if r, err := writeToS3(svc, buck, k, value); err == nil {
								gLog.Trace.Println(buck, *r.ETag, *r)
							} else {
								gLog.Error.Printf("Error %v  - Writing key %s to bucket %s", err, k, buck)
							}
						} else {
							gLog.Trace.Printf("Check mode: Writing key/vakue %s/%s - to bucket %s", k, value, buck)
						}
					}(svc, k, cc, v1, check)

				} else {
					gLog.Error.Printf("Error %v - Marshalling %s:%v", err, k, v)
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
				gLog.Info.Printf("Next marker => %s %d", marker, num)
				// stop if number of iteration > maxLoop
				if maxLoop != 0 && num >= maxLoop {
					Nextmarker = false
				}
			}
		} else {
			gLog.Error.Printf("Error: %v getting prefix %s", response.Err, prefix)
			Nextmarker = false
		}
	}
	gLog.Info.Printf("Index/Prefix: %s/%s -Total number of migrated objects %d - Total number of skipped objects %d - Duration %v",index,prefix,total,totalc,time.Since(start))
}



func incToS3(index string, index1 string) {

	var (
		Key1      = []string{}
		indSpecs  = directory.GetIndexSpec(index)  //should be XX
		indSpecs1 = directory.GetIndexSpec(index1) // should be [PN|PD|BN
		keyObj    = make(map[string][]byte)
		num       = 0
		loaded    = new(Loaded)
	)

	tos3 := datatype.CreateSession{
		EndPoint:  viper.GetString("toS3.url"),
		Region:    viper.GetString("toS3.region"),
		AccessKey: viper.GetString("toS3.access_key_id"),
		SecretKey: viper.GetString("toS3.secret_access_key"),
	}
	svc := s3.New(api.CreateSession2(tos3))

	// fetch key=value from the XX table
	for Nextmarker {

		//  retrieve  keys from XX  index table
		if response = directory.GetSerialPrefix(index, prefix, delimiter, marker, maxKey, indSpecs); response.Err == nil {
			resp := response.Response
			for k, v := range resp.Fetched {
				if v1, err := json.Marshal(v); err == nil {
					gLog.Trace.Println(k, string(v1))
					keyObj[k] = v1
				}
			}

			//  Build an array of document keys  Key1 to be retrieved

			for k, v := range keyObj {
				if err := json.Unmarshal(v, &loaded); err == nil {
					K := strings.Split(k, "/")
					pubDate := loaded.PubDate[0:4] + "/" + loaded.PubDate[4:6] + "/" + loaded.PubDate[6:8]
					if len(K) == 4 {
						if index1 == "PD" {
							Key1 = append(Key1, K[1]+"/"+pubDate+"/"+K[2]+"/"+K[3])
						} else {
							Key1 = append(Key1, K[1]+"/"+K[2]+"/"+K[3])
						}
					} else if len(K) == 3 {
						if index1 == "PD" {
							Key1 = append(Key1, K[1]+"/"+pubDate+"/"+K[2])
						} else {
							Key1 = append(Key1, K[1]+"/"+K[2])
						}
					} else {
						gLog.Warning.Printf("Invalid input key: %s is discarded", k)
					}
				}
			}
			//  sort the  key array Key1
			sort.Strings(Key1)

			// retrieve the document new
			// Build an index

			specs := make(map[string][]string)
			for _, v := range Key1 {
				// index := aKey[i][0:2]
				gLog.Trace.Println(v)
				index := v[0:2]
				if index == "XP" {
					pn := strings.Split(v, "/")
					switch index1 {
					case "PD":
						if pn[4] >= "55000000" && pn[4] < "56000000" {
							index = "NP"
						}
					case "PN", "BN":
						if pn[1] >= "55000000" && pn[1] < "56000000" {
							index = "NP"
						}

					default:
						gLog.Error.Println("Wrong value of index1 %s", index1)
					}
				}
				if indSpecs1[index] == nil {
					index = "OTHER"
				}
				specs[index] = append(specs[index], v)
			}
			gLog.Trace.Println(specs)

			//  retrieve  key=value from [PN|PD|BN] of source URL

			responses := directory.GetAsyncKeys(specs, indSpecs1)
			var indSpec *sindexd.Index_spec
			for _, r := range responses {

				indSpec = r.Index_Spec
				/*  delete All Key not found from the target URL, one key at a time */
				for _, v := range r.Response.Not_found {
					gLog.Warning.Printf("Key %s is not found in indSpect %v in Host %v", v, *indSpec, sindexd.HP.Hosts())
					indSpec = r.Index_Spec
					// there is no legacy BNS XP  tables
					if v[0:2] != "XP" || index1 != "BN" {
						if !check {
							gLog.Warning.Printf("Deleting key %s from bucket %s at endpoint %s",v,bucket, tos3.EndPoint)
							// deleting key from the target S3
							var delreq = delRequest {Service: svc,Bucket: bucket, Key:v,Index:index1,}
							if _,err := delFromS3(delreq); err == nil {
								gLog.Info.Printf("Object %s is removed from %s",v,bucket)
							} else {
								gLog.Error.Printf("%v",err)
							}
						} else {
							gLog.Info.Printf("Check Mode: Deleting key %s from bucket %s at endpoint %s",v,bucket, tos3.EndPoint)
						}
					}
				}
				/*
				     Add concurrently key=value to toS3
				*/
				var addreq = addRequest {Service: svc,Bucket: bucket,Index: index1,Resp : r.Response,Check: check,}
				addToS3(addreq)
			}

			// Reuse the MAP storage rather then let the Garbage free the unused storage
			// this may  create overhead without real benefit

			for k := range keyObj {
				delete(keyObj, k)
			}
			Key1 = Key1[:0]
			if len(resp.Next_marker) == 0 {
				Nextmarker = false
			} else {
				marker = resp.Next_marker
				num++
				gLog.Info.Printf("Next marker => %s", marker)
			}
		} else {
			gLog.Error.Printf("Error: %v getting prefix %s", response.Err, prefix)
			Nextmarker = false
		}
	}
}


func addToS3(req addRequest) {

	var wg sync.WaitGroup
	for k, v := range req.Resp.Fetched {
		if v1, err := json.Marshal(v); err == nil {
			wg.Add(1)
			cc := strings.Split(k, "/")[0]
			go func(svc *s3.S3, k string, cc string, value []byte, check bool) {
				defer wg.Done()
				buck:= setBucketName(cc, req.Bucket, req.Index)
				if !check {
					//check if the object already exists
					stat := datatype.StatObjRequest{Service: svc, Bucket: buck, Key: k,}
					if result, err := api.StatObject(stat); err == nil {
						if len(*result.ETag) > 0 {
							gLog.Warning.Printf("Object %s already existed in the target Bucket %s", k, buck)
						} else {
							if r, err := writeToS3(svc, buck, k, value); err == nil {
								gLog.Trace.Println(buck, *r.ETag, *r)
							} else {
								gLog.Error.Printf("Error %v  - Writing key %s to bucket %s", err, k, buck)
							}
						}
					} else {
						/* check if status 404 */
						if aerr, ok := err.(awserr.Error); ok {
							switch aerr.Code() {
							case s3.ErrCodeNoSuchBucket:
								gLog.Error.Printf("Stat object %v - Bucket %s not found - error %v",k,buck,err)
							case s3.ErrCodeNoSuchKey:
								if r, err := writeToS3(svc, buck, k, value); err == nil {
									gLog.Trace.Println(buck, *r.ETag, *r)
								} else {
									gLog.Error.Printf("Error %v  - Writing key %s to bucket %s", err, k, buck)
								}
							}
						}
					}
				} else {
					gLog.Info.Printf("Check mode: Writing key/vakue %s/%s - to bucket %s", k, value, buck)
				}
			}(req.Service, k, cc, v1, req.Check)

		} else {
			gLog.Error.Printf("Error %v - Marshalling %s:%v", err, k, v)
		}
	}

	wg.Wait()

}

func delFromS3(req delRequest) (*s3.DeleteObjectOutput,error){

	cc := strings.Split(req.Key, "/")[0]
	delreq:= datatype.DeleteObjRequest{
		Service : req.Service,
		Bucket:  setBucketName(cc, req.Bucket, req.Index),
		Key: req.Key,
	}
	return api.DeleteObjects(delreq)

}

