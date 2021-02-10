// Copyright © 2021 NAME HERE <EMAIL ADDRESS>
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
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"sync"
)

// readLogCmd represents the readLog command
var (
	/*
	logFile string
	maxLine int
	*/

	toS3IncCmd = &cobra.Command{
		Use:   "incToS3",
		Short: "Incremental sindexd migration to S3",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("parsing sindexd log")
			toS3Inc(cmd,args)
		},
	}
)

func init() {
	rootCmd.AddCommand(toS3IncCmd)
	toS3IncCmd.Flags().StringVarP(&logFile,"logFile","i","","sindexd input log file")
	toS3IncCmd.Flags().IntVarP(&maxLine,"maxline","m",10,"maximum number of lines")
	toS3IncCmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the prefix of the S3  bucket names")
}

func toS3Inc(cmd *cobra.Command,args []string) {

	if len(logFile) == 0 {
		usage(cmd.Name())
		return
	}

	if len(bucket) == 0 {
		if bucket = viper.GetString("s3.bucket"); len(bucket) == 0 {
			gLog.Info.Println("%s", missingBucket);
			return
		}
	}
	tos3 := datatype.CreateSession{
		EndPoint:  viper.GetString("toS3.url"),
		Region:    viper.GetString("toS3.region"),
		AccessKey: viper.GetString("toS3.access_key_id"),
		SecretKey: viper.GetString("toS3.secret_access_key"),
	}
	svc := s3.New(api.CreateSession2(tos3))
	ToS3Inc(logFile,maxKey,bucket,svc)

}

func ToS3Inc(logFile string,maxKey int,bucket string,svc *s3.S3)  {

	var (
		scanner   *bufio.Scanner
		err error
		idxMap = buildIdxMap()
	)

	if scanner, err = utils.Scanner(logFile); err != nil {
		gLog.Error.Printf("Error scanning %v file %s",err,logFile)
		return
	}
	var wg sync.WaitGroup

	if linea, _ := utils.ScanLines(scanner, int(maxKey)); len(linea) > 0 {
		if l := len(linea); l > 0 {
			wg.Add(l)
			for _, v := range linea {
				go func(v string,svc *s3.S3) {
					defer wg.Done()
					oper := parseSindexdLog(v, idxMap, bucket, bucketNumber)
					// k:=oper.Key;buck:=oper.Bucket;value:=[]byte(oper.Value)
					if oper.Oper == "ADD" {
						gLog.Trace.Println(oper.Oper,oper.Bucket,oper.Key,oper.Value)
						if !check {
							//check if the object already exists
							stat := datatype.StatObjRequest{Service: svc, Bucket: oper.Bucket, Key: oper.Key,}
							if _, err := api.StatObject(stat); err == nil {
								gLog.Warning.Printf("Object %s already existed in the target Bucket %s", oper.Key, oper.Bucket)
							}  else {
								/* check if status 404 */
								if aerr, ok := err.(awserr.Error); ok {
									switch aerr.Code() {
									case s3.ErrCodeNoSuchBucket:
										gLog.Error.Printf("Stat object %v - Bucket %s not found - error %v",oper.Key,oper.Bucket,err)
									case s3.ErrCodeNoSuchKey:
										if r, err := writeToS3(svc, oper.Bucket, oper.Key,[]byte(oper.Value)); err == nil {
											gLog.Trace.Println(oper.Bucket, *r.ETag, *r)
										} else {
											gLog.Error.Printf("Error %v  - Writing key %s to bucket %s", err, oper.Key, oper.Bucket)
										}
									}
								}
							}
						} else {
							gLog.Info.Printf("Check mode: Writing key/vakue %s/%s - to bucket %s", oper.Key, oper.Value,oper.Bucket)
						}
					} else {
						if oper.Oper == "DELETE" {
							gLog.Trace.Println(oper.Bucket,oper.Oper,oper.Key)
							if !check {
								delreq := datatype.DeleteObjRequest{
									Service: svc,
									Bucket:  oper.Bucket,
									Key:     oper.Key,
								}
								// api.DeleteObjects(delreq)
								if _, err := api.DeleteObjects(delreq); err == nil {
									gLog.Info.Printf("Object %s is removed from %s", oper.Key, oper.Bucket)
								} else {
									gLog.Error.Printf("%v", err)
								}
							} else {
								gLog.Info.Printf("Check mode: Deleting object key %s from bucket %s", oper.Bucket, oper.Bucket)
							}
						}
					}
				}(v,svc)
			}
			wg.Wait()
		}
	}
}
