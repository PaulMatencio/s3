package cmd

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)
type Response struct {
	Content string
	Status int
	Err  error
}

var (
	statObjbCmd = &cobra.Command{
		Use:   "stat3b",
		Short: "Retrieve S3 user metadata using levelDB API",
		Long: `Retrieve S3 user metadata using levelDB API
Example: sindexd stat3b -i pn -k AT/000648/U1,AT/000647/U3,AT/,FR/500004/A,FR/567812/A`,
		Run: func(cmd *cobra.Command, args []string) {
			if index != "pn" && index != "pd" && index != "bn" {
				gLog.Warning.Printf("Index argument must be in [pn,pd,bn]")
				return
			}
			if len(bucket) == 0 {
				if bucket = viper.GetString("s3.bucket"); len(bucket) == 0 {
					gLog.Info.Println("%s", missingBucket);
					os.Exit(2)
				}
			}
			stat3b(cmd)
		},
	}
	statObjCmd = &cobra.Command{
		Use:   "stat3",
		Short: "Retrieve S3 user metadata using Amazon S3 SDK",
		Long: `Retrieve S3 user metadata using Amazon S3 SDK
Example: sindexd stat3 -i pn -k AT/000648/U1,AT/000647/U3,AT/,FR/500004/A,FR/567812/A`,
		Run: func(cmd *cobra.Command, args []string) {
			if index != "pn" && index != "pd" && index != "bn" {
				gLog.Warning.Printf("Index argument must be in [pn,pd,bn]")
				return
			}
			if len(bucket) == 0 {
				if bucket = viper.GetString("s3.bucket"); len(bucket) == 0 {
					gLog.Info.Println("%s", missingBucket);
					os.Exit(2)
				}
			}
			stat3(cmd)
		},
	}
	keys string
	keya []string
	resp Response

)

func init() {
	rootCmd.AddCommand(statObjCmd)
	rootCmd.AddCommand(statObjbCmd)
	initStatbFlags(statObjCmd)
	initStatbFlags(statObjbCmd)
}

func initStatbFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&keys,"key","k","","list of keys separated by a commma")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","if set, this will override the bucket name prefix  in the config file")
	cmd.Flags().StringVarP(&index,"index","i","pn","bucket group [pn|pd|bn]")
}

func stat3b(cmd *cobra.Command) {
	var (
		keya = strings.Split(keys,",")
		result string
		err error
		status int
	   // svc   *s3.S3
	)

	if len(keya) > 0 {
		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(len(keya))
		for _, key := range keya {
			go func(key string, bucket string) {
				defer wg.Done()
				gLog.Trace.Printf("key: %s - bucket: %s",key, bucket)
			 	if err,status,result = stat_3b(key); err == nil {
			 		if status == 200 {
						gLog.Info.Printf("Key %s - Usermd: %s", key, result)
					} else {
						gLog.Info.Printf("Key %s - status code %d - Result: %s", key, status, result)
					}
				} else {
					gLog.Error.Printf("key %s - Error: %v ",key,err)
				}
			}(key, bucket)
		}
		wg.Wait()
		gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
	}
}

func stat3(cmd *cobra.Command) {
	var (
		keya = strings.Split(keys,",")
		resp Response
		err error
		svc   *s3.S3
	)

	if len(keya) > 0 {
		start := time.Now()
		var wg sync.WaitGroup

		svc =  s3.New(api.CreateSession())
		for _, key := range keya {

			if len(cc) != 2 {
				err = errors.New(fmt.Sprintf("Wrong country code: %s", cc))
				gLog.Error.Printf("key: %s - Error : %v",key,err)
			} else {
				if len(index) > 0 {
					buck = setBucketName(cc, bucket, index)
				} else {
					buck = bucket
				}
				wg.Add(len(keya))
				go func(key string, bucket string) {
					defer wg.Done()
					gLog.Trace.Printf("key: %s - bucket: %s ", key, bucket)
					if resp = stat_3(bucket, key, svc); resp.Err == nil {
						gLog.Info.Printf("Key: %s - Usermd: %s\n", key, resp.Content)
					} else {

					}
				}(key, buck)
			}
		}
		wg.Wait()
		gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
	}
}


func stat_3b (key string) (error,int,string) {
	var (
		err error
		buck string
		result =""
		lvDBMeta = datatype.LevelDBMetadata{}
		resp Response
	)
	cc := strings.Split(key, "/")[0]
	if len(cc) != 2  {
		err =  errors.New(fmt.Sprintf("Wrong country code: %s", cc))
	} else {
		if len(index) >0 {
			buck = setBucketName(cc, bucket, index)
		} else {
			buck = bucket
		}
		resp = StatObjectLevelDB(buck,key)
		err = resp.Err
		if err == nil  {
			if resp.Status == 200 {
				if err = json.Unmarshal([]byte(resp.Content), &lvDBMeta); err == nil {
					if &lvDBMeta.Object != nil {
						m := &lvDBMeta.Object.XAmzMetaUsermd
						if usermd, err := base64.StdEncoding.DecodeString(*m); err == nil {
							result = string(usermd)
						} else {
							result = fmt.Sprintf("Key: %s - error: %v\n",key,err)
						}
					} else {
						resp.Status = 404
						result = fmt.Sprintf("Key: %s - Status code: %d\n",key,resp.Status)

					}
				}
			} else {
				result = fmt.Sprintf("Key: %s - Status code: %d\n",key,resp.Status)
				gLog.Warning.Printf("%s",result)
			}
		}
	}
	return err,resp.Status, result
}

func stat_3 (bucket string,key string,svc *s3.S3) ( Response) {
	var (
		resp = Response {
			Err: nil,
			Content: "",
		}
		head = datatype.StatObjRequest{
			Service: svc,
			Bucket:  bucket,
			Key:  key,
		}
	)
	 if result,err := api.StatObject(head) ; err == nil {
		 if v, ok := result.Metadata["Usermd"]; ok {
			 usermd, _ := base64.StdEncoding.DecodeString(*v)
			 gLog.Trace.Printf("key:%s - Usermd: %s", key, usermd)
			 resp.Content = string(usermd)
		 } else {
			 resp.Content = fmt.Sprintf("Missing user metadata")
		 }
	 } else {
	 	resp.Err = err
	 }
	return resp
}

/*  to be moved to api later
     api.StatObjectLevelDB

*/
func StatObjectLevelDB( buck string,key string) (Response){

    var (
    	request = "/default/parallel/"+buck+"/"+key+"?versionId="
    	resp = Response  {
    		Err : nil,
    		Content: "",
		}
    )
	/*
			build the request
		    curl -s '10.12.201.11:9000/default/parallel/<bucket>/<key>?verionId='
	*/
	url := levelDBUrl+request
	// gLog.Trace.Println("URL:",url)
	if response,err := http.Get(url); err == nil {
		gLog.Trace.Printf("Key %s - status code:  %d",key,response.StatusCode)

		//  should  return 200 or 500
		//  if an object does not exist , status code is 200
		//   the client must check the content the existence of the object

		if response.StatusCode == 200 {
			defer response.Body.Close()
			if contents, err := ioutil.ReadAll(response.Body); err == nil {
				resp.Content = ContentToJson(contents)
				resp.Status = response.StatusCode
			}
		} else {
			resp.Status = response.StatusCode
		}
	}  else {
		resp.Err= err
	}
	return resp
}

