package cmd

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	statObjCmdb = &cobra.Command{
		Use:   "statObjb",
		Short: "Retrieve S3 user metadata using levelDB API",
		Long: ``,
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

			statObjs(cmd,"b")
		},
	}
	statObjCmd = &cobra.Command{
		Use:   "statObj",
		Short: "Retrieve S3 user metadata using Amazon S3 SDK",
		Long: ``,
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
			statObjs(cmd,"")
		},
	}
	keys string
	keya []string
	resp Response

)

func init() {
	rootCmd.AddCommand(statObjCmd)
	initStatbFlags(statObjCmd)
}

func initStatbFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&keys,"key","k","","list of keys separated by a commma")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the prefix name of the S3  bucket")
	cmd.Flags().StringVarP(&index,"index","i","pn","bucket group [pn|pd|bn]")
}

func statObjs(cmd *cobra.Command, b string) {
	var (
		keya = strings.Split(keys,",")
		result string
		err error
	)

	if len(keya) > 0 {
		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(len(keya))
		for _, key := range keya {
			go func(key string, bucket string) {
				defer wg.Done()
				gLog.Info.Println(key, bucket)
			 	if err,result = statObjb(key,true); err == nil {
			 		gLog.Info.Println(result)
				} else {
					gLog.Error.Println(err)
				}
			}(key, bucket)
		}
		wg.Wait()
		gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
	}
}

func statObjb (key string, b bool) (error,string) {
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
		if len(cc) >0 {
			buck = setBucketName(cc, bucket, index)
		} else {
			buck = bucket
		}
		if b {
			resp = statb(buck,key)
			err = resp.Err
		} else {
			resp = statb(buck, key)
			err = resp.Err
		}

		if err == nil  {
			if resp.Status == 200 {
				gLog.Trace.Println(resp.Content)
				if err = json.Unmarshal([]byte(resp.Content), &lvDBMeta); err == nil {
					m := &lvDBMeta.Object.XAmzMetaUsermd
					usermd, _ := base64.StdEncoding.DecodeString(*m)
					result = string(usermd)
					// gLog.Info.Printf("Key: %s - Usermd: %s", key, result)
				}
			} else {
				result = fmt.Sprintf("Key: %s - status code: %d\n",key,resp.Status)
				// gLog.Warning.Printf(result)
			}
		}
	}
	return err,result
}

func statb( buck string,key string) (Response){

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
	gLog.Trace.Println("URL:",url)
	if response,err := http.Get(url); err == nil {
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