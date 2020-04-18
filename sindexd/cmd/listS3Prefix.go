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
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	listS3Cmd = &cobra.Command{
		Use:   "listS3",
		Short: "List S3 prefix using S3 API ",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			if index != "pn" || index != "pd" || index !="bn" {
				gLog.Warning.Printf("Index argument must be in [pn,pd,bn]")
				return
			}
			listS3(cmd,args)
		},
	}
	listS3Cmd2 = &cobra.Command{
		Use:   "listS3b",
		Short: "List S3 prefix using levelDB API",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			if index != "pn" || index != "pd" || index !="bn" {
				gLog.Warning.Printf("Index argument must be in [pn,pd,bn]")
				return
			}
			listS3b(cmd,args)
		},
	}
	prefixs,buck,index string
	prefixa []string
	maxS3Key,total int64
	loop int
)

func init() {
	RootCmd.AddCommand(listS3Cmd)
	initListS3Flags(listS3Cmd)
	RootCmd.AddCommand(listS3Cmd2)
	initListS3Flags(listS3Cmd2)
}

func initListS3Flags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&prefixs,"prefix","p","","prefix of the keys separated by commma")
	cmd.Flags().StringVarP(&marker, "marker", "k", "","Start with this marker (Key) for the Get Prefix ")
	cmd.Flags().Int64VarP(&maxS3Key,"maxKey","m",20,"maxmimum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the S3  bucket")
	cmd.Flags().IntVarP(&loop,"loop","L",1,"Number of loop using the next marker if there is one")
	cmd.Flags().StringVarP(&index,"index","i","pn","bucket group [pn|pd|bn]")
}

func listS3(cmd *cobra.Command, args []string) {
	var (
		prefixa = strings.Split(prefixs,",")
		nextmarker string
		err error
	)

	if len(prefixa) > 0 {
		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(len(prefixa))
		for _, prefix := range prefixa {
			go func(prefix string, bucket string) {
				defer wg.Done()
				gLog.Info.Println(prefix, bucket)
				if nextmarker, err = listS3Pref(prefix, marker, bucket); err != nil {
					gLog.Error.Println(err)
				}
			}(prefix, bucket)
		}
		wg.Wait()
		gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
	}

}


func listS3Pref(prefix string,marker string,bucket string) (string,error)  {

	var (
		nextmarker string
		N int
		cc = strings.Split(prefix,"/")[0]
	)
	// buck = bucket + "-" + index
	if len(cc) != 2 {
		return nextmarker,errors.New(fmt.Sprintf("Wrong country code: %s",cc))
	} else {
		buck = bucket + "-" + strings.ToLower(index)
		if cc == "XP" {
			buck  = buck+"-05"
		} else {
			buck = buck + "-" + fmt.Sprintf("%02d", utils.HashKey(cc, bucketNumber))

		}

		// buck := bucket + "-" + fmt.Sprintf("%02d", utils.HashKey(cc, bucketNumber))
		req := datatype.ListObjRequest{
			Service : s3.New(api.CreateSession()),
			Bucket: buck,
			Prefix : prefix,
			MaxKey : maxS3Key,
			Marker : marker,
			Delimiter: delimiter,
		}
		for {
			var (
				// nextmarker string
				result  *s3.ListObjectsOutput
				err error
			)
			N++
			if result, err = api.ListObject(req); err == nil {
				gLog.Info.Println(cc,buck,len(result.Contents))

				if l := len(result.Contents); l > 0 {
					total += int64(l)
					var wg1 sync.WaitGroup
					wg1.Add(len(result.Contents))
					for _, v := range result.Contents {
						gLog.Trace.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size,v.LastModified)
						svc := req.Service
						head := datatype.StatObjRequest{
							Service: svc,
							Bucket:  req.Bucket,
							Key:     *v.Key,
						}
						go func(request datatype.StatObjRequest) {
							rh := datatype.Rh{
								Key : head.Key,
							}
							defer wg1.Done()
							rh.Result, rh.Err = api.StatObject(head)
							//procStatResult(&rh)
							utils.PrintUsermd(rh.Key, rh.Result.Metadata)
						}(head)
					}
					if *result.IsTruncated {
						nextmarker = *result.Contents[l-1].Key
						gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
					}
					wg1.Wait()
				}
			} else {
				gLog.Error.Printf("%v", err)
				break
			}

			if  N < loop && *result.IsTruncated {
				req.Marker = nextmarker
			} else {
				gLog.Info.Printf("Total number of objects returned: %d",total)
				break
			}
		}

	}
	return nextmarker,nil
}

func listS3b(cmd *cobra.Command, args []string) {

	prefixa = strings.Split(prefixs,",")
	if len(prefixa) > 0 {
		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(len(prefixa))

		for _, prefix := range prefixa {
			go func(prefix string, bucket string) {
				defer wg.Done()
				var (
					s3Meta = datatype.S3Metadata{}
					marker string
					nextMarker string
					N = 0
				)
				for {
					if err, result := listS3bPref(prefix, marker,bucket); err != nil {
						gLog.Error.Println(err)
					} else {
						if err = json.Unmarshal([]byte(result), &s3Meta); err == nil {
							//gLog.Info.Println("Key:",s3Meta.Contents[0].Key,s3Meta.Contents[0].Value.XAmzMetaUsermd)
							//num := len(s3Meta.Contentss3Meta.Contents)
						 	l := len( s3Meta.Contents)
							for _, c := range s3Meta.Contents {
								//m := &s3Meta.Contents[0].Value.XAmzMetaUsermd
								m := &c.Value.XAmzMetaUsermd
								usermd, _ := base64.StdEncoding.DecodeString(*m)
								gLog.Info.Printf("Key:%s - Metadata: %s" ,c.Key, string(usermd))
							}
							if l > 0 {
								nextMarker = s3Meta.Contents[l-1].Key
								gLog.Info.Printf("Next marker %s Istruncated %v", nextMarker,s3Meta.IsTruncated)
							}
							N++
						} else {
							gLog.Info.Println(err)
						}
					}
					if !s3Meta.IsTruncated  {
						return
					} else {
						marker = nextMarker
						gLog.Info.Printf("marker %s", marker)
					}
					if N >= loop {
						return
					}
				}

			}(prefix, bucket)
		}
		wg.Wait()
		gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
	}
}

func listS3bPref(prefix string,marker string,bucket string) (error,string) {

	var (
		err error
		result,buck string
		contents []byte
	)
	// buck = bucket + "-" + index
	cc := strings.Split(prefix, "/")[0]
	if len(cc) != 2 {
		err =  errors.New(fmt.Sprintf("Wrong country code: %s", cc))
	} else {

		buck = bucket + "-" + strings.ToLower(index)
		if cc == "XP" {
			buck  = buck+"-05"
		} else {
			buck = buck + "-" + fmt.Sprintf("%02d", utils.HashKey(cc, bucketNumber))
		}
		/* curl  -s '10.12.201.11:9000/default/bucket/moses-meta-02?listType=DelimiterMaster&prefix=FR&maxKeys=2' */

		request:= "/default/bucket/"+buck+"?listType=DelimiterMaster&prefix="
		limit := "&maxKeys="+strconv.Itoa(int(maxS3Key))
		keyMarker:= "&marker="+marker

		// url := Host +":"+Port+request+prefix+limit+keyMarker
		url := levelDBUrl+request+prefix+limit+keyMarker
		gLog.Info.Println("URL:",url)

		if response,err := http.Get(url); err == nil {
			defer response.Body.Close()
			if contents, err = ioutil.ReadAll(response.Body); err == nil {
				return err,contentToJson(contents)
			}
		}
	}
	return err,result
}


func getUsermd(req datatype.ListObjRequest , result *s3.ListObjectsOutput, wg sync.WaitGroup){

	for _, v := range result.Contents {
		gLog.Info.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size,v.LastModified)
		svc := req.Service
		head := datatype.StatObjRequest{
			Service: svc,
			Bucket:  req.Bucket,
			Key:     *v.Key,
		}
		go func(request datatype.StatObjRequest) {
			rh := datatype.Rh{
				Key : head.Key,
			}
			defer wg.Done()
			rh.Result, rh.Err = api.StatObject(head)
			//procStatResult(&rh)
			utils.PrintUsermd(rh.Key, rh.Result.Metadata)
		}(head)

	}

}

func contentToJson(contents []byte ) string {

	result:= strings.Replace(string(contents),"\\","",-1)
	result = strings.Replace(result,"\"{","{",-1)
	// result = strings.Replace(result,"\"}]","}]",-1)
	result = strings.Replace(result,"\"}\"}","\"}}",-1)
	return result
}