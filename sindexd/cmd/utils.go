package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"strings"
	"sync"
)

// transform content returned by the bucketd API into JSON string
func ContentToJson(contents []byte ) string {
	result:= strings.Replace(string(contents),"\\","",-1)
	result = strings.Replace(result,"\"{","{",-1)
	// result = strings.Replace(result,"\"}]","}]",-1)
	result = strings.Replace(result,"\"}\"}","\"}}",-1)
	gLog.Trace.Println(result)
	return result
}

func GetUsermd(req datatype.ListObjRequest , result *s3.ListObjectsOutput, wg sync.WaitGroup){

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
