package api

import (
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"time"
)

func GetRaftBucket(url string, bucket string) (error,*datatype.RaftBucket) {
	var (
		raftSessions    datatype.RaftBucket
		req = "buckets/"+bucket
		res Resp
	)
	url  = url + "/_/" + req
	gLog.Trace.Printf("GetRaft Sessions url: %s\t Retry number: %d",url,retryNumber)
	for i := 1; i <= retryNumber; i++ {
		if res = doGet(url,raftSessions); res.Err == nil {
			if res.Status == 200 {
				b:= *res.Result
				raftSessions = b.(datatype.RaftBucket)
				break
			}else {
				gLog.Error.Printf("Status: %d %s",res.Status)
			}
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , res.Err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return res.Err,&datatype.RaftBucket{}
}
