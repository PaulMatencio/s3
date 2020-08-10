package api

import (
	"encoding/json"
	datatype "github.com/s3/datatype"
	"github.com/s3/gLog"
	"io/ioutil"
	"net/http"
	"time"
)
type Resp struct {
	Result interface{}
	Err    error
	Status int
}
var (
	err            error
	// waitTime       = utils.GetWaitTime(*viper.GetViper())
	waitTime time.Duration = 200
	// retryNumber  = utils.GetRetryNumber(*viper.GetViper());
	retryNumber = 3
)

func ListRaftSessions(url string) (error,*datatype.RaftSessions) {

	var (
		raftSessions    datatype.RaftSessions
		req = "raft_sessions"
	)
	url  = url + "/_/" + req
	gLog.Trace.Println("URL:", url)
	for i := 1; i <= retryNumber; i++ {
		if response, err := http.Get(url); err == nil {
			gLog.Trace.Printf("Response: %v",response)
			if response.StatusCode == 200 {
				defer response.Body.Close()
				if contents, err := ioutil.ReadAll(response.Body); err == nil {
					json.Unmarshal(contents,&raftSessions)
				}
			}
			break
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return err,&raftSessions
}

func GetRaftSessions(url string) (error,*datatype.RaftSessions) {
	var (
		raftSessions    datatype.RaftSessions
		req = "raft_sessions"
		res Resp
	)
	url  = url + "/_/" + req
	gLog.Trace.Printf("GetRaft Sessions url: %s\t Retry number: %d",url,retryNumber)
	for i := 1; i <= retryNumber; i++ {
		if res = doGet(url,raftSessions); res.Err == nil {
			if res.Status == 200 {
				raftSessions= res.Result.(datatype.RaftSessions)
				break
			}
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , res.Err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return res.Err,&raftSessions
}

func ListRaftBuckets(url string) (error,[]string) {
	var (
		buckets  []string
		req = "buckets"

		err error
	)
	url  = url + "/_/" + req
	for i := 1; i <= retryNumber; i++ {
		if response, err := http.Get(url); err == nil {
			gLog.Trace.Printf("Response: %v",response)
			if response.StatusCode == 200 {
				defer response.Body.Close()
				if contents, err := ioutil.ReadAll(response.Body); err == nil {
					json.Unmarshal(contents,&buckets)
				}
			}
			break
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return err,buckets
}

func GetRaftBuckets(url string) (error,[]string) {
	var (
		buckets  []string
		req = "buckets"
		// err error
		res Resp
	)
	url  = url + "/_/" + req
	gLog.Trace.Printf("GetRaft bucket url: %s",url)
	for i := 1; i <= retryNumber; i++ {
		if res =doGet(url,buckets); res.Err == nil {
			if res.Status == 200 {
				buckets = res.Result.([]string)
				break
			}
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return res.Err,buckets
}


func GetRaftLeader(url string) (error,datatype.RaftLeader) {
	var (
		req = "raft/leader"
		// err error
		rl  datatype.RaftLeader
		res Resp
	)
	url  = url + "/_/" + req
	gLog.Trace.Printf("GetRaft Leader url: %s",url)
	for i := 1; i <= retryNumber; i++ {
		if res = doGet(url,rl); res.Err == nil {
			if res.Status == 200 {
				rl = res.Result.(datatype.RaftLeader)
				break
			}
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return res.Err,rl
}

func doGet(url string,result interface{}) (Resp) {
	var (
		err error
		response *http.Response
	)

	if response, err = http.Get(url); err == nil {
		gLog.Trace.Printf("Response: %v",response)

		if response.StatusCode == 200 {
			defer response.Body.Close()
			if contents, err := ioutil.ReadAll(response.Body); err == nil {
				json.Unmarshal(contents,&result)
			}
		}
	}
	gLog.Trace.Printf("doGet url:%s\tStatus Code:%d",url,response.StatusCode)
	res := Resp {
		Result: result,
		Err: err,
		Status : response.StatusCode,
	}
	return res

}