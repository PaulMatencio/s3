package api

import (
	"encoding/json"
	"github.com/mitchellh/go-homedir"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/spf13/viper"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"time"
)

func GetRaftConfig(what string, url string) (error,bool) {

	var (
		req = "configuration/"+what
		err error
		rl  bool
	)

	url  = url + "/_/" + req
	gLog.Trace.Printf("GetRaft Leader url: %s",url)
	for i := 1; i <= retryNumber; i++ {
		if response, err := http.Get(url); err == nil {
			gLog.Trace.Printf("Response: %v",response)
			if response.StatusCode == 200 {
				defer response.Body.Close()
				if contents, err := ioutil.ReadAll(response.Body); err == nil {
					json.Unmarshal(contents,&rl)
				}
			}else {
				gLog.Error.Printf("Status: %d %s",response.StatusCode,response.Status)
			}
			break
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return err,rl
}

func ReadRaftConfig(topology string) (error,*datatype.Clusters) {
	var (
		filePath string
		c        datatype.Clusters
		// cluster,meta  string
	)
	if home, err := homedir.Dir(); err == nil {
		filePath = filepath.Join(home, topology)
		viper.Set("topology", filePath)
		if err, c := c.GetClusters(filePath); err == nil {
			return err,c
		}
	}
	return err,&c
}