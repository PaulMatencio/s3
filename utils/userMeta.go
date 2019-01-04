package utils

import (
	"encoding/base64"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/gLog"
	"io/ioutil"
)

func GetuserMeta1(result *s3.GetObjectOutput) (string,error) {

	var (
		err error
		u []byte
	)

	if v,ok := result.Metadata["Usermd"];ok {
		u,err =  base64.StdEncoding.DecodeString(*v)
		return string(u),err
	} else {
		return "",err
	}
}

func GetUserMeta(meta map[string]*string) (string,error) {

	var (
		err error
		u []byte
	)

	if v,ok := meta["Usermd"];ok {
		u,err =  base64.StdEncoding.DecodeString(*v)
		return string(u),err
	} else {
		return "",err
	}

}


func BuildUserMeta(meta []byte) (map[string]*string) {

	metad := make(map[string]*string)
	if len(meta) > 0 {
		m := base64.StdEncoding.EncodeToString(meta)
		metad["Usermd"] = &m
	}
	return metad
}


func WriteUserMeta(meta map[string]*string,pathname string ) {

	var (
		usermd string
		err    error
	)

	if usermd,err  = GetUserMeta(meta); err == nil {
		if err:= ioutil.WriteFile(pathname,[]byte(usermd),0644); err != nil {
			gLog.Error.Printf("Error %v writing %s ",err,pathname)
		}

	}

}


func PrintUserMeta(key string, meta  map[string]*string) {

	for k,v := range meta {
		gLog.Trace.Printf("Key %s - Metadata (k=v) %s=%s",key, k,*v)
	}

	if usermd,err  := GetUserMeta(meta); err == nil {
		gLog.Info.Printf("key:%s - User Metadata: %s", key, usermd)
	}
}
