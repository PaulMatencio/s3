package utils

import (
	"encoding/base64"
	"github.com/aws/aws-sdk-go/service/s3"

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


func GetuserMeta(meta map[string]*string) (string,error) {

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