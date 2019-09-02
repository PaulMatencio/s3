package datatype

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
	"net/http"
)

type GetObjRequest struct {

	Service 	*s3.S3
	Bucket 		string
	Key    		string

}

type CopyObjRequest struct {

	Service 	*s3.S3
	Sbucket 	string
	Tbucket     string
	Skey   		string
	Tkey        string

}

type CopyObjsRequest struct {

	Service 	*s3.S3
	Sbucket 	string
	Tbucket     string
	Skey   		[]string
	Tkey        []string

}

type PutObjRequest struct {
	Service     *s3.S3
	Bucket       string
	Key          string
	Buffer       *bytes.Buffer
	Usermd      map[string]string
	Meta        []byte
}

type FputObjRequest struct {
	Service     *s3.S3
	Bucket       string
	Key          string
	Inputfile     string
	Usermd       map[string]string
	Meta         []byte
}

type ListObjRequest struct {
	Service 	*s3.S3
	Bucket       string
	Prefix       string
	MaxKey	      int64
	Marker        string
	Delimiter     string
}


type ListBucketRequest struct {
	Service 	*s3.S3
}

type MakeBucketRequest struct {
	Service 	*s3.S3
	Bucket string
}


type DeleteBucketRequest struct {
	Service 	*s3.S3
	Bucket string
}

type DeleteObjRequest struct {
	Service 	*s3.S3
	Bucket       string
	Key          string
}

type StatObjRequest struct {
	Service  *s3.S3
	Bucket    string
	Key       string
}

type StatObjRequestV2 struct {
	Client      *http.Client
	Request      string
	AccessKey    string
	SecretKey    string
	Bucket   	 string
	Key          string
}



type StatBucketRequest struct {
	Service 	*s3.S3
	Bucket      string
}

type GetBucketPolicyRequest struct {
	Service 	*s3.S3
	Bucket      string
}

type GetBucketAclRequest struct {
	Service 	*s3.S3
	Bucket      string
}

type GetObjAclRequest struct {
	Service 	*s3.S3
	Bucket 		string
	Key    		string

}

type PutBucketAclRequest struct {
	Service 	*s3.S3
	Bucket 		string
	ACL  		 Acl

}

type PutObjectAclRequest struct {
	Service 	*s3.S3
	Bucket 		string
	Key         string
	ACL  		Acl

}

