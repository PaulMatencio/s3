package datatype

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
)

type GetObjRequest struct {

	Service 	*s3.S3
	Bucket 		string
	Key    		string

}

type PutObjRequest struct {

	Service     *s3.S3
	Bucket       string
	Key          string
	Buffer       *bytes.Buffer
	Meta        []byte
}

type FputObjRequest struct {

	Service     *s3.S3
	Bucket       string
	Key          string
	Inputfile     string
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
	Bucket       string

}

type DeleteObjRequest struct {

	Service 	*s3.S3
	Bucket       string
	Key          string

}