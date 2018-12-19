package api

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/datatype"
)

func ListBuckets(req datatype.ListBucketRequest) (*s3.ListBucketsOutput, error){

	input := &s3.ListBucketsInput{}
	return  req.Service.ListBuckets(input)

}


func ListBucket() (*s3.ListBucketsOutput, error){

	svc := s3.New(CreateSession())
	input := &s3.ListBucketsInput{}
	return  svc.ListBuckets(input)

}

