package api

import (
	"github.com/aws/aws-sdk-go/service/s3"
)

func ListBuckets(svc *s3.S3) (*s3.ListBucketsOutput, error){

	input := &s3.ListBucketsInput{}
	return  svc.ListBuckets(input)

}
func ListBucket() (*s3.ListBucketsOutput, error){

	svc := s3.New(CreateSession())
	input := &s3.ListBucketsInput{}
	return  svc.ListBuckets(input)

}

