package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/datatype"
)

func MakeBucket(req datatype.MakeBucketRequest) (*s3.CreateBucketOutput,error){


	input := &s3.CreateBucketInput{
		Bucket: aws.String(req.Bucket),
	}
	return req.Service.CreateBucket(input)

}
