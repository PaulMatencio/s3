package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func MakeBucket(svc *s3.S3, bucket string ) (*s3.CreateBucketOutput,error){

	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}
	return svc.CreateBucket(input)

}
