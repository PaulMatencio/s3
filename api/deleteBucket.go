package api

import (
"github.com/aws/aws-sdk-go/aws"
"github.com/aws/aws-sdk-go/service/s3"
)

func DeleteBucket(svc *s3.S3, bucket string ) (*s3.DeleteBucketOutput,error){

	input := &s3.DeleteBucketInput {
		Bucket: aws.String(bucket),
	}

	return svc.DeleteBucket(input)

}
