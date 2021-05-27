package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3/datatype"
)

func DeleteObjects(req datatype.DeleteObjRequest) (*s3.DeleteObjectOutput,error){
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
	}
	return req.Service.DeleteObject(input)
}
