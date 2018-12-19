package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/datatype"
)

func ListObject(req datatype.ListObjRequest)  ( *s3.ListObjectsOutput, error) {


	input := &s3.ListObjectsInput{

		Bucket: aws.String(req.Bucket),
		Prefix: aws.String(req.Prefix),
		MaxKeys: aws.Int64(req.MaxKey),
		Marker: aws.String(req.Marker),
		Delimiter: aws.String(req.Delimiter),
	}

	// svc.ListObjectsRequest(input)

	return  req.Service.ListObjects(input);

}
