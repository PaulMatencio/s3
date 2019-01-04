package datatype

import "github.com/aws/aws-sdk-go/service/s3"

type  Rh struct {
	Key string
	Result   *s3.HeadObjectOutput
	Err error
}


type  Ro struct {
	Key string
	Result   *s3.GetObjectOutput
	Err error
}