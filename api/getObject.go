package api
import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)
func GetObjects(svc *s3.S3, bucket string, key string) (*s3.GetObjectOutput,error){

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	return svc.GetObject(input)


}

func GetObject( bucket string, key string) (*s3.GetObjectOutput,error){

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	return s3.New(CreateSession()).GetObject(input)


}
