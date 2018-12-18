package api
import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)
func StatObjects(svc *s3.S3, bucket string, key string) (*s3.HeadObjectOutput,error){

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}


	return svc.HeadObject(input)


}

func StatObject( bucket string, key string) (*s3.HeadObjectOutput,error){

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	return s3.New(CreateSession()).HeadObject(input)
}
