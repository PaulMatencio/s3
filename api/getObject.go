
package api
import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/s3/datatype"
)
func GetObjects(req datatype.GetObjRequest) (*s3.GetObjectOutput,error){

	input := &s3.GetObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
	}

	return req.Service.GetObject(input)


}

func GetObject( bucket string, key string) (*s3.GetObjectOutput,error){

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	return s3.New(CreateSession()).GetObject(input)
}