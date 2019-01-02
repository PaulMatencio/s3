package api
import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/s3/datatype"
)
func StatObjects(req datatype.StatObjRequest) (*s3.HeadObjectOutput,error){

	input := &s3.HeadObjectInput{

		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
	}


	return req.Service.HeadObject(input)


}
