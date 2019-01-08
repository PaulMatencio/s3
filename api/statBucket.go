
package api
import (
"github.com/aws/aws-sdk-go/service/s3"
"github.com/aws/aws-sdk-go/aws"
"github.com/s3/datatype"
)
func StatBucket(req datatype.StatBucketRequest) ( *s3.HeadBucketOutput,error){

	input := &s3.HeadBucketInput{
		Bucket: aws.String(req.Bucket),
	}

	return req.Service.HeadBucket(input)

}
