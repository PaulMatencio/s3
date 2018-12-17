package api


import (
	"github.com/aws/aws-sdk-go/aws/endpoints"
	// "github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
)

func CreateSession() *session.Session {

	myCustomResolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {

		if service == endpoints.S3ServiceID {
			return endpoints.ResolvedEndpoint{
				URL:           "http://127.0.0.1:9000",
				SigningRegion: "us-east-1",
			}, nil
		}
		return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)

	}

	// endpoint := "http://127.0.0.1:9000"
	sess := session.Must(session.NewSession(&aws.Config{
		// Region:           aws.String("us-east-1"),
		EndpointResolver: endpoints.ResolverFunc(myCustomResolver),
		//Endpoint: &endpoint,
		S3ForcePathStyle: aws.Bool(true),
		LogLevel: aws.LogLevel(3),

	}))

	return sess

}