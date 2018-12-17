package api

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/jcelliott/lumber"

	// "github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
)

func CreateSession() *session.Session {

	myCustomResolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {

		if service == endpoints.S3ServiceID {
			return endpoints.ResolvedEndpoint{
				URL:           "http://10.12.201.11",
				SigningRegion: "us-east-1",
			}, nil
		}
		return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)

	}

	// logLevel := aws.LogLevel(1)
	// endpoint := "http://127.0.0.1:9000"
	logLevel := *aws.LogLevel(1)
	fmt.Println(lumber.GetLevel())
	// logLevel = aws.LogDebug


	sess := session.Must(session.NewSession(&aws.Config{
		// Region:           aws.String("us-east-1"),
		EndpointResolver: endpoints.ResolverFunc(myCustomResolver),
		//Endpoint: &endpoint,
		S3ForcePathStyle: aws.Bool(true),
		LogLevel: aws.LogLevel(logLevel),

	}))

	return sess

}