package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/spf13/viper"
)

func CreateSession() *session.Session {

	var sess     *session.Session
	logLevel := *aws.LogLevel(1)  // fmt.Println(lumber.GetLevel()) logLevel = aws.LogDebug

	// set the


	if viper.ConfigFileUsed() == ""  {

		myCustomResolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {

			if service == endpoints.S3ServiceID {
				return endpoints.ResolvedEndpoint{
			   URL: "http://127.0.0.1:9000",
			   //	URL:           "http://10.12.201.11",
			   SigningRegion: "us-east-1",
				}, nil
			}

			return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)

		}

		sess = session.Must(session.NewSession(&aws.Config{
			// Region:           aws.String("us-east-1"),
			Credentials: credentials.NewSharedCredentials("", "minio-account"),
			EndpointResolver: endpoints.ResolverFunc(myCustomResolver),
			//Endpoint: &endpoint,
			S3ForcePathStyle: aws.Bool(true),
			LogLevel: aws.LogLevel(logLevel),

		}))

	} else {

		/*
			Hard coded credential taken from application configuration file )
		*/

		sess, _ = session.NewSession(&aws.Config{

			Region:           aws.String(viper.GetString("sc_region")),
			Endpoint:         aws.String(viper.GetString("sc_url")),
			Credentials:      credentials.NewStaticCredentials(viper.GetString("sc_access_key_id"), viper.GetString("sc_secret_access_key"), ""),
			S3ForcePathStyle: aws.Bool(true),
			LogLevel:         aws.LogLevel(logLevel),

		})
	}

	return sess

}