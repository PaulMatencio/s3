package s3Client

import (
	"github.com/minio/minio-go"
	"log"
)

type S3 struct {
	Site 			Host         	 `json:"host"`
	Endpoint 		string    	 	 `json:"endpoint"`
	AccessKeyID  	string  		 `json:"accessKeyID"`
	SecretKey 		string    		 `json:"secretKey"`
	SSL 			bool             `json:"ssl"`
}

/*

 */
func (s3 *S3) GetHost() Host {
	return s3.Site
}

/*

 */
func (s3 *S3) GetEndPoint()  string {
	return s3.Endpoint
}

/*

 */
func (s3 *S3) GetAccessKey() string {
	return s3.AccessKeyID
}

/*

 */
func(s3 *S3) GetSecretKey() string {
	return s3.SecretKey
}


/*  structure s3Login */
type S3Login struct {
	S3      *S3
	MinioC  *minio.Client
}

/*

 */
func (s3Login *S3Login) GetS3Client() (*minio.Client) {
	return s3Login.MinioC
}

/*

 */
func (s3Login *S3Login) GetS3Config() (*S3) {
	return s3Login.S3
}

/*

 */
func SetS3Config(s3Config Config,location string) (S3) {
	 var (
	 	s 		S3
	 	site 	Host
	 	)
	 site = StructToMap(&s3Config.Hosts)[location]
		s.Site = site
		s.Endpoint = site.URL
		s.AccessKeyID = site.AccessKey
		s.SecretKey = site.SecretKey
		s.SSL = site.SSL
	return s
}

/*

 */

func S3Connect(s3 S3) (*minio.Client) {
	minioc, err := minio.New(s3.Endpoint, s3.AccessKeyID, s3.SecretKey,s3.SSL)
	if err != nil {
		log.Fatalln(err)
	}
	return minioc
}

/*

 */
func New(s3Config Config,location string)  (S3Login){

	s:=  SetS3Config(s3Config,location)
	return  S3Login {
		S3: &s,
		MinioC : S3Connect(s),
	}
}


