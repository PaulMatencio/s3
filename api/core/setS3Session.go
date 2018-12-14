package s3Core

import (
	"github.com/minio/minio-go"
	"log"
	"github.com/s3Client/lib"
)


/*
	return
    	s3Client S3 struct
		minio.Core struct
 */
type S3Login struct {
	S3      *s3Client.S3
	MinioC  *minio.Core
}

/*
	return  a s3Core.S3Login structure ( s3Client S3 structure and s3core session)
 */
func New(s3Config s3Client.Config,location string)  (S3Login){
	s:=  s3Client.SetS3Config(s3Config,location)
	return  S3Login {
		S3: &s,
		MinioC : S3Connect(s),
	}
}

/*
	return an s3Core.S3Login session
 */
func (s3Login *S3Login) GetS3Core() (*minio.Core) {
	return s3Login.MinioC
}

/*
	Return an s3Client S3 structure
 */

func (s3Login *S3Login) GetS3Config() (*s3Client.S3) {
	return s3Login.S3
}

/*
	Connect to S3 endpoint
 */
func S3Connect(s3 s3Client.S3) (*minio.Core) {
	minioc, err := minio.NewCore(s3.Endpoint, s3.AccessKeyID, s3.SecretKey,s3.SSL)
	if err != nil {
		log.Fatalln(err)
	}
	return minioc
}