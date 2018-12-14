package s3Core

import (
	"github.com/minio/minio-go"
)

type S3ListRequest struct {
	MinioC 		*minio.Core
	Bucket 		string
	Prefix  	string
	Delimiter   string
	StartAfter  string /*  V2 */
	Marker 		string
	Limit       int
	FetchOwner	bool
	Trace 		bool
	// Results     *[]minio.ObjectInfo
}



func (r *S3ListRequest) SetS3Core(s3c *minio.Core) {
	r.MinioC = s3c
}

func (r *S3ListRequest) SetBucketName(bucket string) {
	r.Bucket = bucket
}

func (r *S3ListRequest) SetPrefix(prefix string) {
	r.Prefix = prefix
}

func (r *S3ListRequest) SetStartAfter(startAfter string) {
	r.StartAfter = startAfter
}

func (r *S3ListRequest) SetMarker(marker string) {
	r.Marker = marker
}

func (r *S3ListRequest) SetTrace(trace bool) {
	r.Trace = trace
}


func (r *S3ListRequest) GetMarker() string {
	return r.StartAfter
}

func (r *S3ListRequest) GetPrefix() string {
	return r.Prefix
}

func ( r *S3ListRequest) GetS3Core() *minio.Core {
	return r.MinioC
}





