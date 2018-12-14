package s3Core

import (
	"github.com/minio/minio-go"
	"github.com/s3Client/lib"
	"os"
)

func (r *S3ListRequest) S3BuildListRequestV2(login *S3Login, bucket string, prefix string, fetchOwner bool,delimiter string, startAfter string, Marker string, limit int){

	r.MinioC 		=  login.MinioC
	r.Bucket 		=  bucket
	r.Prefix		=  prefix
	r.StartAfter 	=  startAfter
	r.Marker		=  Marker
	r.Limit         = limit
	r.Trace = false
	if s3Client.TRACE {
		r.Trace	= true
	}
	r.FetchOwner = fetchOwner


}

/* https://godoc.org/github.com/minio/minio-go#ListBucketResult */


func ListObjectsV2(r S3ListRequest) (minio.ListBucketV2Result, error) {
	if r.Trace {
		r.MinioC.TraceOn(os.Stdout)
	}
	return r.MinioC.ListObjectsV2(r.Bucket, r.Prefix, r.Marker, r.FetchOwner, r.Delimiter, r.Limit,r.StartAfter )
}
