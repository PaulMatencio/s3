package s3Core

import (
	"bytes"
	"github.com/minio/minio-go"
	"github.com/s3Client/lib"
	"io"
	"os"
)

type S3GetRequest struct {
	MinioC 	*minio.Core
	Bucket 	string
	Key 	string
	Opts 	*minio.GetObjectOptions
	Trace 	bool
}

func (r *S3GetRequest) S3BuildGetRequest(login *S3Login, bucket string, key string, options *minio.GetObjectOptions){
	r.MinioC,r.Bucket,r.Key,r.Opts,r.Trace	=  login.MinioC,bucket,key,options,false
	if s3Client.TRACE {
		r.Trace	= true
	}
}

func GetObject(r S3GetRequest) (*bytes.Buffer, *minio.ObjectInfo ,error) {

	var  n, size int
	if r.Trace {
		r.MinioC.TraceOn(os.Stdout)
	}
	object,objInfo,err := r.MinioC.GetObject(r.Bucket,r.Key,*r.Opts);
	defer object.Close()
	// Create a buffer
	bsize := int(objInfo.Size)
	if bsize > s3Client.BUFFERSIZE {
		bsize = s3Client.BUFFERSIZE
	}
	buffer:= make([]byte,bsize)
	buf := new(bytes.Buffer)
	for {
		n, err = object.Read(buffer)
		if err == nil || err == io.EOF {
			size += n
			buf.Write(buffer[0:n])
			if err == io.EOF  {
				return buf,&objInfo,nil
			}
		} else {
			return buf,&objInfo, err
		}
	}
}