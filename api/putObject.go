package s3Client

import (
	"bytes"
	"errors"
	"github.com/minio/minio-go"
	"io"
	"os"
)

type S3PutRequest struct {
	MinioC 	*minio.Client
	Buf 	*bytes.Buffer
	Bucket 	string
	Key 	string
	PutOpts *minio.PutObjectOptions
	Trace 	bool
}



func (r *S3PutRequest) SetS3Client(s3c *minio.Client) {
	r.MinioC = s3c
}

func (r *S3PutRequest) SetByteBuffer(buf *bytes.Buffer) {
	r.Buf = buf
}

func (r *S3PutRequest) SetBucketName(bucket string) {
	r.Bucket = bucket
}

func (r *S3PutRequest) SetKey(key string) {
	r.Bucket = key
}

func (r *S3PutRequest) SetPutOpts(options *minio.PutObjectOptions) {
	r.PutOpts= options
}

func (r *S3PutRequest) SetTrace(trace bool) {
	r.Trace = trace
}

func (r *S3PutRequest) S3BuildPutRequest(login *S3Login, bucket string, key string, buf *bytes.Buffer, putoptions *minio.PutObjectOptions)  {
	r.MinioC,r.Buf,r.Bucket,r.Key,r.PutOpts,r.Trace 	=  login.MinioC,buf,bucket,key,putoptions,false
	if TRACE {
		r.Trace	= true
	}
}


func (r *S3PutRequest) PutObject() (int64, error) {

	if r.Trace {
		r.MinioC.TraceOn(os.Stdout)
	}

	var (
		rd io.Reader
		b []byte
	)

	if b = r.Buf.Bytes(); b != nil {
		 rd = bytes.NewReader(b)
	} else {
		return 0,errors.New("Input buffer image is empty !!")
	}

	return r.MinioC.PutObject(r.Bucket,r.Key,rd, int64(len(b)),*r.PutOpts);

}


