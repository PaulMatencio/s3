package s3Client

import (
	"bytes"
	"github.com/minio/minio-go"
	"io"
	"os"
)


type S3GetRequest struct {
	MinioC 	*minio.Client
	Bucket 	string
	Key 	string
	Opts 	*minio.GetObjectOptions
	Trace 	bool
}



func (r *S3GetRequest) SetS3Client(s3c *minio.Client) {
	r.MinioC = s3c
}


func (r *S3GetRequest) SetBucketName(bucket string) {
	r.Bucket = bucket
}

func (r *S3GetRequest) SetKey(key string) {
	r.Bucket = key
}

func (r *S3GetRequest) SetPutOpts(options *minio.GetObjectOptions) {
	r.Opts= options
}

func (r *S3GetRequest) SetTrace(trace bool) {
	r.Trace = trace
}

func (r *S3GetRequest) S3BuildGetRequest(login *S3Login, bucket string, key string, options *minio.GetObjectOptions){
	r.MinioC,r.Bucket,r.Key,r.Opts,r.Trace	=  login.MinioC,bucket,key,options,false
	if TRACE {
		r.Trace	= true
	}
}

func(r *S3GetRequest) GetObject() (*bytes.Buffer, error) {
	var (
		object *minio.Object
		err error
	)
	if r.Trace {
		r.MinioC.TraceOn(os.Stdout)
	}
	if object,err = r.MinioC.GetObject(r.Bucket,r.Key,*r.Opts); err != nil {
		return nil,err
	}
	defer object.Close()
	return Stream(object)
}

//  object is  io reader
func Stream ( object io.Reader)  (*bytes.Buffer, error){
	buffer:= make([]byte,BUFFERSIZE)
	buf := new(bytes.Buffer)
	for {

		n, err := object.Read(buffer)
		if err == nil || err == io.EOF {
			buf.Write(buffer[:n])
			if err == io.EOF {
				buffer = buffer[:0] // clear the buffer fot the GC
				return buf,nil
			}
		} else {
			buffer = buffer[:0] // clear the buffer for the GC
			return buf,err
		}
	}
}



