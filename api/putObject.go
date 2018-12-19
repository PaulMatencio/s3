package api

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
)

func FputObjects(svc *s3.S3, bucket string, key string, filename string, meta map[string]*string) (*s3.PutObjectOutput,error){

	f, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return nil,err
	}

	defer f.Close()

	fileInfo, _ := f.Stat()
	var size int64 = fileInfo.Size()

	buffer := make([]byte, size)

	// read file content to buffer
	f.Read(buffer)


	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buffer),
		Metadata: meta,
	}

	return svc.PutObject(input)


}

func PutObjects(svc *s3.S3, bucket string, key string, buffer *bytes.Buffer, meta map[string]*string) (*s3.PutObjectOutput,error){

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body: bytes.NewReader(buffer.Bytes()),
		Metadata:meta,
	}
	return svc.PutObject(input)
}

