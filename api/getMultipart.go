package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/s3/datatype"
	"os"
)

func GetMultipart(req datatype.GetMultipartObjRequest ) (int64,error) {

	if fd, err := os.OpenFile(req.OutputFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666); err == nil {
		downLoader := s3manager.NewDownloaderWithClient(req.Service, func(d *s3manager.Downloader) {
			d.PartSize = req.PartSize
			d.Concurrency = req.Concurrency
		})
		input := s3.GetObjectInput{
			Key:        aws.String(req.Key),
			Bucket:     aws.String(req.Bucket),
			PartNumber: aws.Int64(req.PartNumber),
		}
		return downLoader.Download(fd, &input)

	}  else {
		return 0, err
	}
}
