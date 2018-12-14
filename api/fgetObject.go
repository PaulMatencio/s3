package s3Client

import (
	"errors"
	"github.com/minio/minio-go"
	"github.com/moses/user/files/lib"
	"github.com/moses/user/goLog"
	"io"
	"os"
	"fmt"
	"path/filepath"
)

type S3FGetRequest struct {
	S3GetRequest
	FilePath 	string
	OverWrite   bool
}

func (r *S3FGetRequest) S3BuildFGetRequest(login *S3Login, bucket string, key string, filename string, options *minio.GetObjectOptions,overwrite bool){
	r.MinioC,r.Bucket,r.Key,r.FilePath,r.Opts,r.OverWrite,r.Trace	= login.MinioC,bucket,key,filename,options,overwrite,false
	if TRACE {
		r.Trace	= true
	}
}

//  Use minio FGetObject()  function to download an object

func (r *S3FGetRequest) FgetObject() (error) {
	if r.Trace {
		r.MinioC.TraceOn(os.Stdout)
	}
	return r.MinioC.FGetObject(r.Bucket,r.Key,r.FilePath,*r.Opts)
}

//  alternative method to download an object
func (r *S3FGetRequest) FGetObject()(error) {

	var (
		object *minio.Object
		err error
	)
	if r.Trace {
		r.MinioC.TraceOn(os.Stdout)
	}

	if object,err = r.MinioC.GetObject(r.Bucket,r.Key,*r.Opts); err != nil {
		return err
	}
	defer object.Close()

	return writeToFile(object,r.FilePath,r.OverWrite)

}


// this function is only used in this
func writeToFile ( object io.Reader, filePath string, overwrite bool)  (error) {

	var err 	error

	// Verify if destination already exists and it is not a directory
	f, err := os.Stat(filePath)
	if err == nil {
		if f.IsDir() {
			return errors.New("fileName is a directory.")
		} else {
			if !overwrite {
				return errors.New(fmt.Sprintf("destination file %s exists. Use -O to overwrite.", filePath))
			}
		}
	} else {

		if base, _ := filepath.Split(filePath); base != "" {
			// Create top level  directories if they do not exist
			if _, err = os.Stat(base); err != nil {
				os.MkdirAll(base, DIRMODE)
			}
		}
	}

	//
	buf,err := Stream(object)

	goLog.Info.Println(buf.Len(),err)
	if err == nil {
		files.WriteFile(filePath, buf.Bytes(), FILEMODE)
	}

	return err
}