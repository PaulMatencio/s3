package s3Client

import (
	"github.com/minio/minio-go"
	"log"
	"time"
)

func PrintOk(object string, size int64, start time.Time) {
	duration := time.Since(start)
	speed :=  1000* float64(size ) / float64(duration)
	log.Printf("Uploading %s successfully. Size: %d. Duration:%s. Speed:%.4f  MBps",object,size,duration,speed)
}
func PrintNotOk(object string, err error) {
	log.Printf("Error uploading  %s %v",object,err)
}

func SetMetadata(opts minio.PutObjectOptions){
	usermd := map[string]string{
		"Owner":  "Pmatencio",
		"Street":   "Regentesselaan",
		"Number": "14",
		"Zipcode": "2562 CS",
		"City": "The Hague",
		"Country": "NL",
	}
	opts.UserMetadata= usermd
}