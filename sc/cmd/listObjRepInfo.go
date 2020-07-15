package cmd
import (
	"encoding/json"
	"github.com/s3/gLog"
	"github.com/spf13/viper"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"strings"
	"time"
)


var (
	lrishort = "Command to list object replication info for given bucket"
	lriCmd = &cobra.Command{
		Use:   "lsObjsRepInfo",
		Short: lrishort,
		Long: ``,
		// Hidden: true,
		Run: ListObjRepInfo,
	}
	done bool
	listMaster bool

)

func initLriFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker,"marker","M","","start processing from this key")
	// cmd.Flags().BoolVarP(&loop,"loop","L",false,"loop until all keys are processed")
	cmd.Flags().IntVarP(&maxLoop,"maxLoop","",1,"maximum number of loop, 0 means no upper limit")
	cmd.Flags().BoolVarP(&listMaster,"listMaster","",true,"List last version only")
	cmd.Flags().StringVarP(&delimiter,"delimiter","d","","key delimiter")
	cmd.Flags().BoolVarP(&done,"completed","",false,"print objects with COMPLETED/REPLICA status,by default only PENDING or FAILED are printed out ")

}

func init() {
	RootCmd.AddCommand(lriCmd)
	RootCmd.MarkFlagRequired("bucket")
	initLriFlags(lriCmd)
}



func ListObjRepInfo(cmd *cobra.Command,args []string) {

	var url string
	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		return
	}
	if url = utils.GetLevelDBUrl(*viper.GetViper()); len(url) == 0 {
		gLog.Warning.Printf("levelDB url is missing")
		return
	}
	var (
		nextMarker string
		repStatus, backendStatus *string
		p,r,f,o,t,cl int64
		cp,cf,cc int64
		size float64
		req        = datatype.ListObjLdbRequest{
			Url:       url,
			Bucket:    bucket,
			Prefix:    prefix,
			MaxKey:    maxKey,
			Marker:    marker,
			ListMaster: listMaster,
			Delimiter: delimiter,
		}
		s3Meta = datatype.S3Metadata{}
		N = 1
	)
	begin := time.Now()
	for {
		start := time.Now()
		if result, err := api.ListObjectLdb(req); err != nil {
			if err == nil {
				gLog.Error.Println(err)
			} else {
				gLog.Info.Println("Result is empty")
			}
		} else {

			if err = json.Unmarshal([]byte(result.Contents), &s3Meta); err == nil {
				//gLog.Info.Println("Key:",s3Meta.Contents[0].Key,s3Meta.Contents[0].Value.XAmzMetaUsermd)
				//num := len(s3Meta.Contentss3Meta.Contents)
				l := len(s3Meta.Contents)
				for _, c := range s3Meta.Contents {
					//m := &s3Meta.Contents[i].Value.XAmzMetaUsermd
					repStatus = &c.Value.ReplicationInfo.Status

					lastModified := &c.Value.LastModified
					t++
					switch *repStatus {
						case "PENDING" :{
							p++
							gLog.Warning.Printf("Key: %s - Last Modified: %v - size: %d - replication status: %v", c.Key, lastModified, c.Value.ContentLength,*repStatus)
						}
						case "FAILED" : {
							f++
							gLog.Warning.Printf("Key: %s - Last Modified: %v - size: %d - replication status: %v", c.Key,lastModified,c.Value.ContentLength,*repStatus)
						}
						case "COMPLETED":{
							r++
							backendStatus = &c.Value.ReplicationInfo.Backends[0].Status
							switch *backendStatus {
								case "PENDING" :{
									cp++
								}
								case "COMPLETED": {
									cc++
									cl += int64(c.Value.ContentLength)
								}
								case "FAILED" : {
									cf++
								}
							}
							if done {
								gLog.Info.Printf("Key: %s - Last Modified: %v - size: %d - replication status: %v", c.Key,lastModified,c.Value.ContentLength,*repStatus)
							}
						}
						case "REPLICA" : {
							r++
							cl += int64(c.Value.ContentLength)
							if done {
								gLog.Info.Printf("Key: %s - Last Modified: %v - size: %d - replication status: %v", c.Key,lastModified,c.Value.ContentLength,*repStatus)
							}
						}
						default: o++
					}
					gLog.Trace.Printf("Key: %s - Last Modified:%v - size: %d - replication status: %v", c.Key,lastModified,c.Value.ContentLength, *repStatus)
				}
				N++
				if l > 0 {
					nextMarker = s3Meta.Contents[l-1].Key
					gLog.Info.Printf("Next marker %s Istruncated %v", nextMarker,s3Meta.IsTruncated)
				}
			} else {
				gLog.Info.Println(err)
			}
			size = float64(cl)/(1024.0*1024.0*1024.0)
			if !s3Meta.IsTruncated {

				gLog.Warning.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d / size(MB):%.2f - cc:%d - cp:%d - cf:%d - other:%d", time.Since(begin),t, p,f,r,size,cc,cp,cf,o)
				return
			} else {
				// marker = nextMarker, nextMarker could contain Keyu00 if  bucket versioning is on
				Marker := strings.Split(nextMarker,"u00")
				req.Marker = Marker[0]
				// gLog.Warning.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d - cc:%d - cp:%d - cf:%d - other:%d", time.Since(start),t, p,f,r,cc,cp,cf,o)
				gLog.Warning.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d / size(GB):%.2f - cc:%d - cp:%d - cf:%d - other:%d", time.Since(start),t, p,f,r,size,cc,cp,cf,o)
			}
			if maxLoop != 0 && N > maxLoop {
				// gLog.Warning.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d - cc:%d - cp:%d - cf:%d - other:%d", time.Since(begin),t, p,f,r,cc,cp,cf,o)
				gLog.Warning.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d / size(MB):%.2f - cc:%d - cp:%d - cf:%d - other:%d", time.Since(begin),t, p,f,r,size,cc,cp,cf,o)
				return
			}
		}
	}

}
