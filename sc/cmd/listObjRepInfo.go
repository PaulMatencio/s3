package cmd
import (
	"encoding/json"
	"github.com/s3/gLog"
	"github.com/spf13/viper"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
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

)

func initLriFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maxmimum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker,"marker","M","","start processing from this key")
	cmd.Flags().BoolVarP(&loop,"loop","L",false,"loop until all keys are processed")
	cmd.Flags().IntVarP(&maxLoop,"maxLoop","",1000,"maximum number of loop")

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
		p,r,f,o,t int64
		req        = datatype.ListObjLdbRequest{
			Url:       url,
			Bucket:    bucket,
			Prefix:    prefix,
			MaxKey:    maxKey,
			Marker:    marker,
			Delimiter: delimiter,
		}
		s3Meta = datatype.S3Metadata{}
		N = 0
	)
	for {
		start := time.Now()
		if result, err := api.ListObjectLdb(req); err != nil {
			if err != nil {
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
					repStatus := &c.Value.ReplicationInfo.Status
					lastModified := &c.Value.LastModified
					t++
					switch *repStatus {
						case "PENDING" :{
							p++
							gLog.Warning.Printf("Key: %s - Last Modified: %v  - replication status: %v ", c.Key, lastModified, *repStatus)
						}
						case "FAILED" : {
							f++
							gLog.Warning.Printf("Key: %s - Last Modified: %v  - replication status: %v ", c.Key,lastModified,*repStatus)
						}
						case "COMPLETED": r++
						default: o++
					}
					gLog.Trace.Printf("Key: %s - Last Modified:%v  - replication status: %v ", c.Key,lastModified, *repStatus)
				}
				N++
				if l > 0 {
					nextMarker = s3Meta.Contents[l-1].Key
					gLog.Info.Printf("Next marker %s Istruncated %v", nextMarker,s3Meta.IsTruncated)
				}
			} else {
				gLog.Info.Println(err)
			}

			if !s3Meta.IsTruncated {
				return
			} else {
				marker = nextMarker
				gLog.Warning.Printf("Elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d - other:%d - nextMarker:%s", time.Since(start),t, p,f,r,o,marker)
			}
			if N > maxLoop {
				return
			}
		}
	}
}
