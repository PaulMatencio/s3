package cmd

import (
	"encoding/json"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"strings"
	"time"
)

const (
	ISOLayout = "2006-01-02"
	dayToAdd = 7
)
var (
	lrishort = "Command to list object replication info for given bucket"
	lriCmd = &cobra.Command{
		Use:   "lsObjsRepInfo",
		Short: lrishort,
		Long: `Default Config file $HOME/.sc/config.yaml
        Example:
        sc  lisObjsRepInfo -b pxi-prod.00 -m  300 -maxLoop 10 -p <prefix-key> -m <marker>
        sc  -c <full path of config file> -b <bucket> -m <number> -maxLoop <number> --listMaster=false`,
		// Hidden: true,
		Run: ListObjRepInfo,
	}
	done , listMaster, rBackend bool
	toDate string
	lastDate time.Time
	err error
)

func initLriFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&prefix,"prefix","p","","key prefix")
	cmd.Flags().Int64VarP(&maxKey,"maxKey","m",100,"maximum number of keys to be processed ")
	cmd.Flags().StringVarP(&marker,"marker","M","","start key processing from marker")
	// cmd.Flags().BoolVarP(&loop,"loop","L",false,"loop until all keys are processed")
	cmd.Flags().IntVarP(&maxLoop,"maxLoop","",1,"maximum number of loop, 0 means no upper limit")
	cmd.Flags().BoolVarP(&listMaster,"listMaster","",true,"list the current version only")
	cmd.Flags().StringVarP(&delimiter,"delimiter","d","","key delimiter")
	cmd.Flags().BoolVarP(&done,"completed","",false,"print objects with COMPLETED/REPLICA status,by default only PENDING or FAILED are printed out ")
	cmd.Flags().BoolVarP(&rBackend,"rback","",false,"print report of both S3 metadata and backend replication info (sproxyd)")
	cmd.Flags().StringVarP(&toDate,"toDate","","","List replication info up to this given date <yyyy-mm-dd>")
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

    if len(toDate) > 0 {
		if lastDate, err = time.Parse(ISOLayout, toDate); err != nil {
			gLog.Error.Printf("Wrong date format %s", toDate)
			return
		}
	} else {
		lastDate = time.Now().AddDate(0,0,dayToAdd)
	}
	gLog.Info.Printf("Counting objects from last modified date %v",lastDate)

	var (
		nextMarker string
		repStatus, backendStatus *string
		p,r,f,o,t,cl int64
		cp,cf,cc, skip int64
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
		report Report

		s3Meta = datatype.S3Metadata{}
		N = 1 /* number of loop */
	)
	gLog.Info.Printf("%v",req)

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
					value := c.Value
					repStatus = &value.ReplicationInfo.Status
					if  value.LastModified.Before(lastDate) {

						// lastModified := &value.LastModified
						t++
						switch *repStatus {
						case "PENDING":
							{
								p++
								//gLog.Warning.Printf("Key: %s - Last Modified: %v - size: %d - replication status: %v", c.Key, lastModified, c.Value.ContentLength,*repStatus)
								value.PrintRepInfo(c.Key,gLog.Warning)
							}
						case "FAILED":
							{
								f++
								//gLog.Warning.Printf("Key: %s - Last Modified: %v - size: %d - replication status: %v", c.Key,lastModified,c.Value.ContentLength,*repStatus)
								value.PrintRepInfo(c.Key,gLog.Warning)
							}
						case "COMPLETED":
							{
								r++
								backendStatus = &c.Value.ReplicationInfo.Backends[0].Status
								switch *backendStatus {
								case "PENDING":
									{
										cp++
									}
								case "COMPLETED":
									{
										cc++
										cl += int64(c.Value.ContentLength)
									}
								case "FAILED":
									{
										cf++
									}
								}
								if done {
									value.PrintRepInfo(c.Key,gLog.Info)
									// gLog.Info.Printf("Key: %s - Last Modified: %v - size: %d - replication status: %v", c.Key,lastModified,c.Value.ContentLength,*repStatus)
								}
							}
						case "REPLICA":
							{
								r++
								cl += int64(c.Value.ContentLength)
								if done {
									value.PrintRepInfo(c.Key,gLog.Info)
									// gLog.Info.Printf("Key: %s - Last Modified: %v - size: %d - replication status: %v", c.Key,lastModified,c.Value.ContentLength,*repStatus)
								}
							}
						default:
							o++
						}
					} else {
						skip++  /* number of objecys skipped after last modified date > toDate */
					}
					value.PrintRepInfo(c.Key,gLog.Trace)
					// gLog.Trace.Printf("Key: %s - Last Modified:%v - size: %d - replication status: %v", c.Key,lastModified,c.Value.ContentLength, *repStatus)
				}
				N++
				if l > 0 {
					nextMarker = s3Meta.Contents[l-1].Key
					gLog.Info.Printf("Next marker %s Istruncated %v", nextMarker,s3Meta.IsTruncated)
				}
			} else {
				gLog.Info.Println(err)
			}
			size = float64(cl)/(1024.0*1024.0*1024.0)  //  expressed in GB
			report.Total= t
			report.ReportMeta.Completed=r
			report.ReportMeta.Pending =p
			report.ReportMeta.Failed = f
			report.ReportMeta.Other= o
			report.ReportBackend.Completed= cc
			report.ReportBackend.Pending= cp
			report.ReportBackend.Pending= cf

			report  = Report {
				Total: t,
				Size: size,
				ReportMeta: ReportNumber {
					Completed: r,
					Pending: p,
					Failed: f,
					Other: o,
			},
				ReportBackend: ReportNumber{
					Completed: cc,
					Pending: cp,
					Failed: cf,
				},
				Skipped : skip,
			}

			if !s3Meta.IsTruncated {
				report.Elapsed= time.Since(begin)
				//gLog.Warning.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d / size(GB):%.2f - cc:%d - cp:%d - cf:%d - other:%d", time.Since(begin),t, p,f,r,size,cc,cp,cf,o)
				report.printReport(gLog.Warning,rBackend)
				return
			} else {
				// marker = nextMarker, nextMarker could contain Keyu00 if  bucket versioning is on
				Marker := strings.Split(nextMarker,"u00")
				req.Marker = Marker[0]
				// gLog.Warning.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d - cc:%d - cp:%d - cf:%d - other:%d", time.Since(start),t, p,f,r,cc,cp,cf,o)
				report.Elapsed= time.Since(start)
				// gLog.Warning.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d / size(GB):%.2f - cc:%d - cp:%d - cf:%d - other:%d", time.Since(start),t, p,f,r,size,cc,cp,cf,o)
				report.printReport(gLog.Warning,rBackend)
			}
			if maxLoop != 0 && N > maxLoop {
				// gLog.Warning.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d - cc:%d - cp:%d - cf:%d - other:%d", time.Since(begin),t, p,f,r,cc,cp,cf,o)
				// gLog.Warning.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d / size(GB):%.2f - cc:%d - cp:%d - cf:%d - other:%d", time.Since(begin),t, p,f,r,size,cc,cp,cf,o)
				report.Elapsed= time.Since(begin)
				report.printReport(gLog.Warning,rBackend)
				return
			}
		}
	}

}

type Report struct {
	Elapsed time.Duration
	Total    int64
	Size      float64
	ReportMeta	 ReportNumber
	ReportBackend  ReportNumber
	Skipped  int64
}

type ReportNumber struct {
	Pending  int64
	Failed   int64
	Completed int64
	Other     int64
}

func (r Report) printReport (log *log.Logger,back bool) {
	if back {
		log.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d / size(GB):%.2f - cc:%d - cp:%d - cf:%d - other:%d - skipped:%d",
			r.Elapsed, r.Total, r.ReportMeta.Pending,
			r.ReportMeta.Failed, r.ReportMeta.Completed,r.Size,
			r.ReportBackend.Completed,r.ReportBackend.Failed,r.ReportMeta.Other)
	} else {
		log.Printf("Total elapsed time: %v - total:%d - pending:%d - failed:%d - completed:%d / size(GB):%.2f - other:%d - skipped:%d",
			r.Elapsed, r.Total, r.ReportMeta.Pending,
			r.ReportMeta.Failed, r.ReportMeta.Completed,r.Size,
			r.ReportMeta.Other,r.Skipped)
	}

}
