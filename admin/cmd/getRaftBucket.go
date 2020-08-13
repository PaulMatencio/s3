
package cmd

import (
	"fmt"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	getRaftBucketCmd = &cobra.Command{
		Use:   "getRaftBucket",
		Short: "Get Raft Bucket info",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			getRaftBucket(cmd,args)
		},
	}

)


func init() {
	rootCmd.AddCommand(getRaftBucketCmd)
	initaGrFlags(getRaftBucketCmd)
}

func initaGrFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&url,"url","u","","bucketd url <htp://ip:port>")
	// cmd.Flags().StringVarP(&raft, "raft", "i", ".admin/RaftSessions.json","path to raft sessions file")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","bucket name")
}


func getRaftBucket(cmd *cobra.Command, args []string){

	if len(url) == 0 {
		if url = utils.GetBucketdUrl(*viper.GetViper()); len(url) == 0 {
			if url = utils.GetLevelDBUrl(*viper.GetViper()); len(url) == 0 {
				gLog.Warning.Printf("The url of metadata server is missing")
				return
			}
		}
	}
	gLog.Info.Printf("Url: %s",url)

	if err,rb := api.GetRaftBucket(url,bucket); err == nil {
		printBucket(*rb)
		// Get Raft session
		if err,rs := api.GetRaftSession(url,rb.RaftSessionID); err == nil {
			printMembers(*rs)
		}
	} else {
		fmt.Printf("%v",err)
	}
}

func printBucket(rb datatype.RaftBucket) {
	fmt.Printf("Bucket:\t%s", bucket)
	fmt.Printf("\tSession ID:%s",rb.RaftSessionID)
	fmt.Printf("\tLeader:%s\n",rb.Leader)
}