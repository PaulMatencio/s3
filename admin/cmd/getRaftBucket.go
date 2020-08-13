
package cmd

import (
	"github.com/s3/api"
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
	initaLrFlags(getRaftBucketCmd)
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
				gLog.Warning.Printf("The url of Bucketd server is missing")
				return
			}
		}
	}
	gLog.Info.Printf("Url: %s",url)
	if err,raftSess := api.ListRaftSessions(url); err == nil {
		if id >= 0 && id <= len(*raftSess) {
			getRaftSession((*raftSess)[id])
		} else {
			for _, r := range *raftSess {
				getRaftSession(r)
			}
		}
	} else {
		gLog.Error.Printf("%v",err)
	}
}