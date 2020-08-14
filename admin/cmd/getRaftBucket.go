
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
			fmt.Printf("Leader\n")
			printMember(rs.Leader)
			fmt.Printf("Connected\n")
			for _,m := range rs.Connected {
				printMember(m)
			}
			fmt.Printf("Disconnected\n")
			for _,m := range rs.Disconnected {
				printMember(m)
			}
		}
	} else {
		fmt.Printf("%v\n",err)
	}
}

func printBucket(rb datatype.RaftBucket) {
	fmt.Printf("Bucket:\t%s", bucket)
	fmt.Printf("\tSession ID:%d",rb.RaftSessionID)
	fmt.Printf("\tLeader IP:%s\tPort:%d\n",rb.Leader.IP,rb.Leader.Port)
}


func printMember(v datatype.RaftMembers){
	fmt.Printf("\tId: %d\tName: %s\tHost: %s\tPort: %d\tSite: %s\tAdmin port: %d\n", v.ID, v.Name, v.Host, v.Port, v.Site,v.AdminPort)
}