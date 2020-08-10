// Copyright © 2020 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"path/filepath"
	"strconv"
)

var (
	listRaftCmd = &cobra.Command{
		Use:   "listRaft",
		Short: "list Raft sessions info",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			listRaft(cmd,args)
		},

	}
	raft,Host  string
	buckets []string
	leader *datatype.RaftLeader
	err error
	Port int
)
const http ="http://"

func init() {
	rootCmd.AddCommand(listRaftCmd)
	initaLrFlags(listRaftCmd)
}

func initaLrFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&url,"url","u","","bucketd url <ip:port>")
	cmd.Flags().StringVarP(&raft, "raft", "i", ".admin/RaftSessions.json","path to raft sessions file")
}


func listRaft(cmd *cobra.Command,args []string) {
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
		 for _,r:= range *raftSess {
			fmt.Printf("Id: %d\tconnected: %v\n",r.ID,r.ConnectedToLeader)
			for _,v := range r.RaftMembers {
				fmt.Printf("\tID: %d\tName: %s\tHost: %s\tPort: %d\tSite: %s\n", v.ID, v.Name, v.Host, v.Port, v.Site)
				Host=v.Host
				Port=v.Port
			}
			if err,buckets = getBucket(Host,Port); err ==nil {
				fmt.Printf("\t\tBuckets: %v\n",buckets)
			} else {
				fmt.Printf("\t\tError: %v\n",err)
			}
			if err,leader = getLeader(Host,Port); err ==nil {
				fmt.Printf("\t\tLeader\t IP:%d\t%s\n",&leader.IP,&leader.Port)
			} else {
				fmt.Printf("\t\tError: %v\n",err)
			}
		}
	} else {
	 	gLog.Error.Printf("%v",err)
	}
}

/*  used for testing */
func listRaft1(cmd *cobra.Command,args []string) {
	if home, err := homedir.Dir(); err == nil {
		filePath := filepath.Join(home, raft)
		viper.Set("raft", filePath)
		c := datatype.RaftSessions{}
		if err, raftSess := c.GetRaftSessions(filePath); err == nil {
			for _, r := range *raftSess {
				fmt.Printf("Id: %d\tconnected: %v\n", r.ID, r.ConnectedToLeader)
				for _, v := range r.RaftMembers {
					fmt.Printf("\tId: %d\tName: %s\tHost: %s\tPort: %d\tSite: %s\n", v.ID, v.Name, v.Host, v.Port, v.Site)
					Host=v.Host
					Port=v.Port
				}
				if err,buckets = getBucket(Host,Port); err ==nil {
					fmt.Printf("\t\tBuckets: %v\n",buckets)
				}
			}
		} else {
			gLog.Error.Printf("%v", err)
		}
	} else {
		gLog.Error.Printf("%v", err)
	}
}

func getBucket(host string,port int) (error,[]string){
	// var buckets []string
	// admin port
	port += 100
	url := http+ host+":"+strconv.Itoa(port)
	return api.ListRaftBuckets(url)
}

func getLeader(host string,port int) (error,*datatype.RaftLeader){
	port += 100
	url := http+ host+":"+strconv.Itoa(port)
	return api.ListRaftLeader(url)
}