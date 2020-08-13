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
		Use:   "listRaftSessions",
		Short: "list Raft sessions info",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			listRaft(cmd,args)
		},

	}
	raft,Host,status  string
	buckets []string
	leader *datatype.RaftLeader
	state *datatype.RaftState
	set,conf,all bool
	err error
	Port,id int
)
const http ="http://"

func init() {
	rootCmd.AddCommand(listRaftCmd)
	initaLrFlags(listRaftCmd)
}

func initaLrFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&url,"url","u","","bucketd url <htp://ip:port>")
	// cmd.Flags().StringVarP(&raft, "raft", "i", ".admin/RaftSessions.json","path to raft sessions file")
	cmd.Flags().IntVarP(&id,"id","i",-1,"raft session id")
	cmd.Flags().BoolVarP(&conf,"conf","",false,"Print Raft config information ")
	cmd.Flags().BoolVarP(&all,"all","",true," Print all member")
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

func getRaftSession(r datatype.RaftSession) {

	fmt.Printf("Id: %d\tconnected: %v\n", r.ID, r.ConnectedToLeader)
	for _, v := range r.RaftMembers {
		Host,Port = v.Host,v.Port
		if err, status = getStatus(Host, Port); err !=  nil {
			fmt.Printf("\t\tError: %v\n", err)
			return
		}
		if err, leader = getLeader(Host, Port); err != nil {
			fmt.Printf("\tError: %v\n", err)
			return
		}

		if !isInitialized(status) || isLeader(Host,leader.IP) || all {
			fmt.Printf("\tMember Id: %d\tName: %s\tHost: %s\tPort: %d\tSite: %s\tisLeader:%v\n", v.ID, v.Name, Host, Port, v.Site, isLeader)
			// fmt.Printf("\t\tStatus:\t%+v\n", status)
			if id >= 0 {
				fmt.Printf("\t\tStatus:\t%+v\n", status)
				printDetail(Host, Port, conf)
				fmt.Printf("\n")
			}
		}
	}

	// print the Buckets
	if id == -1 {
		fmt.Printf("\t\tStatus:\t%+v\n", status)
		printDetail(leader.IP,leader.Port,false)
	}
	printBucket(Host,Port)


}

func getBucket(host string,port int) (error,[]string){
	port += 100
	url := http+ host+":"+strconv.Itoa(port)
	return api.GetRaftBuckets(url)
}

func getLeader(host string,port int) (error,*datatype.RaftLeader){
	port += 100
	url := http+ host+":"+strconv.Itoa(port)
	return api.GetRaftLeader(url)
}

func getState(host string,port int) (error,*datatype.RaftState){
	port += 100
	url := http+ host+":"+strconv.Itoa(port)
	return api.GetRaftState(url)
}

func getStatus(host string,port int) (error,string){
	port += 100
	url := http+ host+":"+strconv.Itoa(port)
	return api.GetRaftStatus(url)
}

func getConfig(what string, host string,port int) (error,bool){
	port += 100
	url := http+ host+":"+strconv.Itoa(port)
	return api.GetRaftConfig(what,url)
}

func printDetail(Host string,Port int, failed bool){
	printStatus(Host,Port)
	printState(Host,Port)
	if conf {
		printConfig(Host,Port)
	}
}

func printStatus(Host string, Port int){
	if err, status = getStatus(Host, Port); err == nil {
		// fmt.Printf("\t\tLeader\t IP:%s\t%d\n",leader.IP,leader.Port)
		fmt.Printf("\t\tStatus:\t%+v\n", status)
	} else {
		fmt.Printf("\t\tError: %v\n", err)
	}
}

func printState(Host string, Port int) {
	if err, state = getState(Host, Port); err == nil {
		// fmt.Printf("\t\tLeader\t IP:%s\t%d\n",leader.IP,leader.Port)
		fmt.Printf("\t\tState:\t%+v\n", *state)
	} else {
		fmt.Printf("\t\tError: %v\n", err)
	}
}

func printBucket(Host string, Port int){

	if err, buckets = getBucket(Host, Port); err == nil {
		l := len(buckets)
		if l > 0 {
			fmt.Printf("\tBuckets:\t%s\n", buckets[0])
		}
		for i := 1; i < l; i++ {
			fmt.Printf("\t\t\t%s\n", buckets[i])
		}
	} else {
		fmt.Printf("\tError: %v\n", err)
	}
}


func printConfig(Host string, Port int) {
	if err, set = getConfig("prune", Host, Port); err == nil {
		fmt.Printf("\t\tPrune:\t%+v\n", set)
	} else {
		fmt.Printf("\t\tError: %v\n", err)
	}
	if err, set = getConfig("prune_on_leader", Host, Port); err == nil {
		fmt.Printf("\t\tPrune_on_leader:\t%+v\n", set)
	} else {
		fmt.Printf("\t\tError: %v\n", err)
	}
	if err, set = getConfig("backup", Host, Port); err == nil {
		fmt.Printf("\t\tbackup:\t%+v\n", set)
	} else {
		fmt.Printf("\t\tError: %v\n", err)
	}
}

func isInitialized( status string) (bool){
	if status == "isInitialized" {
		return true
	} else {
		return false
	}
}

func isLeader( ip1 string, ip2 string) (bool){
	if ip1==ip2 {
		return true
	} else {
		return false
	}
}