package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"path/filepath"
	"strings"
)

var (
	listConfigCmd = &cobra.Command{
		Use:   "listConfig",
		Short: "list bucket info",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			listConfig(cmd,args)
		},
	}
	topoLogy string
)

func init() {
	rootCmd.AddCommand(listConfigCmd)
	initaLcFlags(listConfigCmd)
}

func initaLcFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&topoLogy, "topoLogy", "i", ".admin/topology.json","path to the S3 metadata configuration file")
}


func listConfig(cmd *cobra.Command,args []string) {
	var (
		filePath string
		c  datatype.Clusters
		cluster,meta  string
	)
	if home, err := homedir.Dir(); err == nil {
		filePath = filepath.Join(home,topoLogy)
		viper.Set("topology",filePath)
		if err,c := c.GetClusters(filePath); err == nil {

			for _,r := range c.Topology {
				a:= r.Repds
				for _,v := range a {
					cluster = strings.Split(v.Name,"-")[1]
					meta = strings.Split(v.Name,"-")[0]
					fmt.Printf("Repd: %d\tCluster: %s\t Meta:%s\tHost:%s\tPort:%d\tSite:%s\n", r.Num, cluster,meta,v.Host,v.Port,v.Site)
				}
				w := r.Wsbs
				for _,v := range w {
					cluster = strings.Split(v.Name,"-")[1]
					meta = strings.Split(v.Name,"-")[0]
					fmt.Printf("Wsb: %d\tCluster: %s\t Meta:%s\tHost:%s\tPort:%d\tSite:%s\n", r.Num, cluster,meta,v.Host,v.Port,v.Site)
				}
				fmt.Printf("\n")

			}
		} else {
			gLog.Error.Printf("%v",err)
		}
	} else {
		gLog.Error.Printf("Error opening topology file: %v - filepath",err)
	}
}

func PrettyPrint(i interface{}) (string) {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}
