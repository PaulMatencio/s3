// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
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
	"github.com/jcelliott/lumber"
	"os"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// rootCmd represents the base command when called without any subcommands
var (
	cfgFile,logLevel,bucket,key 	 string
	verbose, Debug		 bool
	missingBucket = "Missing bucket - please provide the bucket name"
	missingKey = "Missing key - please provide the key of the object"
	missingInputFile ="Missing date input file - please provide the input file path (absolute or relative to current directory"
	missingMetaFile ="Missing meta input file - please provide the meta file path (absolute or relative to current directory"
	missingOutputFolder ="Missing output directory - please provide the output directory path( absolute or relative to current directory"

	rootCmd = &cobra.Command {
	Use:   "sc",
	Short: "Scality S3 frontend commands",
	Long: ``,
	TraverseChildren: true,
})

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {

	// persistent flags

	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose","v", false, "verbose output")
	rootCmd.PersistentFlags().StringVarP(&logLevel, "logLevel", "l", "INFO","Output level of logs (TRACE, DEBUG, INFO, WARN, ERROR, FATAL)")

	viper.BindPFlag("verbose",rootCmd.PersistentFlags().Lookup("verbose"))
	viper.BindPFlag("logLevel",rootCmd.PersistentFlags().Lookup("logLevel"))

	// fmt.Println(verbose,logLevel)

	// local flags
	rootCmd.Flags().StringVarP(&cfgFile,"config", "c","", "config file (default is $HOME/.sc.yaml)")

	cobra.OnInitialize(initConfig)

	// init the logger
	logLvl := lumber.LvlInt(viper.GetString(logLevel))
	lumber.Prefix("["+ rootCmd.Name()+"]")
	lumber.Level(logLvl)

}


// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".sc" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".sc")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
