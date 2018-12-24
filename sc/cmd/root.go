
package cmd

import (
	"fmt"
	"github.com/jcelliott/lumber"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
)

// rootCmd represents the base command when called without any subcommands
var (
	cfgFile,logLevel,bucket,key 	 string
	verbose, Debug,autoCompletion		 bool
	log          = lumber.NewConsoleLogger(lumber.INFO)

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
	rootCmd.Flags().StringVarP(&cfgFile,"config", "c","", "sc config file; default $HOME/.sc/config.yaml")
	rootCmd.Flags().BoolVarP(&autoCompletion,"autoCompletion", "",true, "generate bash auto completion")


	// bind application flags to viper key for future viper.Get()
	// viper also to set default value to any key

	viper.BindPFlag("verbose",rootCmd.PersistentFlags().Lookup("verbose"))
	viper.BindPFlag("logLevel",rootCmd.PersistentFlags().Lookup("logLevel"))
	// read and init the config with  viper
	cobra.OnInitialize(initConfig)

	// init the application logger
	// logLvl := lumber.LvlInt(viper.GetString(logLevel))

	log.Prefix("["+ rootCmd.Name()+"]")


}


// initConfig reads in config file and ENV variables if set.
func initConfig() {
	var configPath string
	if cfgFile != "" {
		// Use config file from the application flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
        configPath = filepath.Join(home,".sc")
		viper.AddConfigPath(configPath) // path to look for the config file
		viper.SetConfigName("config")  // set the config file
	}


	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file: %s", viper.ConfigFileUsed())
	}  else {
		log.Warn("Error %v  reading config file %s",err,viper.ConfigFileUsed())
		log.Info("AWS sdk shared config will be used if present ")
	}

	if autoCompletion {

		rootCmd.GenBashCompletionFile(filepath.Join(configPath,"bash_completion"))
		
	}
}
