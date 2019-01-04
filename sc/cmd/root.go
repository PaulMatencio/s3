
package cmd

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/s3/gLog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"os"
	"path/filepath"
)

// rootCmd represents the base command when called without any subcommands
var (
	cfgFile,bucket,key,metaEx 	 string
	verbose, Debug,autoCompletion		 bool
	// log          = lumber.NewConsoleLogger(lumber.INFO)

	odir,pdir  string
	loglevel int

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
	rootCmd.PersistentFlags().IntVarP(&loglevel, "loglevel", "l", 0,"Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)")
	rootCmd.Flags().StringVarP(&cfgFile,"config", "c","", "sc config file; default $HOME/.sc/config.yaml")
	rootCmd.PersistentFlags().BoolVarP(&autoCompletion,"autoCompletion", "g",false, "generate bash auto completion")

	// bind application flags to viper key for future viper.Get()
	// viper also to set default value to any key

	viper.BindPFlag("verbose",rootCmd.PersistentFlags().Lookup("verbose"))
	viper.BindPFlag("loglevel",rootCmd.PersistentFlags().Lookup("loglevel"))
	viper.BindPFlag("autoCompletion",rootCmd.PersistentFlags().Lookup("autoCompletion"))
	// read and init the config with  viper
	cobra.OnInitialize(initConfig)


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
			log.Fatalln(err)
		}

		configPath = filepath.Join("/etc/sc") // call multiple times to add many search paths
		viper.AddConfigPath(configPath)            // another path to look for the config file

        configPath = filepath.Join(home,".sc")
		viper.AddConfigPath(configPath)            // path to look for the config file

		viper.AddConfigPath(".")               // optionally look for config in the working directory
		viper.SetConfigName("config")          // name of the config file without the extension
		viper.SetConfigType("yaml")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Printf("Using config file: %s", viper.ConfigFileUsed())
	}  else {
		log.Printf("Error %v  reading config file %s",err,viper.ConfigFileUsed())
		log.Printf("AWS sdk shared config will be used if present ")
	}
	// setLogLevel()
	gLog.InitLog("",rootCmd.Name(),setLogLevel())
	gLog.Info.Printf("Logging level : %d",loglevel)

	if  autoCompletion {
		autoCompScript := filepath.Join(configPath,"bash_completion")
		rootCmd.GenBashCompletionFile(autoCompScript)
		gLog.Info.Printf("Generate bash completion script %s",autoCompScript)
	}
	if metaEx = viper.GetString("meta.extension"); metaEx == "" {
		metaEx = "md"
	}

}

func setLogLevel() (int) {

	if loglevel == 0 {
		loglevel = viper.GetInt("logging.log_level")
	}

	if verbose {
		loglevel= 4
	}
	return loglevel

}