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
	cfgFile,bucket,metaEx 	 string
	verbose, Debug,autoCompletion,test 	 bool
	// log          = lumber.NewConsoleLogger(lumber.INFO)

	odir,pdir,ifile  string
	loglevel,profiling int

	missingBucket = "Missing bucket - please provide the bucket name"
	missingInputFile ="Missing date input file - please provide the input file path (absolute or relative to current directory"
	missingOutputFolder ="Missing output directory - please provide the output directory path( absolute or relative to current directory)"
	missingInputFolder ="Missing input directory - please provide the input directory path( absolute or relative to current directory)"

	RootCmd = &cobra.Command {
		Use:   "pxi",
		Short: "pxi migration tools",
		Long: ``,
		TraverseChildren: true,
	})


// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}

func init() {

	// persistent flags

	RootCmd.PersistentFlags().BoolVarP(&verbose, "verbose","v", false, "verbose output")
	RootCmd.PersistentFlags().IntVarP(&loglevel, "loglevel", "l", 0,"Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)")
	RootCmd.Flags().StringVarP(&cfgFile,"config", "c","", "sc config file; default $HOME/.sc/config.yaml")
	RootCmd.PersistentFlags().BoolVarP(&autoCompletion,"autoCompletion", "C",false, "generate bash auto completion")
	RootCmd.PersistentFlags().IntVarP(&profiling,"profiling", "P",0, "display memory usage every P seconds")
	RootCmd.PersistentFlags().BoolVarP(&test,"test","t",false,"test mode")
	// bind application flags to viper key for future viper.Get()
	// viper also to set default value to any key

	viper.BindPFlag("verbose",RootCmd.PersistentFlags().Lookup("verbose"))
	viper.BindPFlag("loglevel",RootCmd.PersistentFlags().Lookup("loglevel"))
	viper.BindPFlag("autoCompletion",RootCmd.PersistentFlags().Lookup("autoCompletion"))
	viper.BindPFlag("profiling",RootCmd.PersistentFlags().Lookup("profiling"))
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
	gLog.InitLog("",RootCmd.Name(),setLogLevel())
	gLog.Info.Printf("Logging level : %d",loglevel)

	if  autoCompletion {
		autoCompScript := filepath.Join(configPath,"pxi_bash_completion")
		RootCmd.GenBashCompletionFile(autoCompScript)
		gLog.Info.Printf("Generate bash completion script %s to",autoCompScript)
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