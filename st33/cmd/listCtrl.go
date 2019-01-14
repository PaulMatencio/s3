
package cmd

import (
	"github.com/s3/gLog"
	"github.com/s3/st33/utils"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
)

// readConvalCmd represents the readConval command
var readConvalCmd = &cobra.Command{
	Use:   "lsCtrl",
	Short: "Command to list a control file",
	Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		readConval(cmd,args)
	},
}

func initLvFlags(cmd *cobra.Command) {

	// cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&ifile,"ifile","i","","ST33 Conval input file")
	// cmd.Flags().StringVarP(&odir,"ofile","o","","output file")
}


func init() {

	RootCmd.AddCommand(readConvalCmd)
	initLvFlags(readConvalCmd)


}

func readConval(cmd *cobra.Command, args []string) {
	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}

	if c,err:=  st33.BuildConvalArray(ifile); err == nil {
		for k, v := range *c {
			gLog.Info.Println(k, utils.Reverse(v.PxiId),v.PxiId, v.Pages)
		}
	} else {
		gLog.Error.Println(err)
	}

}
