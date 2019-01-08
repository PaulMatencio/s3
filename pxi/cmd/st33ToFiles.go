

package cmd

import (
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"github.com/s3/pxi/utils"
	"path/filepath"
)

// st33ToFilesCmd represents the st33ToFiles command
var (

	st33TFCmd = &cobra.Command{
		Use:   "st33ToFiles",
		Short: "Command to extract ST33 documents and output to files",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			st33ToFiles(cmd,args)
		},
	}
	)

func initTfFlags(cmd *cobra.Command) {

	// cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&ifile,"ifile","i","","ST33 input file")
	cmd.Flags().StringVarP(&odir,"odir","O","","output directory")
}

func init() {
	RootCmd.AddCommand(st33TFCmd)
	initTfFlags(st33TFCmd)

}

func st33ToFiles(cmd *cobra.Command, args []string) {

	if len(odir) >0 {
		pdir = filepath.Join(utils.GetHomeDir(),odir)
		utils.MakeDir(pdir)
	} else {
		gLog.Info.Printf("%s",missingInputFolder)
		return
	}
	pxi.ST33ToFiles(ifile,odir,test)

}

