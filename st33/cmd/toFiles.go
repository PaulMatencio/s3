

package cmd

import (
	"github.com/s3/st33/utils"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"path/filepath"
)

// st33ToFilesCmd represents the st33ToFiles command
var (

	toFilesCmd = &cobra.Command{
		Use:   "toFiles",
		Short: "Command to extract an ST33 file containing Tiff images and Blobs to Files",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			toFilesFunc(cmd,args)
		},
	}
	)

func initTfFlags(cmd *cobra.Command) {

	// cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&ifile,"ifile","i","","ST33 input file")
	cmd.Flags().StringVarP(&odir,"odir","O","","output directory")
}

func init() {
	RootCmd.AddCommand(toFilesCmd)
	initTfFlags(toFilesCmd)

}

func toFilesFunc(cmd *cobra.Command, args []string) {

	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}

	if len(odir) >0 {
		pdir = filepath.Join(utils.GetHomeDir(),odir)
		utils.MakeDir(pdir)
	} else {
		gLog.Info.Printf("%s",missingInputFolder)
		return
	}

	gLog.Info.Printf("Processing input file %s",ifile)
	st33.ToFiles(ifile,odir,test)

}

