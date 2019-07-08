

package cmd

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/s3/gLog"
	"github.com/s3/st33/db"
	"github.com/s3/st33/utils"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"path/filepath"
	"time"
)

// st33ToFilesCmd represents the st33ToFiles command
var (
	blob,DB string
	database *badger.DB

	toFilesCmd = &cobra.Command{
		Use:   "toFiles",
		Short: "Command to extract an ST33 and write to local files",
		Long: `Command to extract an ST33 file containing Tiff images and Blobs and write to  files`,
		Hidden: true,
		Run: func(cmd *cobra.Command, args []string) {
			toFilesFunc(cmd,args)
		},
	}
	toFiles2Cmd = &cobra.Command{
		Use:   "toFiles2",
		Short: "Command to extract an ST33 and write to local files",
		Long: `Command to extract an ST33 file containing Tiff images and Blobs and write to  files`,
		Run: func(cmd *cobra.Command, args []string) {
			toFilesFuncV2(cmd,args)
		},
	}

	)

func initTfFlags(cmd *cobra.Command) {

	// cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket")
	cmd.Flags().StringVarP(&ifile,"ifile","i","","ST33 input file containing st33 formated data")
	cmd.Flags().StringVarP(&odir,"odir","O","","output directory of the extraction")
	cmd.Flags().StringVarP(&blob,"blob","B","","Blob output folder relative to the output directory")
	cmd.Flags().StringVarP(&DB,"DB","D","","name of the badger database")
}

func init() {
	RootCmd.AddCommand(toFilesCmd)
	initTfFlags(toFilesCmd)
	RootCmd.AddCommand(toFiles2Cmd)
	initTfFlags(toFiles2Cmd)


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
		gLog.Info.Printf("%s",missingOutputFolder)
		return
	}


	bdir = pdir
	if len(blob) > 0 {
		bdir  = filepath.Join(pdir,blob)
		utils.MakeDir(bdir)
	}

	//  open badger database
	if len(DB) > 0 {
		database,_ := db.OpenBadgerDB(DB)
		kv := map[string]string{
			"st33" : ifile,
			"start" : fmt.Sprintf("%v",time.Now()),
		}
		db.UpdateBadgerDB(database,kv)

	}

	gLog.Info.Printf("Processing input file %s",ifile)
	if numpages,numdocs,numerrors,err  :=  st33.ToFiles(ifile,odir,bdir, test); err ==nil {
		gLog.Info.Printf("%d documents/ %d pages were processed. Number errors %d", numdocs, numpages, numerrors)
	} else {
		gLog.Error.Printf("%v",err)
	}

}

func toFilesFuncV2(cmd *cobra.Command, args []string) {

	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}

	if len(odir) >0 {
		pdir = filepath.Join(utils.GetHomeDir(),odir)
		utils.MakeDir(pdir)
	} else {
		gLog.Info.Printf("%s",missingOutputFolder)
		return
	}



	bdir = pdir
	if len(blob) > 0 {
		bdir  = filepath.Join(pdir,blob)
		utils.MakeDir(bdir)
	}

	//  open badger database
	if len(DB) > 0 {
		database,_ := db.OpenBadgerDB(DB)
		kv := map[string]string{
			"st33" : ifile,
			"start" : fmt.Sprintf("%v",time.Now()),
		}
		db.UpdateBadgerDB(database,kv)

	}

	gLog.Info.Printf("Processing input file %s",ifile)
	if numpages,numdocs,numerrors,err:=  st33.ToFilesV2(ifile,odir,bdir, test); err != nil {
		gLog.Error.Printf("%v",err)
	} else {
		gLog.Info.Printf("%d documents/ %d pages were processed. Number errors %d", numdocs, numpages, numerrors)
	}

}
