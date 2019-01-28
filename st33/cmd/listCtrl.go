
package cmd

import (
	"bufio"
	"fmt"
	"github.com/s3/gLog"
	"github.com/s3/st33/utils"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
)

// readConvalCmd represents the readConval command
var (
	ofile string
	lsCtrlCmd = &cobra.Command{
	Use:   "lsCtrl",
	Short: "Command to list a control file",
	Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		listConval(cmd,args)
	},
})


func initLvFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&ifile,"ifile","i","","ST33 input control file")
	cmd.Flags().StringVarP(&ofile,"ofile","o","","the output file relative to the home directory")
}


func init() {

	RootCmd.AddCommand(lsCtrlCmd)
	initLvFlags(lsCtrlCmd)


}

func listConval(cmd *cobra.Command, args []string) {

	var (

		home,pathname,file  string
		of *os.File
		err error
		w  *bufio.Writer
	)


	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}
	if len(ofile) != 0 {

        home = utils.GetHomeDir()
        pathname = filepath.Join(home,ofile)
        if _,err = os.Stat(pathname); os.IsNotExist(err) {
        	os.Create(pathname)
		}
		if of, err = os.OpenFile(pathname, os.O_APPEND|os.O_WRONLY, 0644); err != nil {
			gLog.Warning.Printf("Error %v opening file %s",pathname,err)
			os.Exit(1)
		} else {
			w = bufio.NewWriter(of)
			defer of.Close()
		}
	}

	_,file = filepath.Split(ifile)

	if c,err:=  st33.BuildConvalArray(ifile); err == nil {
		for k, v := range *c {
			imageType := ""
			lp := len(v.PxiId)
			if v.PxiId[lp-2:lp-1] == "B" {
				imageType = "BLOB"
			}
			if len(ofile) == 0 {
				gLog.Info.Printf("%d %s %s %d ... %s",k, v.PxiId, utils.Reverse(v.PxiId), v.Pages,imageType)
			} else {
				fmt.Fprintln(w, file, k, v.PxiId, utils.Reverse(v.PxiId), v.Pages,imageType)
			}
		}
		 if w != nil {
			 w.Flush()
		 }


	} else {
		gLog.Error.Println(err)
	}

}
