
package cmd

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/s3/st33/utils"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"strconv"
)

// readConvalCmd represents the readConval command
var checkS3Cmd = &cobra.Command{
	Use:   "chkS3",
	Short: "Command to check if all the  Tiff images and Blobs of a given st33 data file have been migrated to a S3 bucket",
	Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		checkS3(cmd,args)
	},
}

func initCvFlags(cmd *cobra.Command) {


	cmd.Flags().StringVarP(&ifile,"ifile","i","","the corresponding control file that was used to migrate the data file")
	cmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the bucket you would like to check against a given data file")
	// cmd.Flags().StringVarP(&odir,"ofile","o","","output file")
}


func init() {

	RootCmd.AddCommand(checkS3Cmd)
	initCvFlags(checkS3Cmd)


}

func checkS3(cmd *cobra.Command, args []string) {

	var (
		pages,w  int=0,0
	)
	if len(ifile) == 0 {
		gLog.Info.Printf("%s",missingInputFile)
		return
	}

	if c,err:=  st33.BuildConvalArray(ifile); err == nil {

		svc := s3.New(api.CreateSession())

		for k, v := range *c {

			req := datatype.StatObjRequest{
				Service:  svc,
				Bucket: bucket,
				Key:utils.Reverse(v.PxiId)+".1",
			}

			if result, err  := api.StatObject(req); err == nil {

				p,_ := strconv.Atoi(*result.Metadata["Pages"]) // Number of pages of the document
				vp := int(v.Pages) // number of pages taken from the control file
				if vp != 0 && p != vp  {  // the number of pages of a blob document = 0
					gLog.Warning.Printf("Conval index %d - Conval Key %s -  S3 Key %s  - Conval pages %d !=  S3 pages %d",k, v.PxiId, req.Key,v.Pages,p)
					w++
				}
				pages += p

			} else {
				gLog.Error.Printf("Error %v while getting the metadata of key %s",err,req.Key)
				w++
			}
		}

		if w ==  0 {
			gLog.Info.Printf("%d documents, %d  pages have been migrated without error",len(*c),pages)
		}
	} else {
		gLog.Error.Println(err)
	}
}