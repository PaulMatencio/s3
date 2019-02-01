
// Copyright © 2019 NAME HERE <EMAIL ADDRESS>
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
	"github.com/s3/api"
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"github.com/spf13/cobra"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"errors"
	"strings"
	"time"
)

// getS3DocCmd represents the dirInq command

var (
	key string
	getS3DocCmd = &cobra.Command{
		Use:   "getS3Doc",
		Short: "Command to get PXI S3 document(s)",
		Long: ``,
		Run: func(cmd *cobra.Command, args []string) {
			getS3Doc(cmd,args)
		},
	})

func init() {

	RootCmd.AddCommand(getS3DocCmd)
	getS3DocCmd.Flags().StringVarP(&key,"key","k","","the PXI ID of the document you would like to retrieve")
	getS3DocCmd.Flags().StringVarP(&bucket,"bucket","b","","the name of the  bucket")
	getS3DocCmd.Flags().StringVarP(&odir,"odir","O","","the ouput directory (relative to the home directory)")
	getS3DocCmd.Flags().StringVarP(&ifile,"ifile","i","","full pathname of an input  file containing a list of pix ids to be downloaded")
}


func getS3Doc(cmd *cobra.Command,args []string)  {

	var (
		start = utils.LumberPrefix(cmd)
		Keys   []string
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s",missingBucket)
		utils.Return(start)
		return
	}

	if len(key) == 0  && len(ifile)== 0 {
		gLog.Warning.Printf("%s","missing PXI id and input file containing list of pxi ids")
		utils.Return(start)
		return
	}

	if len(key) > 0 {
		Keys=append(Keys, key)
	}

	if len(ifile) >  0 {

		if b,err := ioutil.ReadFile(ifile); err == nil  {
			Keys = strings.Split(string(b)," ")
		} else {
			gLog.Error.Printf("%v",err)
			utils.Return(start)
			return
		}
	}

	if len(odir) >0 {
		pdir = filepath.Join(utils.GetHomeDir(),odir)
		if _,err:=os.Stat(pdir); os.IsNotExist(err) {
			utils.MakeDir(pdir)
		}
	}

	for _,key := range Keys {

		lp := len(key);
		if key[lp-2:lp-1] == "P" {
			if len(odir) == 0 {
				getDocP(key)
			} else {
				saveDocP(key, pdir)
			}

		} else if key[lp-2:lp-1] == "B" {
			getDocB(key, pdir)
		}
	}

	utils.Return(start)
}


func getDocP(key string ) {

	KEY := utils.Reverse(key)
	KEYx := KEY+".1"
	svc := s3.New(api.CreateSession())
	req := datatype.GetObjRequest{
		Service : svc,
		Bucket: bucket,
		Key : KEYx,  // Get the first Object
	}

	if resp,err  := api.GetObject(req); err == nil {
		var (
			pages int
			err   error
			val   string
		)

		// the first page should contain PXI metadata
		if val,err = utils.GetPxiMeta(resp.Metadata); err != nil {
			gLog.Error.Printf("Key : %s - PXI metadata %s is missing ",KEYx)
			return
		}

		if pages,err = strconv.Atoi(val); err != nil || pages == 0 {
			gLog.Error.Printf("Key : %s - PXI metadata %s is invalid", KEYx)
			return
		}

		//

		readObject(KEYx,resp)

		if pages > 1 {
			readObjects(KEY, pages-1, svc)
		}


	} else {
		gLog.Error.Printf("%v",err)
	}
}

//
//  Save  S3 ST33 Object into
//
//

func saveDocP(key string,pdir string ) {

	KEY := utils.Reverse(key)
	KEYx := KEY+".1"
	svc := s3.New(api.CreateSession())
	req := datatype.GetObjRequest{
		Service : svc,
		Bucket: bucket,
		Key : KEYx,  // Get the first Object
	}


	if resp,err  := api.GetObject(req); err == nil {
		var (
			pages int
			err   error
			val   string

		)
		// the first page should contain PXI metadata
		if val,err = utils.GetPxiMeta(resp.Metadata); err != nil {
			gLog.Error.Printf("Key : %s - PXI metadata %s is missing ",KEYx)
			return
		}

		if pages,err = strconv.Atoi(val); err != nil || pages == 0 {
			gLog.Error.Printf("Key : %s - PXI metadata %s is invalid", KEYx)
			return
		}

		// Save first object and its metadata
		saveMeta(KEYx,resp.Metadata)
		if err := saveObject(KEYx,resp,pdir); err != nil {
			gLog.Error.Printf("Saving object %s %v ",KEYx,err)
		}

		// save other objects
		if pages > 1 {
			saveObjects(KEY, pages-1, svc)
		}


	} else {
		gLog.Error.Printf("%v",err)
	}
}

//
//   Retrieve S3 BLOB Object
//   key :  Key of the S3 Object
//   pdir : output directory
//   if pdir is given, output will be saved into pdir
//

func getDocB(key string,pdir string) {

	KEY := utils.Reverse(key)
	// Get the number of Pages
	KEYx := KEY+".1"
	req := datatype.GetObjRequest{
		Service : s3.New(api.CreateSession()),
		Bucket: bucket,
		Key : KEYx,
	}

	if resp,err  := api.GetObject(req); err == nil {

		var (
			pages int
			err error
			val string
		)
		// document should contain user  metadata
		if val,err = utils.GetPxiMeta(resp.Metadata); err != nil {
			gLog.Warning.Printf("Key : %s - PXI metadata %s is missing ",KEYx)
			// Check if usermeta data  is valid
			if pages,err = strconv.Atoi(val); err != nil || pages != 1 {
				gLog.Warning.Printf("Key : %s - PXI metadata %s is invalid", KEYx)
			}
		}

		if len(pdir) == 0 {
			if err := readObject(KEYx, resp); err == nil {
				gLog.Info.Printf("Pxi id %s is retrieved",key)
			}


		} else {

			saveMeta(KEYx,resp.Metadata)
			if err := saveObject(KEYx, resp, pdir); err == nil { // save Object content
				gLog.Info.Printf("Pxi id %s is downloaded to %s", key,pdir)
			}
		}

		if pages > 1 {
			err = errors.New("Number of Pages:"+ *resp.Metadata["Pages"])
			gLog.Warning.Printf("Oop! Wrong number of pages for %s %v",KEYx, err)
		}

	} else {
		gLog.Error.Printf("%v",err)
	}
}


//  save S3 object in streaming mode
//  key : S3 key of the object
//  resp :  S3 GetObjectOutput response
//  pdir : output directory

func saveObject(key string, resp *s3.GetObjectOutput,pdir string ) (error) {
	pathname := filepath.Join(pdir,strings.Replace(key,string(os.PathSeparator),"_",-1))
	return utils.SaveObject(resp,pathname)
}


//  key : S3 key of the object
//  resp :  S3 GetObjectOutput response

func readObject(key string, resp*s3.GetObjectOutput) (error){

	b, err := utils.ReadObject(resp.Body)
	if err == nil {
		gLog.Trace.Printf("Key: %s  - ETag: %s  - Content length: %d - Object lenght: %d",key,*resp.ETag,*resp.ContentLength,b.Len())
	} else {
		gLog.Error.Printf("%v",err)
	}
	return err
}


//
// Retrieve Objects and display their length
//
// Input
//		Key : PXI id
//		Pages: Number of pages
//      svc : S3 service
//

func readObjects(key string, pages int,svc *s3.S3 ) {

	var (
		N = pages
		T = 0
		ch  = make(chan *datatype.Rb)
	)

	for p:=1 ; p <= pages; p++ {

		KEYx := key + "." + strconv.Itoa(p)
		go func(KEYx string) {

			req := datatype.GetObjRequest{
				Service: svc,
				Bucket:  bucket,
				Key:     KEYx,
			}

			if resp, err := api.GetObject(req); err == nil {
				b, err := utils.ReadObject(resp.Body)
				// gLog.Trace.Printf("Go Response Key: %s %v", KEYx,err)
				if err == nil {
					ch <- &datatype.Rb{
						Key:      KEYx,
						Object:   b,
						Result:   resp,
						Err:      err,
					}
				}
			} else {
				//gLog.Trace.Printf("Response Key: %s  %v", KEYx, err)
				ch <- &datatype.Rb{
					Key:    KEYx,
					Object: nil,
					Result: nil,
					Err:    err,
				}
			}

			req = datatype.GetObjRequest{}
		}(KEYx)
	}

	for  {
		select {
		case rb := <-ch:
			T++
			if rb.Err == nil {
				gLog.Trace.Printf("Key: %s  - ETag: %s  - Content length: %d - Object length: %d", rb.Key, *rb.Result.ETag, *rb.Result.ContentLength, len(rb.Object.Bytes()))
			} else {
				gLog.Error.Printf("Error getting object key %s: %v",rb.Key,rb.Err)
			}
			if T == N {
				gLog.Info.Printf("%d objects are retrieved for pxid %s",N+1,key)
				return
			}
		case <-time.After(300 * time.Millisecond):
			fmt.Printf("w")
		}
	}
}

//
// Download  Objects from S3  to Folder
//
// input
// 		key  PXI key
// 		number of pages to download
// 		S3 service
//

func saveObjects(key string, pages int,svc *s3.S3 ) {

	var (
		N  = pages
		T  = 0
		ch = make(chan *datatype.Ro)
		resp *s3.GetObjectOutput
		err  error
	)

	for p:=1 ; p <= pages; p++ {

		KEYx := key + "." + strconv.Itoa(p)
		go func(KEYx string) {

			req := datatype.GetObjRequest{
				Service: svc,
				Bucket:  bucket,
				Key:     KEYx,
			}

			if resp, err = api.GetObject(req); err == nil {
				saveObject(KEYx,resp,pdir)
			}

			ch <- &datatype.Ro {
				Key : KEYx,
				Result: resp,
				Err : err,
			}

			req = datatype.GetObjRequest{}
		}(KEYx)
	}

	for  {
		select {
		case ro := <-ch:
			T++
			if ro.Err == nil {
				gLog.Trace.Printf("Key: %s - ETag: %s - Content length: %d - downloaded to: %s", ro.Key, *ro.Result.ETag, *ro.Result.ContentLength,pdir)
			} else {
				gLog.Error.Printf("Error getting object key %s: %v",ro.Key,ro.Err)
			}
			if T == N {
				gLog.Info.Printf("%d objects are downloaded for pxid %s to %s",N+1,key, pdir)
				return
			}
		case <-time.After(50 * time.Millisecond):
			fmt.Printf("w")
		}
	}
}

func saveMeta(key string, metad map[string]*string) {

	pathname := filepath.Join(pdir,strings.Replace(key,string(os.PathSeparator),"_",-1)+".md")
	utils.WriteUsermd(metad,pathname)

}