// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/aws/session"

	"github.com/spf13/cobra"
)

// getObjectCmd represents the getObject command
var getObjectCmd = &cobra.Command{
	Use:   "getObject",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("getObject called")
	},
}

var bucket,key string

func init() {
	rootCmd.AddCommand(getObjectCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// getObjectCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// getObjectCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	getObjectCmd.Flags().StringVarP(&bucket,"bucket","b","","the bucket name to get the object")
	getObjectCmd.Flags().StringVarP(&key,"key","k","","the  key of the object")
}


//  getObject utilizes the api to get object
func getObject(ccmd *cobra.Command,args []string) {
	// handle any missing args
	switch {

	case len(bucket) == 0:
		fmt.Println("Missing bucket - please provide the bucket for object you'd like to get")
		return

	case len(key) == 0:
		fmt.Println("Missing key - please provide the key for object you'd like to get")
		return
	}
	
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := svc.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				fmt.Println(s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	fmt.Println(result.Metadata)

}