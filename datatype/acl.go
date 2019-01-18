package datatype

import "github.com/aws/aws-sdk-go/service/s3"

type acl struct {

	Owner       s3.Owner   // used for setting a new owner
	Grantee  	s3.Grantee //  Email address and type
	Email       string
	UserType    string
	Permission  string

}

/*

	s3.Grantee{EmailAddress: &address, Type: &userType}




 */


