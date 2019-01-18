package utils

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
)

func PrintBucketAcl( r *s3.GetBucketAclOutput) {
	fmt.Printf("\nBucket owner:\n Display name:%s\n ID:%s\n",*r.Owner.DisplayName,*r.Owner.ID)
	for _,v := range r.Grants {

		fmt.Printf("Grantee:\n Display name:%s\n ID:%s\n",*v.Grantee.DisplayName,*v.Grantee.ID)
		fmt.Printf(" Permission %s\n",*v.Permission)
	}

}

func PrintObjectAcl( r *s3.GetObjectAclOutput) {
	fmt.Printf("\nBucket owner:\n Display name:%s\n ID:%s\n",*r.Owner.DisplayName,*r.Owner.ID)
	for _,v := range r.Grants {

		fmt.Printf("Grantee:\n Display name:%s\n ID:%s\n",*v.Grantee.DisplayName,*v.Grantee.ID)
		fmt.Printf(" Permission %s\n",*v.Permission)
	}

}
