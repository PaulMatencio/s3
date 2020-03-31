package datatype

import "time"

type S3Metadata struct {
	CommonPrefixes []interface{} `json:"CommonPrefixes"`
	Contents       []Contents    `json:"Contents"`
	IsTruncated    bool          `json:"IsTruncated"`
}

type ACL struct {
	Canned      string        `json:"Canned"`
	FULLCONTROL []interface{} `json:"FULL_CONTROL"`
	WRITEACP    []interface{} `json:"WRITE_ACP"`
	READ        []interface{} `json:"READ"`
	READACP     []interface{} `json:"READ_ACP"`
}

type Tags struct {
}

type ReplicationInfo struct {
	Status             string        `json:"status"`
	Backends           []interface{} `json:"backends"`
	Content            []interface{} `json:"content"`
	Destination        string        `json:"destination"`
	StorageClass       string        `json:"storageClass"`
	Role               string        `json:"role"`
	StorageType        string        `json:"storageType"`
	DataStoreVersionID string        `json:"dataStoreVersionId"`
}

type Value struct {
	OwnerDisplayName                          string          `json:"owner-display-name"`
	OwnerID                                   string          `json:"owner-id"`
	ContentLength                             int             `json:"content-length"`
	ContentMd5                                string          `json:"content-md5"`
	XAmzVersionID                             string          `json:"x-amz-version-id"`
	XAmzServerVersionID                       string          `json:"x-amz-server-version-id"`
	XAmzStorageClass                          string          `json:"x-amz-storage-class"`
	XAmzServerSideEncryption                  string          `json:"x-amz-server-side-encryption"`
	XAmzServerSideEncryptionAwsKmsKeyID       string          `json:"x-amz-server-side-encryption-aws-kms-key-id"`
	XAmzServerSideEncryptionCustomerAlgorithm string          `json:"x-amz-server-side-encryption-customer-algorithm"`
	XAmzWebsiteRedirectLocation               string          `json:"x-amz-website-redirect-location"`
	ACL                                       ACL             `json:"acl"`
	Key                                       string          `json:"key"`
	Location                                  interface{}     `json:"location"`
	IsDeleteMarker                            bool            `json:"isDeleteMarker"`
	Tags                                      Tags            `json:"tags"`
	ReplicationInfo                           ReplicationInfo `json:"replicationInfo"`
	DataStoreName                             string          `json:"dataStoreName"`
	LastModified                              time.Time       `json:"last-modified"`
	MdModelVersion                            int             `json:"md-model-version"`
	XAmzMetaUsermd                            string          `json:"x-amz-meta-usermd"`
}

type Contents struct {
	Key   string `json:"key"`
	Value Value  `json:"value"`
}