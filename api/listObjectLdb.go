package api

import (
	"github.com/s3/datatype"
	"github.com/s3/gLog"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

func ListObjectLdb(request datatype.ListObjLdbRequest) (datatype.Rlb, error) {

	var (
		keyMarker, req  string
		contents []byte
		resp datatype.Rlb
		delim,prefix string
		err error
	)

		/*
				build the request
			    curl -s '10.12.201.11:9000/default/bucket/moses-meta-02?listType=DelimiterMaster&prefix=FR/&maxKeys=2&delimiter=/'
		*/
		req = "/default/bucket/"+request.Bucket+"?listType=DelimiterMaster&prefix="
		limit := "&maxKeys="+strconv.Itoa(int(request.MaxKey))

		if len(request.Delimiter) >0 {
			delim = "&delimiter="+ request.Delimiter
		}
		if len(request.Marker) > 0 {
			keyMarker = "&marker=" + request.Marker
		}
		if len(request.Prefix) > 0 {
			prefix = request.Prefix
		}
		// url := Host +":"+Port+request+prefix+limit+keyMarker+delim
		url := request.Url+req+prefix+limit+keyMarker+delim
		gLog.Trace.Println("URL:",url)
		if response,err := http.Get(url); err == nil {
			resp.StatusCode = response.StatusCode
			if response.StatusCode == 200 {
				defer response.Body.Close()
				if contents, err = ioutil.ReadAll(response.Body); err == nil {
					resp.Contents = ContentToJson(contents)
				}
			}
		}
		return resp,err
}



// transform content returned by the bucketd API into JSON string
func ContentToJson(contents []byte ) string {
	result:= strings.Replace(string(contents),"\\","",-1)
	result = strings.Replace(result,"\"{","{",-1)
	// result = strings.Replace(result,"\"}]","}]",-1)
	result = strings.Replace(result,"\"}\"}","\"}}",-1)
	result = strings.Replace(result,"}\"","}",-1)
	gLog.Trace.Println(result)
	return result
}

func ListCommonPrefix( cp []interface{}) {
	gLog.Info.Println("List Common prefix:")
	for _, p := range cp{
		gLog.Info.Printf("Common prefix %s", p)
	}
}
