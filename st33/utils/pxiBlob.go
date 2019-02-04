package st33

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/s3/utils"
	"github.com/s3/gLog"
)

type pxiBlob struct {
	Key     	string
	DatePub		string
	Size    	int
	Record      int
	Blob   		*bytes.Buffer
}



func ( blob *pxiBlob ) BuildPxiBlob(buf []byte, l int64 ) (int64, error) {

	var (
		err      			error
		Big      			binary.ByteOrder = binary.BigEndian
		bdw, rdw, blobl,totalRecs   	uint16
		totalLength         uint32
	)

	/*
		Seek the start of a  Blob  record .
		previous  RDW are not sufficient since
		some records are append with garbage ( 0 padding for instance)

	*/

	l,bdw,rdw,err = seekBdw(buf,l)
	k := l + 8 					// skip bdw and rdw

	if  isST33Blob(buf,k) {

		gLog.Trace.Printf("ST33 BLOB Buffer pointer => Key: %s - Buffer  length:%d - r: %d - l: x'%X'  -  k: x'%X'", blob.Key, len(buf), blob.Record,l, k)

		_ = binary.Read(bytes.NewReader(buf[k+84 : k+86]), Big, &totalRecs)
		_ = binary.Read(bytes.NewReader(buf[k+214 : k+218]), Big, &totalLength)

		//  First  build the  resulting  blob with all the ST33 records
		//
		blobL := 0

		for r:= 0; r < int(totalRecs); r ++ {

			err = binary.Read(bytes.NewReader(buf[k+250 : k+252]), Big, &blobl)
			blobL += int(blobl)
			blob.Blob.Write(buf[k+252 : k+252+int64(blobl)]) 	// Build blob
			l = l + int64(bdw) 									// Next record
			k = l + 8  			 								// skip bdw and rdw
		}

		// check if total image length given by the ST33 header  is equal to the length of the blob
		if int(totalLength) != blobL {
			err = errors.New(fmt.Sprintf("ST33 Blob %s bad Header - Total Lenght %d != Blob length %d",blob.Size,blobL,totalLength))
		}

		// Second continue with the other remaining blob records
		// the number of blob records are taken from the control file

		for r := 0; r < int(blob.Record); r++ {

			l,bdw,rdw,err = seekBdw(buf,l) 							    // Look for bdw and rdw
			// gLog.Trace.Printf("k: %s - r :%d - l: x'%X'  bdw: %d rdw: %d ",blob.Key,r,l,bdw,rdw)
			k:= l+8 										// skip bdw and rdw
			blob.Blob.Write(buf[k : k-4 + int64(rdw)])      // build the blob
			l = l + int64(bdw) 								// point to next record
		}

	}  else {

		gLog.Trace.Printf( "PXIID %s Key: %s  - BLOB records number : %d - BLOB Buffer pointer => Buffer length: %d  x'%X'  l: x'%X' k: x'%X'", blob.Key, utils.Reverse(blob.Key), int(blob.Record),len(buf), len(buf),l, k)

		if l >= int64(len(buf)) {
			error := fmt.Sprintf("PXIID %s Key: %s  - BLOB records number : %d - BLOB Buffer pointer => Buffer length: %d  x'%X'  l: x'%X' k: x'%X'", blob.Key, utils.Reverse(blob.Key), int(blob.Record),len(buf), len(buf),l, k)
			err := errors.New(error)
			return l,err
		}
		blob.Blob.Write(buf[k : k-4 +int64(rdw)])
		l = l + int64(bdw)

		for r := 0; r < int(blob.Record)-1; r++ {
			/* a blob record may be longer than its RDW value */
			l,bdw,rdw,err = seekBdw(buf,l)
			if l >= int64(len(buf)) {
				error := fmt.Sprintf("PXIID %s Key: %s  - BLOB records number: %d - BLOB Buffer pointer => Buffer length: %d  x'%X'  l: x'%X' k: x'%X'", blob.Key, utils.Reverse(blob.Key), int(blob.Record),len(buf), len(buf),l, k)
				err := errors.New(error)
				return l,err
			}
			k:= l+8
			blob.Blob.Write(buf[k : k-4 + int64(rdw)])       // Build the blob with records
			l = l + int64(bdw)

		}
	}

	return l,err
}


func isST33Blob(buf []byte, k int64) (bool) {

	if im :=  string(utils.Ebc2asci(buf[k+180 : k+182])); im == "IM"{
		return true
	} else {
		return false}
}


/*
	Seek the start of a  Blob  record . BDW and RDW  aree not sufficient
	some records are longer than its RDW with garbage
*/

func seekBdw(buf []byte, l int64) (int64, uint16,uint16,error){

	var 	(
		bdw, rdw 	uint16
		err			error
	)
	Big  := binary.BigEndian

	for {
		// buf1 := bytes.NewReader(buf[l : l+2])
		err = binary.Read(bytes.NewReader(buf[l : l+2]), Big, &bdw)
		// buf1 = bytes.NewReader(buf[l+4 : l+6])
		err = binary.Read(bytes.NewReader(buf[l+4 : l+6]), Big, &rdw)

		if bdw-rdw != 4 {
			l++
			gLog.Trace.Printf("======================> Seek  x'%v' l  ",l)
			if l > int64(len(buf)) {
				l = int64(len(buf))                       //  issue  slice outbound
				break
			}
		} else {
			break
		}

	}
	gLog.Trace.Printf("Seek l = x'%X' bdw = %d  rdw = %d ",l, bdw,rdw)
	return l,bdw,rdw,err
}



func ( blob *pxiBlob ) BuildPxiBlob1(buf []byte, l int64 ) (int64, error){

	var (
		err error
		// Big binary.ByteOrder= binary.BigEndian
	)

	for r:= 0; r < int(blob.Record);r++ {
		var bdw,rdw uint16
		l,bdw,rdw,err = seekBdw(buf,l)
		blob.Blob.Write(buf[l+8:l+int64(rdw)+4])
		l = l + int64(bdw)
	}
	return l,err
}

