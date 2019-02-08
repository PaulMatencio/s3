package st33

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"os"
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

			//  a blob record may be longer than its RDW value

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
	)

	for r:= 0; r < int(blob.Record);r++ {
		var bdw,rdw uint16
		l,bdw,rdw,err = seekBdw(buf,l)
		blob.Blob.Write(buf[l+8:l+int64(rdw)+4])
		l = l + int64(bdw)
	}
	return l,err
}

func ( blob *pxiBlob ) BuildPxiBlob2(r *St33Reader,v Conval) ( error) {

	var (
		err      			error
		Big      			binary.ByteOrder = binary.BigEndian
		blobl,totalRecs   	uint16
		totalLength         uint32
		nrec                int
	)


	// 	Read the first BLOB  record
	buf,err := r.Read()
	nrec++
	//  Check if is it a ST33 BLOB ?
	if  IsST33Blob(buf,0) {

		gLog.Trace.Printf("ST33 BLOB Buffer pointer => Key: %s - Buffer  length:%d - r: %d - Previous : x'%X'  -  Current: x'%X'", blob.Key, len(buf), blob.Record,r.GetPrevious(), r.GetCurrent())

		_ = binary.Read(bytes.NewReader(buf[84 : 86]), Big, &totalRecs)
		_ = binary.Read(bytes.NewReader(buf[214 : 218]), Big, &totalLength)

		//
		//  Read the total number of BLOB records
		//
		blobL := 0

		for rec:= 2; rec <= int(totalRecs); rec ++ {

			if buf,err = r.Read(); err == nil {
				nrec++
				if len(buf) > 252 {
					err = binary.Read(bytes.NewReader(buf[250:252]), Big, &blobl)
					blobL += int(blobl)
					blob.Blob.Write(buf[252 : 252+int64(blobl)]) // Build blob
				}
			} else {
				 gLog.Error.Printf("Error %v after reading BLOB record number %d from input data file at Prev : X'%x'  Cur: X'%x' position",err,nrec, r.Previous, r.Current)
				 return err
			}
		}

		// check if total length of the image obtained from ST33 header   is equal to the length of the blob
		if int(totalLength) != blobL {
			err = errors.New(fmt.Sprintf("ST33 Blob %s bad Header - Total Lenght %d != Blob length %d",blob.Size,blobL,totalLength))
			os.Exit(100)
		}

		//
		// Read other blob ST33 records  The number of other blob records are taken from the control file
		//
		for rec := 1; rec <= int(v.Records); rec++ {
			if buf,err = r.Read(); err != nil {
				nrec ++
				blob.Blob.Write(buf)
			} else       /* if err == io.EOF */ {
				break
			}
		}
		// Check if all BLOB records are read . Discard unread bucket
		if int(v.Records) > nrec  {
			extra := v.Records - nrec
			for m:=1 ; m <= extra; m++ {
				if _,err:= r.Read(); err == nil {
					gLog.Warning.Printf("ST33 BLOB PIXID %d  -  SKIP record %d",v.PxiId, extra+nrec)
				}
			}
		}

	}  else {

		gLog.Trace.Printf( "BLOB PXIID %s  Key: %s  - BLOB records number : %d - BLOB Buffer pointer => Buffer length: %d  x'%X'  Previous: x'%X' Current: x'%X'", blob.Key, utils.Reverse(blob.Key), int(blob.Record),len(buf), len(buf),r.Previous, r.Current)
		blob.Blob.Write(buf)

		for rec := 0; rec < int(blob.Record)-1; rec++ {
			if buf,err  := r.Read(); err == nil {
				blob.Blob.Write(buf) // Build the blob with records
			}
		}
	}

	return err
}
