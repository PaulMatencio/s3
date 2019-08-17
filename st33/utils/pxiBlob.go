package st33

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/s3/gLog"
	"github.com/s3/utils"
)

type pxiBlob struct {
	Key     	string
	DatePub		string
	Size    	int
	Record      int
	Blob   		*bytes.Buffer
}

func  NewPxiBlob(key string, records int) (*pxiBlob) {

	var pxiblob = pxiBlob{
		Key : utils.Reverse(key)+".1",
		Record: records,
		Blob  :	 new(bytes.Buffer),
	}
	return &pxiblob
}

func ( blob *pxiBlob ) BuildPxiBlobV1(buf []byte, l int64 ) (int64, error) {

	var (
		err      			error
		Big      			binary.ByteOrder = binary.BigEndian
		bdw, rdw, blobl,blobRecs   	uint16
		totalLength         uint32
	)

	/*
		Seek the start of a  Blob  record .
		previous  RDW are not sufficient since
		some records are append with garbage ( 0 padding for instance)

	*/

	l,bdw,rdw,err = seekBdw(buf,l)
	k := l + 8 				            						// skip bdw and rdw

	if  IsST33Blob(buf,k) {

		// gLog.Info.Printf("ST33 BLOB Buffer pointer => Key: %s - Buffer  length:%d - recs: %d%d - l: x'%X'  -  k: x'%X'", blob.Key, len(buf), blob.Record,l, k)

		_ = binary.Read(bytes.NewReader(buf[k+84 : k+86]), Big, &blobRecs)
		_ = binary.Read(bytes.NewReader(buf[k+214 : k+218]), Big, &totalLength)
		gLog.Info.Printf("ST33 BLOB Buffer pointer => Key: %s - Total blob length:%d  - Buffer length:%d - Blob recs: %d/%d - l: x'%X'  -  k: x'%X'", blob.Key, totalLength,len(buf), blob.Record,blobRecs,l, k)
		//  First  build the  resulting  blob with all the ST33 records
		//
		blobL := 0

		for rec:= 1; rec <= int(blobRecs); rec ++ {

			err = binary.Read(bytes.NewReader(buf[k+250 : k+252]), Big, &blobl)
			gLog.Info.Printf("ST33 BLOB Buffer pointer => Key: %s - Blob chunk length: %d  - rec of/recs: %d/%d - l: x'%X'  -  k: x'%X'  bdw/rdw: %d/%d", blob.Key,blobl, rec,blobRecs,l, k,bdw,rdw)

			blobL += int(blobl)
			blob.Blob.Write(buf[k+252 : k+252+int64(blobl)]) 	// Build blob
			l = l + int64(bdw) 									// Next record
			k = l + 8  			 								// skip bdw and rdw
		}

		// check if total image length given by the ST33 header  is equal to the length of the blob
		if int(totalLength) != blobL {
			err = errors.New(fmt.Sprintf("ST33 Blob %s bad Header - Total Lenght %d != Blob length %d",blob.Key, blobL,totalLength))
			gLog.Error.Printf("%v",err)
		}

		// Second continue with the other remaining blob records
		// the number of blob records are taken from the control file
		gLog.Info.Printf("PXIID:%s - BLOB  Key: %s - remaining Blob records: %d  - k: X'%x'",utils.Reverse(blob.Key), blob.Key,blob.Record,k)

		for rec := 1; rec <= int(blob.Record); rec++ {
			gLog.Info.Printf("l: X'%x' - k: X'%x'",l,k )
			l,bdw,rdw,err = seekBdw(buf,l) 					 // Look for bdw and rdw
			k:= l+8 										// skip bdw and rdw
			gLog.Info.Printf(" Blod record =====>  %d  -  l: X'%x' - k: X'%x' rdw:%d",rec,k,l,rdw-4)
			blob.Blob.Write(buf[k : k-4 + int64(rdw)])      // build the blob
			l = l + int64(bdw) 								// point to next record
		}

		gLog.Info.Printf("ST33 Blob Id %s  - Blob length %d",blob.Key, blob.Blob.Len())

	}  else {

		gLog.Trace.Printf( "PXIID %s Key: %s  - BLOB records number : %d - BLOB Buffer pointer => Buffer length: %d  x'%X'  l: x'%X' k: x'%X'", blob.Key, utils.Reverse(blob.Key), int(blob.Record),len(buf), len(buf),l, k)

		if l >= int64(len(buf)) {
			error := fmt.Sprintf("PXIID %s Key: %s  - BLOB records number : %d - BLOB Buffer pointer => Buffer length: %d  x'%X'  l: x'%X' k: x'%X'", blob.Key, utils.Reverse(blob.Key), int(blob.Record),len(buf), len(buf),l, k)
			err := errors.New(error)
			return l,err
		}
		blob.Blob.Write(buf[k : k-4 +int64(rdw)])

		l = l + int64(bdw)


		for rec := 2; rec <= int(blob.Record); rec++ {

			//  a blob record may be longer than its RDW value
			//  look for a pair of bdw and rdw
			l,bdw,rdw,err = seekBdw(buf,l)
			//  stop if the address is beyomd buffer length
			if l >= int64(len(buf)) {
				error := fmt.Sprintf("PXIID %s Key: %s  - BLOB records number: %d - BLOB Buffer pointer => Buffer length: %d  x'%X'  l: x'%X' k: x'%X'", blob.Key, utils.Reverse(blob.Key), int(blob.Record),len(buf), len(buf),l, k)
				err := errors.New(error)
				return l,err
			}
			// SKip the good pair of  bdw,rdw
			k:= l+8
			//  Write the blob as it is
			// rdw contains the record len
			blob.Blob.Write(buf[k : k-4 + int64(rdw)])       // Build the blob with records
			//  l is the address of the enxt record
			l = l + int64(bdw)

		}
		gLog.Info.Printf("Regular Blob Id %s  - Blob length %d",blob.Key, blob.Blob.Len())

	}

	return l,err
}


/*
	Seek the start of Blob  records .
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
			gLog.Trace.Printf("====> Seek  x'%v' l  ",l)   // For trouble shooting
			if l > int64(len(buf)) {
				l = int64(len(buf))                               // Prevent  issue  slice outbound
				break
			}
		} else {
			break
		}

	}
	gLog.Trace.Printf("Seek l = x'%X' bdw = %d  rdw = %d ",l, bdw,rdw)
	return l,bdw,rdw,err
}



//
//  pxiblob method : buildpxiblob version 2
//  Input : The pxiblob ( object)
//          St33 Reader
//          Current entry of the control file
//  Read St33 record by record of the BLOB
//  return   number of record, error
//
func ( blob *pxiBlob ) BuildPxiBlobV2(r *St33Reader,v Conval) (int, error) {

	var (
		err      			error
		Big      			binary.ByteOrder = binary.BigEndian
		blobl,blobRecs   	uint16
		blobLength         uint32
		nrec                int = 0
	)

	gLog.Trace.Printf("Build BLOB  - Buffer pointer => Key: %s - Previous: x'%X' - Current: x'%X'", blob.Key,r.GetPrevious(),r.GetCurrent())

	buf,err := r.Read()    // 	Read the first BLOB  record
	nrec++                 //   increment the number of records

	if  IsST33Blob(buf,0) {    //  Check if is it a ST33 BLOB ?


		err = CheckST33Length(&v,r,buf)
		if err != nil {
			gLog.Error.Println(err)
			gLog.Info.Printf("%s", hex.Dump(buf[0:255]))
			return nrec,err
		}

		_ = binary.Read(bytes.NewReader(buf[84 : 86]), Big, &blobRecs)       // Number of Bloc records
		_ = binary.Read(bytes.NewReader(buf[214 : 218]), Big, &blobLength)   // Total length of the BLOB
		_ = binary.Read(bytes.NewReader(buf[250:252]), Big, &blobl)          // Get the length of the first chunk of the BLOB



		//
		//   Append the first chunk to the Blob buffer
		//

		blobL := int(blobl)
		// gLog.Info.Printf("ST33 BLOB Buffer pointer => Key: %s - Buffer length:%d - blob Record: %d /%d - Previous : x'%X' - Current: x'%X'", blob.Key, len(buf), blob.Record,  blobRecs,r.GetPrevious(), r.GetCurrent())
		blob.Blob.Write(buf[252 : 252+int64(blobl)])   // Append the first chunk to the blob

		gLog.Trace.Printf("ST33 BLOB  Key: %s - Total Blob length:%d - Blob chunk length %d  - rec of/recs: %d/%d - Prev: x'%X' - Cur: x'%X'", blob.Key,  blobLength,blobl, 1, blobRecs,r.GetPrevious(),r.GetCurrent())

		for rec:= 2; rec <= int(blobRecs); rec ++ {    // Read  the other chunks

			if buf,err = r.Read(); err == nil  {
				nrec++
				if len(buf) > 252 {
					err = binary.Read(bytes.NewReader(buf[250:252]), Big, &blobl) // blobl =length of a  chunk
					gLog.Info.Printf("ST33 BLOB  Key: %s - Blob chunk length: %d  - rec of/recs: %d/%d - Prev: x'%X' - Cur: x'%X' ", blob.Key, blobl, rec, blobRecs, r.Previous, r.Current)
					blobL += int(blobl) //  blobL = length of the BLOB
					if int(blobl) + 252 > len(buf) {
						blob.Blob.Write(buf[252:]) // Append chunk to the BLOB buffer
					} else {
						blob.Blob.Write(buf[252 : 252+int64(blobl)]) // Append chunk to the BLOB buffer
					}
				}
			} else {
				gLog.Error.Printf("Error %v after reading BLOB record number %d from input data file at Prev : X'%x'  Cur: X'%x' position",err,nrec, r.Previous, r.Current)
				return nrec,err
			}
		}


		//
		// Continue to build the blob
		// STOP  if computed blob length differ from the data file
		//

		if int(blobLength) != blobL {
			err = errors.New(fmt.Sprintf("==> ST33 Blob bad Header. %s - Total length:%d != Blob length:%d",blob.Size,blobL,blobLength))
			gLog.Error.Printf("%v",err)

		}

		gLog.Trace.Printf("PXIID:%s - ST33 BLOB Key: %s - remaining Blob records: %d ",v.PxiId, blob.Key,blob.Record)

		//
		// Read some other blob ST33 records
		// The number of other blob records are read from the control file
		//  v.Records == blob.Record
		//

		for rec := 1; rec <= int(blob.Record); rec++ {
			gLog.Info.Printf("Blob record Prev : X'%x'  Cur: X'%x'",r.Previous,r.Current)
			if buf,err = r.Read(); err == nil  {
				nrec ++
				blob.Blob.Write(buf)
			} else   {
				break
			}
		}


		gLog.Trace.Println("PXIID: %s - Key %s - Blob control records/Reda records %d/%d",v.PxiId,utils.Reverse(v.PxiId),blobRecs,nrec)

		//  Skip and discard unread records
		if int(v.Records) > nrec  {
			extra := v.Records - nrec
			for m:=1 ; m <= extra; m++ {
				if _,err:= r.Read(); err == nil  {
					gLog.Warning.Printf("==> ST33 BLOB PIXID %d  -  SKIP record %d",v.PxiId, extra+nrec)
				}
			}
		}
		gLog.Info.Printf("ST33 Blob Id %s  - Blob length %d",blob.Key, blob.Blob.Len())

	}  else {                                              // Just a regular BLOB

		gLog.Trace.Printf( "PXIID %s - Key: %s - BLOB records# : %d - BLOB Buffer pointer => Buffer length: %d  x'%X'  Previous: x'%X' Current: x'%X'", blob.Key, utils.Reverse(blob.Key), int(blob.Record),len(buf), len(buf),r.Previous, r.Current)
		// blob.Blob.Write(buf[:len(buf)-4])                  // add the first record to the BLOB
		blob.Blob.Write(buf)
		for rec := 1; rec <  int(blob.Record); rec++ {     // append records to the BLOB
			if buf,err  := r.Read(); err == nil  {
				nrec++
				blob.Blob.Write(buf)
			}
		}
		gLog.Trace.Printf("Regular Blob Id %s  - Blob length %d",blob.Key, blob.Blob.Len())
	}

	return nrec,err
}


