package st33

import (
	"bytes"
	"encoding/binary"
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

//
//  pxiblob method : buildpxiblob version 2
//  Input : The pxiblob ( object)
//          St33 Reader
//          Current entry of the control file
//  Read St33 record by record of the BLOB
//  return   number of record, error
//
func ( blob *pxiBlob ) BuildPxiBlob(r *St33Reader,v Conval) (int, error) {

	var (
		err      			error
		Big      			binary.ByteOrder = binary.BigEndian
		blobl,blobRecs   	uint16
		blobLength          uint32
		nrec                int = 0
	)

	gLog.Trace.Printf("Build BLOB  - Buffer pointer => Key: %s - Previous: x'%X' - Current: x'%X'", blob.Key,r.GetPrevious(),r.GetCurrent())

	buf,err := r.Read()    // 	Read the first BLOB  record
	nrec++                 //   increment the number of records

	if  IsST33Blob(buf,0) {    //  Check if is it a ST33 BLOB ?

		/*

		Already been checked by IsTss3Blob

		err = CheckST33Length(&v,r,buf)
		if err != nil {
			gLog.Error.Println(err)
			if len(buf) > 252 {
				gLog.Info.Printf("%s", hex.Dump(buf[0:252]))
			}
			return nrec,err
		}
		*/


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


