package utils

import (
	"bytes"
	"encoding/binary"
	"github.com/s3/gLog"
	"os"
)

type VBToRecord struct {

	File        *os.File        // File  -> File descriptor of the VB file
	Buffer      *bytes.Buffer   // in memory buffer
	Size        int64           // Size of the file
	Current     int64           //  Current address of the record to read
	Previous    int64           //  Pointer to previous recod ( BDW,RDW,data)
	OutFile     *os.File        //  Outfile -> File descriptor of the Record file
}

// create a new VBtoRecord instance
func NewVBtoRecord(infile string , outfile string) (*VBToRecord, error,error) {

	f,err:= os.Open(infile)
	of,err1 := os.Create(outfile)
	if err == nil && err1 == nil  {
		finfo, _ := f.Stat()
		size := finfo.Size()
		return &VBToRecord{
			File:     f,
			Size:     size,
			Previous: 0,
			Current:  0,
			OutFile: of,
		}, err,err1
	} else {
		return nil,err,err1
	}
}

//
// Set position of the current record to read
//
func  (r *VBToRecord)  SetCurrent(c int64) {
	r.Current = c
}


//
//   Return the location of the current record
//
func  (r *VBToRecord)  GetCurrent() (int64){
	return r.Current
}

//
// Set  the location of the previous record
//
func  (r *VBToRecord)  setPrevious(c int64){
	r.Previous = c
}

//
// return the location of the previous  record
//
func  (r *VBToRecord)  GetPrevious() (int64) {
	return r.Previous
}


//
//   read a VB record
//   A record should start with BDW ( 4 bytes) and RDW  ( 4 bytes)
//   BDW -RDW should = 4
//

func (vb *VBToRecord) Read()  ([]byte,error){
	_,rdw,err := vb.getBDW()
	if err != nil  {
		return nil,err
	} else {
		b,err := vb.getRecord(int(rdw)) 	//  read  rdw bytes  at the position r.Current
		return b,err
	}
}

//
//  read n bytes from the current position (r.Current)
//

func (vb *VBToRecord)  getRecord(n int) ( []byte,error) {
	n = n - 4 //  minus 4 bytes ( rdw length )
	byte := make([]byte, n)
	_, err := vb.ReadAt(byte)
	vb.Previous = vb.Current
	vb.Current += int64(n)
	return byte, err
}

//
// read  b bytes from the current  position
//
func (vb *VBToRecord) ReadAt(b []byte) (int, error){

	f := vb.File
	c := vb.Current
	return f.ReadAt(b, c)
}


/*
func (vb *VBToRecord) readVB() ([]byte, error){
	_,rdw,err := vb.getBDW()
	if err != nil  {
		return nil,err
	} else {
		b,err := vb.getRecord(int(rdw)) 	//  read  rdw bytes  at the position r.Current
		return b,err
	}
}
*/


func (vb *VBToRecord) getBDW() (uint16,uint16,error) {
	var (
		Big binary.ByteOrder = binary.BigEndian
		bdw uint16
		rdw uint16
		err error
	)

	byte := make([]byte, 4)

	seek := vb.Current

	_, err = vb.ReadAt(byte) // Read BDW ( first 2 bytes )

	if err == nil {
		err = binary.Read(bytes.NewReader(byte), Big, &bdw)

		vb.Current += 4
	}
	// read rdw
	_, err = vb.ReadAt(byte) // Read RDW ( first 2 bytes)
	err = binary.Read(bytes.NewReader(byte[0:2]), Big, &rdw)

	if err != nil {
		return bdw,rdw,err
	}

	for {
		if bdw-rdw == 4 {
			vb.Current += 4 // skip RDW
			break

		} else {

			seek ++
			vb.Current = seek        // get Next Byte
			gLog.Info.Printf("bdw:%d -rdw:%d - Seek X'%x':",bdw,rdw,vb.Current)
			_, err = vb.ReadAt(byte)   // READ BDW
			err1 := binary.Read(bytes.NewReader(byte), Big, &bdw)
			vb.Current += 4        // READ RDW
			_, err = vb.ReadAt(byte)
			err1 = binary.Read(bytes.NewReader(byte), Big, &rdw)

			if err != nil || err1 != nil {
				gLog.Info.Printf("bdw:%d -r dw:%d - Seek X'%x' %v %v ",bdw,rdw,vb.Current,err,err1)
				break
			}
		}

	}

	return bdw,rdw,err

}