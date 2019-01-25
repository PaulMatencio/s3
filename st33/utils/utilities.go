package st33

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/moses/user/files/lib"
	"github.com/s3/utils"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	// imaging "github.com/desintegration/imaging"
)



type Tiff struct {
	Enc          binary.ByteOrder
	SFrH         []byte
	SFrW         []byte
	NlFrH        []byte
	NlFrW        []byte
	RotationCode []byte
	TotalLength  int
}


type Request struct {
	InputFile  string
	WriteTo    string
	OutputDir  string   /* Directory an S3 bucket  */
}

type Response struct {
	Bucket  string
	Key   	string
	Data    bytes.Buffer
	Number	int
	Error 	S3Error

}

type S3Error struct {
	Key 	string
	Err	    error
}

// import "github.com/s3/st33/utils"

type St33ToS3      struct {
	Request  		 ToS3Request       `json:"request"`
	Response         ToS3Response      `json:"response"`
}


type ToS3Request struct {

	File 		      string     `json:"input-file"`
	Bucket 		      string     `json:"target-bucket"`
	LogBucket         string    `json:"logging-bucket"`
	DatafilePrefix    string     `json:"data-file-prefix"`
	CrlfilePrefix     string     `json:"control-file-prefix"`
	Profiling 	      int        `json:"run-with-profiling"`
	Async 		      int        `json:"run-with-concurrent-number"`

}

type ToS3Response struct {

	Time   time.Time  `json:"ended-time"`
	Status string     `json:"uploaded-status"` // upload status
	Duration  string  `json:"time-to-uploaded"` // duration of the process
	Docs  int         `json:"number-of-documents"` // number of documenst
	Pages int         `json:"number_of-pages"`  // number of pages
	Size  int         `json:"total-uploaded-size"` // total uploaded size
	Errors int		  `json:"number-of-errors"` // number of errors
	// Error  []S3Error  `json:"array-of-errors,omitempty"` // key value
}
/*
	image orientation
*/
func GetOrientation(rotation_code []byte) uint16 {
	orientation, _ := strconv.Atoi(string(rotation_code))
	switch orientation {
	case 1:
		return uint16(1)
	case 2:
		return uint16(6)
	case 3:
		return uint16(3)
	case 4:
		return uint16(8)
	default:
		return uint16(1)
	}
}

func Getuint32(in []byte) uint32 {
	out, _ := strconv.Atoi(string(in))
	return uint32(out)
}

func Getuint16(in []byte) uint16 {
	out, _ := strconv.Atoi(string(in))
	return uint16(out)
}

/*
		6 bytes
 */

func SetTiffMagicNumber(buffer *bytes.Buffer,enc binary.ByteOrder  )  error {
	_, err := io.WriteString(buffer, beHeader)          // magic number
	err = binary.Write(buffer, enc, uint32(ifdOffset)) // IFD offset
	err = binary.Write(buffer, enc, uint16(ifdLen))    // number of IFD entries
	return err
}
/*
		12  bytes
 */
func SetTiffImageWidth(buffer *bytes.Buffer, enc binary.ByteOrder, width []byte) error{
	err := binary.Write(buffer, enc, uint16(tImageWidth)) //  image Width
	err = binary.Write(buffer, enc, uint16(dtLong))      //  long
	err = binary.Write(buffer, enc, uint32(1))           //  value
	err = binary.Write(buffer, enc, Getuint32(width))
	return err
}
/*
			12  bytes
 */
func SetTiffImageLength(buffer *bytes.Buffer, enc binary.ByteOrder, width []byte) error {
	err := binary.Write(buffer, enc, uint16(tImageLength)) //  image lenght
	err = binary.Write(buffer, enc, uint16(dtLong))        //  long
	err = binary.Write(buffer, enc, uint32(1))             //  value
	err = binary.Write(buffer, enc, Getuint32(width))
	return err
}
/*
		12  bytes
 */
func SetTiffImageCompression(buffer *bytes.Buffer,enc binary.ByteOrder) error{
	err := binary.Write(buffer, enc, uint16(tCompression)) //  Compression
	err = binary.Write(buffer, enc, uint16(dtShort))      //  short
	err = binary.Write(buffer, enc, uint32(1))            //  value
	err = binary.Write(buffer, enc, uint16(cG4))          //  CCITT Group 4
	err = binary.Write(buffer, enc, uint16(0))            //  CCITT Group 4
	return err
}

/*
		12 bytes
 */
func SetTiffImagePhotometric(buffer *bytes.Buffer,enc binary.ByteOrder) error{
	err := binary.Write(buffer, enc, uint16(tPhotometricInterpretation)) //  Photometric
	err = binary.Write(buffer, enc, uint16(dtShort))                    //  short
	err = binary.Write(buffer, enc, uint32(1))                          //  value
	err = binary.Write(buffer, enc, uint32(0))                          //  white
	return err
}
/*
		 12 bytes
 */
func SetTiffImageStripOffset(buffer *bytes.Buffer,enc binary.ByteOrder) error{
	err := binary.Write(buffer, enc, uint16(tStripOffsets)) //  StripOffsets
	err = binary.Write(buffer, enc, uint16(dtLong))        //  long
	err = binary.Write(buffer, enc, uint32(1))             //  value
	err = binary.Write(buffer, enc, uint32(150))           //  0xA0
	return err
}
/*

 */
func SetTiffImageOrientation(buffer *bytes.Buffer,enc binary.ByteOrder,rotationCode []byte) error {
	err := binary.Write(buffer, enc, uint16(tOrientation)) // Orientation
	err = binary.Write(buffer, enc, uint16(dtShort))      //  short
	err = binary.Write(buffer, enc, uint32(1))
	err = binary.Write(buffer, enc, GetOrientation(rotationCode)) // rotation code
	err = binary.Write(buffer, enc, uint16(0))
	return err
}
/*
			12 bytes
 */
func SetTiffImageStripByteCount(buffer *bytes.Buffer,enc binary.ByteOrder, totalLength uint32) error{
	err := binary.Write(buffer, enc, uint16(tStripByteCounts)) //  StripbyteCounts
	err = binary.Write(buffer, enc, uint16(dtLong))           //  long
	err = binary.Write(buffer, enc, uint32(1))
	// imageLPos := buffer
	err = binary.Write(buffer, enc, uint32(totalLength))      //  image size
	return err
}

/*
		12 bytes
 */

func SetTiffImageXresolution(buffer *bytes.Buffer, enc binary.ByteOrder) error{
	err := binary.Write(buffer, enc, uint16(tXResolution)) // Xresolution
	err = binary.Write(buffer, enc, uint16(dtRational))   // rational
	err = binary.Write(buffer, enc, uint32(1))
	err = binary.Write(buffer, enc, uint32(xoffset)) //
	return err
}

/*
		12 bytes
 */

func SetTiffImageYresolution(buffer *bytes.Buffer,enc binary.ByteOrder) error {
	err := binary.Write(buffer, enc, uint16(tYResolution)) // Yresolution
	err = binary.Write(buffer, enc, uint16(dtRational))   // rational
	err = binary.Write(buffer, enc, uint32(1))
	err = binary.Write(buffer, enc, uint32(yoffset))
	return err
}

/*
			12 bytes
 */
func SetTiffImageResolutionUnit(buffer *bytes.Buffer,resolution []byte,enc binary.ByteOrder) error {
	err := binary.Write(buffer, enc, uint16(tResolutionUnit)) // resolution Unit
	err = binary.Write(buffer, enc, uint16(dtShort))         //  value
	err = binary.Write(buffer, enc, uint32(1))
	err = binary.Write(buffer, enc, uint16(2)) //  2 instead of 3
	err = binary.Write(buffer, enc, uint16(0))
	return err
}

/*
	Generate a magic number for bucket location
 */
func GetMagic( id string) (string){
	var total uint = 0
	for i,r := range id {
		if r & 1 == 1 {
			total += uint(r)*uint(i)
		} else {
			total += uint(r) << 1
		}
	}
	total = total & 0xFF
	return  "pxi"+fmt.Sprint(total)
}




/*
	write image to a file
	create the file path if directories don't exist
 */


func WriteImgToFile(pathname string ,img *bytes.Buffer){
	// log.Println("write",filePath)
	dir,_ := filepath.Split(pathname)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0755)
	}
	files.Check(utils.WriteFile(pathname, img.Bytes(), 0644))
}

func BuildUsermd(v Conval) (map[string]string,error)  {

	var (
		err 	error
		metad = make(map[string]string)
	)

	if jsonB,err  := json.Marshal(v); err == nil {
		json :=base64.StdEncoding.EncodeToString(jsonB)
		pages:= strconv.Itoa(int(v.Pages))
		lp := len(v.PxiId)
		if v.PxiId[lp-2:lp-1] == "B" {
			pages = "1"
		}
		metad["Usermd"] = json
		metad["Pages"] = pages
	}

	return metad,err
}


func WriteUsermd(metad map[string]string,pathname string)  (error){

	if usermd,err := json.Marshal(metad) ; err == nil {
		return ioutil.WriteFile(pathname,[]byte(usermd),0644)
	} else {
		return err
	}
}

