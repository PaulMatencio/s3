package st33

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"github.com/s3/gLog"
	"github.com/s3/utils"
)

type PxiImg struct {
	PxiId   	[]byte
	PageNum 	[]byte
	RefNum  	[]byte
	Img     	*bytes.Buffer
	DataType 	[]byte
	NumPages 	[]byte
}

//  return the address of aPxiImg structure
func NewPxiImg() (*PxiImg) {

	return &PxiImg{}
}


//  Return a TIFF image
func (image *PxiImg) GetImage() *bytes.Buffer {
	return image.Img
}

func (image *PxiImg) GetPageNum() []byte {
	return image.PageNum
}

func (image *PxiImg) GetPxiId() []byte{
	return image.PxiId
}

func (image *PxiImg) SetPxiId(p []byte) {
	image.PxiId = p
}

func (image *PxiImg) GetRefNum() []byte {
	return image.RefNum
}

func (image *PxiImg) GetDataType() []byte {
	return image.DataType
}



func (image *PxiImg) BuildTiffImage(r *St33Reader, v Conval) (int,error) {

	var (
		totalRec       uint16
		totalLength    uint32
		recs           uint16
		imgl           uint16
		err   		   error
		nrec           int = 0
	)

	Little	:= binary.LittleEndian
	Big		:= binary.BigEndian
	enc 	:= Big

	buf,err := r.Read()   // Read  the First record
	if err != nil {
		return nrec,err
	}

	nrec++

	err = CheckST33Length(&v,r,buf)
	if err != nil {
		gLog.Error.Println(err)
		gLog.Info.Printf("%s", hex.Dump(buf[0:255]))
		return nrec,err
	}

	err 		= 	binary.Read(bytes.NewReader(buf[25 : 27]), Big, &recs)
	err 		= 	binary.Read(bytes.NewReader(buf[84 : 86]), Big, &totalRec)
	err 		= 	binary.Read(bytes.NewReader(buf[214 : 218]), Big, &totalLength)
	err			= 	binary.Read(bytes.NewReader(buf[250 : 252]), Big, &imgl)

	/*
		convert St33 encoded Big Endian input data ( EBCDIC) to  Little Endian (ASCII)
	*/


	st33 	:= utils.Ebc2asci(buf[0: 214])

	// long, _ := 	strconv.Atoi(string(st33[0:5]))

	image.PxiId		= st33[5:17]   //  PXI ID
	image.PageNum	= st33[17:21]  // page nunber
	image.RefNum	= st33[34:45]  // was 41
	image.NumPages 	= st33[76:80]  // Total number of pages
	image.DataType	= st33[180:181]  // data type

	/*
		comp_meth := st33[181:183]
		k_fac := st33[183:185]
		Resolution := st33[185:187]
	*/
	Resolution := st33[185:187]
	sFrH 	:= st33[187:190]
	sFrW 	:= st33[190:193]
	nlFrH 	:= st33[193:197]
	nlFrW 	:= st33[197:201]
	rotationCode := st33[201:202]

	// fr_x := st33[202:206]
	// fr_y := st33[206:210]
	// fr_stat := st33[210:211]

	version := st33[211:214]

	buf1	 := bytes.NewReader(buf[214 : 218]) // get  the ST33 version number

	if string(version) == "V30" {

		//	buf1 = bytes.NewReader(buf[k+214 : k+218])
		//	 some V30 total_length are encoded with big Endian byte order

		_ = binary.Read(buf1, Little, &totalLength)

		if int(totalLength) > 16777215 {
			buf1 = bytes.NewReader(buf[214 : 218])
			_ = binary.Read(buf1, Big, &totalLength)
		}

	} else {
		_ = binary.Read(buf1, Big, &totalLength) // get  total length of the image
	}

	//  Build the tiff image header

	var img = new(bytes.Buffer)

	_ = SetTiffMagicNumber(img, enc)                    // Magic number   6 bytes
	_ = SetTiffImageWidth(img, enc, nlFrW)              //  image WIDTH   12 bytes
	_ = SetTiffImageLength(img, enc, nlFrH)             //  image HEIGHT  12 bytes
	_ = SetTiffImageCompression(img, enc)               // image compression cG4 12 bytes
	_ = SetTiffImagePhotometric(img, enc)               //  image Photometric 12 bytes
	_ = SetTiffImageStripOffset(img, enc)               //  image Stripoffsets  12 bytes
	_ = SetTiffImageOrientation(img, enc, rotationCode) //  image Orientation   12 bytes

	//   computing the image length is done after every records containing the image  are read
	//   continue to create the  TIFF header before the image lenght attribute

	var img2 = new(bytes.Buffer)


	_ = SetTiffImageXresolution(img2,enc)    //  image X resolution
	_ = SetTiffImageYresolution(img2, enc)    //  image Y resolution
	_ = SetTiffImageResolutionUnit(img2, Resolution, enc) //  image resolution Unit

	_ = binary.Write(img2, enc, uint32(0)) // next IFD = 0

	_ = binary.Write(img2, enc, Getuint32(nlFrW)*25) // Xresolution value
	_ = binary.Write(img2, enc, Getuint32(sFrW))

	_ = binary.Write(img2, enc, Getuint32(nlFrH)*25) // Yresoluton value
	_ = binary.Write(img2, enc, Getuint32(sFrH))


	imageL := 0    // Total length of the image
	// build the image with the St33 first record
	_ = binary.Read(bytes.NewReader(buf[250:252]), Big, &imgl)
	img2.Write(buf[252 : 252+int64(imgl)])       // append  the image length found in this record  to the  image
	imageL += int(imgl)

	// read all the records for this image.
	// the number of records are extracted from the image header

	for rec := 2; rec <= int(totalRec); rec++ {

		if buf,err = r.Read();err == nil  {
			nrec++   // increment the number of records

			_ = binary.Read(bytes.NewReader(buf[250:252]), Big, &imgl)

			if int64(imgl) <= int64(len(buf) - 252) {
				img2.Write(buf[252 : 252+int64(imgl)]) // append  the image length found in this record  to the  image
				imageL += int(imgl)                    //  Compute the total  image length
			} else {
				err := errors.New("Invalid image length")
				return nrec,err
			}
		} else {
			//  return the read error  to the caller
			break    /*  12-04-2019  */
		}
	}

	//	Check if the input header of the first record we read is an ST33 recird
	//	totalLength is the length of the TIFF image extracted from the first record  of the image
	//	imageL is the sum of the image length of all records = true length of the image

	var img3 = new(bytes.Buffer)

	if int(totalLength) < imageL {
		totalLength = uint32(imageL)
	}

	//  set the TIFF image length */
	_ = SetTiffImageStripByteCount(img3, enc, uint32(imageL)) //  image Strip Byte Counts


	//		Append  img3 and img2 into img to form the final TIFF image
	//	    img2 and  img3  bytes buffer willbe reset when this function is exited

	img.Write(img3.Bytes())
	img.Write(img2.Bytes())

	defer img2.Reset()
	defer img3.Reset()

	// return the final image in the image struct
	//  It is recommanded to reset the the buffer of the image when it is consummed  by the client  */
	image.Img = img

	// check if number of records match
	// If not skip all the remaining records in the data file

	return  nrec,err

}


