package st33

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/s3/gLog"
	"github.com/s3/utils"
	"os"
	"strconv"
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


func (image *PxiImg) BuildTiffImage(buf []byte, l int64) (int64,error) {

	var (
		totalRec       uint16
		totalLength    uint32
		recs           uint16
		imgl           uint16
		bdw,rdw	       uint16
		err   		   error

	)

	Little	:= binary.LittleEndian
	Big		:= binary.BigEndian
	enc 	:= Big



	l,bdw,rdw,err = seekBdw(buf,l)
	if l >= int64(len(buf)) {
		error := fmt.Sprintf("PXIID %s Key: %s  - ST33 records number: %d - BLOB Buffer pointer => Buffer length: %d  x'%X'  l: x'%X'", string(image.PxiId), utils.Reverse(string(image.PxiId)),l)
		err := errors.New(error)
		return l,err
	}


	// buf1 := bytes.NewReader(buf[l : l+2])
	// _ = binary.Read(bytes.NewReader(buf[l : l+2]), Big, &bdw)
	// buf1 = bytes.NewReader(buf[l+4 : l+6])
	// _ = binary.Read(bytes.NewReader(buf[l+4 : l+6]), Big, &rdw)


	gLog.Trace.Printf("TIFF Buffer pointer => %d x'%X' %d %d ",len(buf),l,bdw,rdw)

	k := l      /* save begininng of the record */
	l = l + 8   /* skip BDW and RDW of the record*/

	/*
	    extract values that are Big Endian encoded :
		total number of records that contain the image
		total length of the image
	    image length in this record
	*/

	err 		= 	binary.Read(bytes.NewReader(buf[l+25 : l+27]), Big, &recs)
	err 		= 	binary.Read(bytes.NewReader(buf[l+84 : l+86]), Big, &totalRec)
	err 		= 	binary.Read(bytes.NewReader(buf[l+214 : l+218]), Big, &totalLength)
	err			= 	binary.Read(bytes.NewReader(buf[l+250 : l+252]), Big, &imgl)


	/*
		convert St33 encoded Big Endian input data ( EBCDIC) to  Little Endian (ASCII)
	*/


	st33 	:= utils.Ebc2asci(buf[l: l+214])

	long, _ := 	strconv.Atoi(string(st33[0:5]))

	image.PxiId		= st33[5:17]
	image.PageNum	= st33[17:21]
	image.RefNum	= st33[34:41]
	image.NumPages 	= st33[76:80]
	image.DataType	= st33[180:181]

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

	buf1	 := bytes.NewReader(buf[l+214 : l+218]) // get  the ST33 version number

	if string(version) == "V30" {
		/*
			buf1 = bytes.NewReader(buf[k+214 : k+218])
			 some V30 total_length are encoded with big Endian byte order
		*/
		_ = binary.Read(buf1, Little, &totalLength)

		if int(totalLength) > 16777215 {
			buf1 = bytes.NewReader(buf[l+214 : l+218])
			_ = binary.Read(buf1, Big, &totalLength)
		}

	} else {
		_ = binary.Read(buf1, Big, &totalLength) // get  total length of the image
	}

	/*
		 Build the tiff image header
	*/
	var img = new(bytes.Buffer)

	_ = SetTiffMagicNumber(img, enc)                    // Magic number   6 bytes
	_ = SetTiffImageWidth(img, enc, nlFrW)              //  image WIDTH   12 bytes
	_ = SetTiffImageLength(img, enc, nlFrH)             //  image HEIGHT  12 bytes
	_ = SetTiffImageCompression(img, enc)               // image compression cG4 12 bytes
	_ = SetTiffImagePhotometric(img, enc)               //  image Photometric 12 bytes
	_ = SetTiffImageStripOffset(img, enc)               //  image Stripoffsets  12 bytes
	_ = SetTiffImageOrientation(img, enc, rotationCode) //  image Orientation   12 bytes

	/*
	   computing the image length is done after every records containing the image  are read
	   continue to create the  TIFF header before the image lenght attribute
	*/

	var img2 = new(bytes.Buffer)


	_ = SetTiffImageXresolution(img2,enc)    //  image X resolution
	_ = SetTiffImageYresolution(img2, enc)    //  image Y resolution
	_ = SetTiffImageResolutionUnit(img2, Resolution, enc) //  image resolution Unit

	_ = binary.Write(img2, enc, uint32(0)) // next IFD = 0

	_ = binary.Write(img2, enc, Getuint32(nlFrW)*25) // Xresolution value
	 _ = binary.Write(img2, enc, Getuint32(sFrW))

	_ = binary.Write(img2, enc, Getuint32(nlFrH)*25) // Yresoluton value
	_ = binary.Write(img2, enc, Getuint32(sFrH))

	/*
	  For every record of the document

		-	SKip BDW,RDW of the record
		-	Get the length ( 2 bytes) of the partial image (  l + 250)
		- 	Read the  next  partial image
	  	- 	Append  the partial image to the  image  --> img2
	*/

	imageL := 0 // Total length of the image

	for r := 0; r < int(totalRec); r++ {
		/*
		var bdw,rdw uint16
		buf1 	:= 	bytes.NewReader(buf[k : k+2])
		_ 		= 	binary.Read(buf1, Big, &bdw)
		buf1 	= 	bytes.NewReader(buf[k+4 : k+6])
		_ 		= 	binary.Read(buf1, Big, &rdw)
		*/

		l = k +  8
		l1 := utils.Ebc2asci(buf[l : l+5])              // convert EBCDIC to Ascii
		long, _ = strconv.Atoi(string(l1))            	 //  Record length
		_ = binary.Read(bytes.NewReader(buf[l+250:l+252]), Big, &imgl)
		img2.Write(buf[l+252 : l+252+int64(imgl)])       // append  the image length found in this record  to the  image
		l = l + int64(long)                              // l point  the next record
		k  =  l
		imageL += int(imgl)   							//  Compute the total  image length

	}
	/*
		Check if the input header of thefirts record we read is consistent
		totalLength is the TIFF image length that is extracted from the first record  of the image
		imageL is the sum of the image lenght of each records = True length
	*/

	var img3 = new(bytes.Buffer)


	if int(totalLength) < imageL {
		totalLength = uint32(imageL)
	}

	/*  set the TIFF image length */
	//  _ = SetTiffImageStripByteCount(img3, enc, totalLength) //  image Strip Byte Counts
	_ = SetTiffImageStripByteCount(img3, enc, uint32(imageL)) //  image Strip Byte Counts

	/*
		Append  img3 and img2 into img to form the final TIFF image
	    img2 and  img3  bytes buffer willbe reset when this function is exited
	*/
	img.Write(img3.Bytes())
	img.Write(img2.Bytes())

	defer img2.Reset()
	defer img3.Reset()


	/*  return the final image in the image struct */
	/*  It is recommanded to reset the the buffer of the image when it is consummed  by the client  */
	image.Img = img

	return l,err

}


func (image *PxiImg) BuildTiffImage2(r *St33Reader, v Conval) (int,error) {

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

	l1 := utils.Ebc2asci(buf[0: 5])              // convert EBCDIC to Ascii
	long, _ := strconv.Atoi(string(l1))

	if long != len(buf) {
		gLog.Fatal.Println("Inavlid ST33 record %s  - %s ",long,len(buf))
		os.Exit(100)
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

	image.PxiId		= st33[5:17]
	image.PageNum	= st33[17:21]
	image.RefNum	= st33[34:41]
	image.NumPages 	= st33[76:80]
	image.DataType	= st33[180:181]

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

	// read all the records of this image
	for rec := 2; rec <= int(totalRec); rec++ {

		if buf,err = r.Read();err == nil  {
			nrec++
			_ = binary.Read(bytes.NewReader(buf[250:252]), Big, &imgl)
			img2.Write(buf[252 : 252+int64(imgl)]) // append  the image length found in this record  to the  image
			imageL += int(imgl)                    //  Compute the total  image length
		}
	}

	//	Check if the input header of thefirts record we read is consistent
	//	totalLength is the TIFF image length that is extracted from the first record  of the image
	//	imageL is the sum of the image lenght of each records = True length


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


