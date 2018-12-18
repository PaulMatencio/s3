package utils

import (
	"bytes"
	"io"
)

//  object is  io reader
func ReadObject( object io.Reader)  (*bytes.Buffer, error){
	buffer:= make([]byte,32768)
	buf := new(bytes.Buffer)
	for {

		n, err := object.Read(buffer)
		if err == nil || err == io.EOF {
			buf.Write(buffer[:n])
			if err == io.EOF {
				buffer = buffer[:0] // clear the buffer fot the GC
				return buf,nil
			}
		} else {
			buffer = buffer[:0] // clear the buffer for the GC
			return buf,err
		}
	}
}