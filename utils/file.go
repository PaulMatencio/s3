package utils

import (
	"io/ioutil"
	"os"
)

func ReadFile(filename string) ([]byte , error) {
	var (
		data []byte
		err error

	)

	if _,err = os.Stat(filename); err == nil {

		if r,err := os.Open(filename); err == nil {
			data, err = ioutil.ReadAll(r)
		}
	}
	return data,err
}