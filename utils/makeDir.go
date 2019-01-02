package utils

import (
	"os"
)

func MakeDir(dir string) {
	if _,err:= os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir,0755)
	}
}
