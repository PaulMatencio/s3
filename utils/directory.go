package utils

import (
	"os"
	"runtime"
)

func GetHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return os.Getenv("HOME")
}


func MakeDir(dir string) {
	if _,err:= os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir,0755)
	}
}