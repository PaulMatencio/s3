package utils

import (
	"encoding/base64"
	"encoding/json"
	"github.com/s3/gLog"
	"io/ioutil"
	"os"
)


func GetUserMeta(metad map[string]*string) (string,error) {

	var (
		err error
		u []byte
	)

	if v,ok := metad["Usermd"];ok {
		u,err =  base64.StdEncoding.DecodeString(*v)
		return string(u),err
	} else {
		return "",err
	}

}

func GetPxiMeta(metad map[string]*string) (string,error) {

	var (
		err error
	)

	if v,ok := metad["Pages"];ok {
		return *v,err
	} else {
		return "",err
	}

}



func BuildUserMeta(meta []byte) (map[string]*string) {

	metad := make(map[string]*string)
	if len(meta) > 0 {
		m := base64.StdEncoding.EncodeToString(meta)
		metad["Usermd"] = &m
	}
	return metad
}





func WriteUserMeta(metad map[string]*string,pathname string ) {

	var (
		usermd string
		err    error
	)
	/* convert map to json */


	if usermd,err  = GetUserMeta(metad); err == nil && len(usermd) > 0 {
		if err:= ioutil.WriteFile(pathname,[]byte(usermd),0644); err != nil {
			gLog.Error.Printf("Error %v writing %s ",err,pathname)
		}

	}

}



func PrintUserMeta(key string, metad  map[string]*string) {

	if v,ok:= metad["Usermd"];ok {
		usermd,_ :=  base64.StdEncoding.DecodeString(*v)
		gLog.Info.Printf("key:%s - User metadata: %s", key, usermd)
	}

}

func PrintPxiMeta(key string, metad  map[string]*string) {

	if v,ok := metad["Pages"];ok {
		gLog.Info.Printf("Key %s - Pxi metadata %s=%s", key,"Pages", *v)
	}

}

func BuildUsermd(usermd map[string]string) (map[string]*string) {

	metad := make(map[string]*string)
	for k,v := range usermd {
		V:= v     /*
		             Circumvent  a Go pointer  problem  => &v points to same address for every k
		          */
		metad[k] = &V
	}
	return metad
}




func PrintUsermd(key string, metad map[string]*string) {

	for k,v := range metad {
		if k == "Usermd" {
			usermd,_ :=  base64.StdEncoding.DecodeString(*v)
			gLog.Info.Printf("Key: %s %s = %s",key, k,string(usermd))
		} else {
			gLog.Info.Printf("Key: %s %s = %s", key, k,*v)
		}
	}
}

func WriteUsermd(metad map[string]*string ,pathname string ) {

	/* convert map into json */
	if usermd,err := json.Marshal(metad); err ==  nil {
		if err:= ioutil.WriteFile(pathname,[]byte(usermd),0644); err != nil {
			gLog.Error.Printf("Error %v writing %s ",err,pathname)
		}

	}
}

func ReadUsermd(pathname string) (map[string]string,error) {

	var (
		meta = map[string]string{}
		err   error
		usermd []byte
	)
	if  _,err:= os.Stat(pathname) ; err == nil {

		if usermd, err = ioutil.ReadFile(pathname); err == nil {
			if err = json.Unmarshal(usermd, &meta); err == nil {
				return meta, err
			}
		}
	}
	if os.IsNotExist(err) {
		return meta,nil
	}
	return meta,err

}