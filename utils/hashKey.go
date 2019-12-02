package utils

func HashKey(key string, modulo int) (int) {
	v := 0;
	for k :=0; k < len(key); k++ {
		v += int(key[k])
	}
	return v % modulo
	//fmt.Printf("key:%s - hash:%d - modulo %d : %d \n",key, v, modulo, v1)

}

func KeyToAscii(key string) (int) {
	v := 0;
	for k :=0; k < len(key); k++ {
		v += int(key[k])
	}
	return v
	//fmt.Printf("key:%s - hash:%d - modulo %d : %d \n",key, v, modulo, v1)

}