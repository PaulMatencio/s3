package s3Client

import (
	"net/http"
)

func ExtractUserMeta1(m http.Header) map[string]string{

	m1 := make(map[string]string)
	for k,v := range m {
		if len(k) > 10 {
			if k[0:10] == "X-Amz-Meta" {
				k1 := k[11:]
				m1[k1] = v[0]
			}
		}
	}
	return m1
}


func ExtractUserMeta(m http.Header) map[string]string{
	m1 := make(map[string]string)
	for k,v := range m {
		if len(k) > 10 && k[0:10] == "X-Amz-Meta" {
				k1 := k[11:]
				m1[k1] = v[0]
			}
		}
	return m1
}