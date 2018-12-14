package s3Client

import "os"

const  (

	BUFFERSIZE  	int			=65536                     /* GetObject buffer Size*/
	DELIMITER   	string		= "/"					/* Object delimiter */
	ABUCKET			string		=  "-b a Bucket"
	ALOCATION		string		= "-s a Location"
	AINDIRECTORY	string      = "-i an Input Directory"
	AOUTPUTDIR		string		= "-o an Output Directory"
	APREFIX			string		= "-p a Prefix"
	AFILE			string		= "-f a File"
	AOBJECT			string 		= "-o an Object"
	ADELIMITER		string		= "-d a Delimiter"
	AMAXKEY			string		= "-m maximum keys returned"
	TRACEON			string		= "-t trace on"
	FILEMODE		os.FileMode = 0600
	DIRMODE			os.FileMode = 0700

)