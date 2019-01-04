### s3  front end commands
  

Usage:
  
  **sc [command]**

**Available Commands:**

  ~~~~
  delBucket   Command to delete a bucket
  fgetObj     Command to download an objet to a file
  fputObj     Command to upload a file to a bucket
  getObj      Command to get an object
  getObjs     Command to download multiple objects from a bucket
  headObj     Command to retrieve an object metadata
  help        Help about any command
  lsBucket    Command to list all your buckets
  lsObjs      Command to list the objects in a specific bucket
  mkBucket    Command to create a bucket
  rmBucket    Command to delete a bucket
  rmObj       Command to delete an object
  rmObjs      Command to delete multiple objects
  statObj     Command to retrieve an object metadata
  statObjs    Command to retieve multiple objects metadata

Flags:
  -g, --autoCompletion   generate bash auto completion
  -c, --config string    sc config file; default $HOME/.sc/config.yaml
  -h, --help             help for sc
  -l, --loglevel int     Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)
  -v, --verbose          verbose output (equivalent to -l 4 )

Use "sc [command] --help" for more information about a command.
~~~~


**Configuration file** 

`./config.yaml`

`~/.sc/config.yaml`

`/etc/sc/config.xaml`



~~~~
sc_url: http://10.12.201.11
sc_region: us-east-1
sc_access_key_id: kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk
sc_secret_access_key: ssssssssssssssssssssssssssssssssssss
logging:
  log_level: 3   
meta:
  extension: md
~~~~

**Command delBucket**
~~~~
Command to delete a bucket

Usage:
  sc delBucket [flags]

Flags:
  -b, --bucket string   the bucket name to get the object
  -h, --help            help for delBucket

Global Flags:
  -g, --autoCompletion   generate bash auto completion
  -l, --loglevel int     Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)
  -v, --verbose          verbose output

  ~~~~
  
  **Command fgetObj**
  
  ~~~~
  Command to download an objet to a file
  
  Usage:
    sc fgetObj [flags]
  
  Flags:
    -b, --bucket string   the bucket name to get the object
    -h, --help            help for fgetObj
    -k, --key string      the  key of the object
    -o, --odir string     the ouput directory you'like to save
  
  Global Flags:
    -g, --autoCompletion   generate bash auto completion
    -l, --loglevel int     Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)
    -v, --verbose          verbose output
~~~~

  **Command fputObject** 
  
  ~~~~Command to upload a file to a bucket
  
  Usage:
    sc fputObj [flags]
  
  Flags:
    -b, --bucket string     the bucket name
    -f, --datafile string   the data file to upload
    -h, --help              help for fputObj
  
  Global Flags:
    -g, --autoCompletion   generate bash auto completion
    -l, --loglevel int     Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)
    -v, --verbose          verbose output
~~~~

