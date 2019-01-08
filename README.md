###s3  front end commands

This S3 command line may used with any compatible S3 storage: Amazone, Scality, Minio, etc..
It is built on top of the AWS golang SDK version 1. 

####  Installation

~~~~
install and configure  go 1.10+
cd $GOPATH
git pull https://github.com/PaulMatencio/s3
cd $GOPATH/github/s3/sc
make deps
make install
~~~~

####Configuration 

sc will look into these 3 locations for a configuration file name config.yaml

`./config.yaml`

`~/.sc/config.yaml`

`/etc/sc/config.xaml`

if a config.yaml file could not be found, sc will use the aws shared configuration 
files that are located in the .aws folder of the home directory. It is recommended to use the 
config.yaml file of the command line, however you can also configure the credential and
config files created by aws utility. 

***Example of a configuration file***
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

#### Usage:
  
  **sc [command]**

#####Available Commands:

~~~~

    delBucket   Command to delete a bucket
    fgetObj     Command to download an objet to a file
    fputObj     Command to upload a file to a bucket
    getObj      Command to get an object
    getObjs     Command to download some or all  objects and their metadata to a specific directory
    headObj     Command to retrieve an object metadata
    headObjs    Command to retieve some of the metadata of specific or every object in the bucket
    help        Help about any command
    lsBucket    Command to list all your buckets
    lsObjs      Command to list specific or every object of a bucket
    mkBucket    Command to create a bucket
    putObjs     Command to upload obbjects and their user metadata from a specific directory
    rmBucket    Command to delete a bucket
    rmObj       Command to delete an object
    rmObjs      Command to delete multiple objects
    statObj     Command to retrieve an object metadata
    statObjs    Command to retieve some of the metadata of specific or every object in the bucket
  
  Flags:
    -C, --autoCompletion   generate bash auto completion
    -c, --config string    sc config file; default $HOME/.sc/config.yaml
    -h, --help             help for sc
    -l, --loglevel int     Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)
    -P, --profiling int    display memory usage every P seconds
    -v, --verbose          verbose output
    
~~~~
***Use "sc [command] --help" for more information about a command.***

####Bash autocompletion script

Use the flag -C along with any command to generate a bash auto completion script.
 Copy the generated _sc_bash_completion_ script to _/etc/bash_completion.d_ or 
 just add "source sc_bash_completion" to your .basrc file to active the bash autocompletion
 for the sc CLI

