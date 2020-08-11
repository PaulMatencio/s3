package datatype

import (
	"encoding/json"
	"github.com/s3/gLog"
	"os"
)
type Clusters struct {
	Topology []Topology `json:"topology"`
}
type Repds struct {
	AdminPort   int    `json:"adminPort"`
	DisplayName string `json:"display_name"`
	Host        string `json:"host"`
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Port        int    `json:"port"`
	Site        string `json:"site"`
}

type Wsbs struct {
	AdminPort   int    `json:"adminPort"`
	DisplayName string `json:"display_name"`
	Host        string `json:"host"`
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Port        int    `json:"port"`
	Site        string `json:"site"`
}
type Topology struct {
	Num   int     `json:"num"`
	Repds []Repds `json:"repds"`
	Wsbs  []Wsbs  `json:"wsbs"`
}

func  (c Clusters) GetClusters(file string) (error,*Clusters) {
	gLog.Trace.Printf("Input json file :%s",file)
	if cFile, err := os.Open(file); err == nil {
		defer cFile.Close()
		// decoder:= json.NewDecoder(cFile)
		return json.NewDecoder(cFile).Decode(&c),&c

	} else {
		return err,&c
	}
}

type RaftSessions []struct {
	ID                int           `json:"id"`
	RaftMembers       []RaftMembers `json:"raftMembers"`
	ConnectedToLeader bool          `json:"connectedToLeader"`
}
type RaftSession struct {
	ID                int           `json:"id"`
	RaftMembers       []RaftMembers `json:"raftMembers"`
	ConnectedToLeader bool          `json:"connectedToLeader"`

}

type RaftMembers struct {
	AdminPort   int    `json:"adminPort"`
	DisplayName string `json:"display_name"`
	Host        string `json:"host"`
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Port        int    `json:"port"`
	Site        string `json:"site"`
}

type RaftLeader struct {
	IP string `json:"ip,omitempty"`
	Port int `json:"port,omitempty"`
}


type RaftState struct {
	Term       int `json:"term"`
	Voted      int `json:"voted"`
	Appended   int `json:"appended"`
	Backups    int `json:"backups"`
	Committing int `json:"committing"`
	Committed  int `json:"committed"`
	Pruned     int `json:"pruned"`
}



func  (c RaftSessions) GetRaftSessions(file string) (error,*RaftSessions) {
	gLog.Trace.Printf("Input json file :%s",file)
	if cFile, err := os.Open(file); err == nil {
		defer cFile.Close()
		return json.NewDecoder(cFile).Decode(&c),&c
	} else {
		return err,&c
	}
}








