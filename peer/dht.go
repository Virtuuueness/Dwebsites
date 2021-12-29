package peer

import (
	"go.dedis.ch/cs438/types"
	"io"
)

type KademliaDHT interface {
	Store(key string, value []byte)
	FindValue(key string) ([]byte, bool)
	FindNode(key string) []types.Contact
	Ping(dest string)
	Bootstrap(peer string)
	UploadDHT(data io.Reader) (metahash string, err error)
	DownloadDHT(metahash string) ([]byte, error)
}

// max number of contacts stored in one k-bucket
const K = 4

// degree of parallelism for network calls made while finding k closest nodes
const Alpha = 3
