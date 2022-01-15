package peer

import (
	"go.dedis.ch/cs438/types"
	"io"
)

type KademliaDHT interface {
	// Stores given key/value pair
	Store(key string, value []byte)

	// Finds value in DHT associated with given key
	// - a value is a list of nodes which contain data associated with given key, in a form
	// of a string where addresses are seperated with MetaFileSep
	FindValue(key string) ([]byte, bool)

	// Finds K-Closest nodes to given key
	FindNode(key string) []types.Contact

	// Pings given node
	Ping(dest string)

	// Bootstraps node so they can exchange DHT messages
	Bootstrap(peer string)

	// Stores given data localy, and updates DHT with appropriate info
	UploadDHT(data io.Reader) (metahash string, err error)

	// Downloads data associated with given metahash from the DHT
	DownloadDHT(metahash string, keep bool) ([]byte, error)
}

// max number of contacts stored in one k-bucket
const K = 4

// degree of parallelism for network calls made while finding k closest nodes
const Alpha = 3
