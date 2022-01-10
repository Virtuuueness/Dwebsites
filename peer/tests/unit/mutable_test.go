package unit

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
)

// Creates and validates a PointerRecord object using a RSA keypair, validation should not throw error
func Test_MUTABLE_CreateValidate(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	record, err := node1.CreatePointerRecord(privateKey, "some_metahash", 0, 10)
	require.NoError(t, err)

	err = node1.ValidatePointerRecord(record, &privateKey.PublicKey)
	require.NoError(t, err)
}

func getId(addr string) int64 {
	hash := sha1.Sum([]byte(addr))
	id := *big.NewInt(0)
	id.SetBytes(hash[:])
	return id.Int64()
}

// tests simple store and fetch
func Test_MUTABLE_KademliaSimple(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()
	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()
	node4 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()
	node5 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()
	node6 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node2.Bootstrap(node1.GetAddr())
	node3.Bootstrap(node1.GetAddr())
	node4.Bootstrap(node1.GetAddr())
	node5.Bootstrap(node1.GetAddr())
	node6.Bootstrap(node1.GetAddr())
	time.Sleep(time.Second * 1)

	key := "5a00fc30e073b095a6266136552a3da1d4622d0fdaa057f0b3135aa803321e1c"
	node2.Store(key, []byte("val1"))
	time.Sleep(time.Second * 1)
	// nodes 1,2,3,6 will store {key1, val1} in local table

	val1, ok := node2.FindValue(key)
	// should simply find it in local table
	require.Equal(t, true, ok)
	require.Equal(t, "val1", string(val1[:]))

	val1, ok = node4.FindValue(key)
	// doesn't find it localy, queries other nodes to fetch value
	require.Equal(t, true, ok)
	require.Equal(t, "val1", string(val1[:]))
}

// Since normaly key = hash(val), overwriting {key,val} pairs can only happen when uploading new PointerRecords
func Test_MUTABLE_KademliaOverwrite(t *testing.T) {
	numNodes := 10
	if numNodes < 2 {
		return
	}

	transp := channel.NewTransport()
	nodes := make([]z.TestNode, numNodes)

	for i := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
		defer node.Stop()

		nodes[i] = node
	}

	// bootstrap all nodes
	for i := 1; i < numNodes; i++ {
		nodes[i].Bootstrap(nodes[0].GetAddr())
	}
	time.Sleep(time.Second * 1)

	nodes[0].Store("key1", []byte("val1"))
	time.Sleep(time.Second * 1)

	// one node stores, everyone should be able to fetch it
	for i := range nodes {
		val1, ok := nodes[i].FindValue("key1")
		// queries other nodes to fetch value
		require.Equal(t, true, ok)
		require.Equal(t, "val1", string(val1[:]))
	}

	time.Sleep(time.Second * 1)
	nodes[1].Store("key1", []byte("val2"))
	time.Sleep(time.Second * 1)

	// everyone should fetch new value
	for i := range nodes {
		val1, ok := nodes[i].FindValue("key1")
		// queries other nodes to fetch value
		require.Equal(t, true, ok)
		require.Equal(t, "val2", string(val1[:]))
	}
}

// one node uploads file, everyone should be able to download it
func Test_MUTABLE_KademliaFileUploadDownload(t *testing.T) {
	numNodes := 20
	if numNodes < 2 {
		return
	}

	transp := channel.NewTransport()
	nodes := make([]z.TestNode, numNodes)

	for i := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
		defer node.Stop()

		nodes[i] = node
	}

	// bootstrap all nodes
	for i := 1; i < numNodes; i++ {
		nodes[i].Bootstrap(nodes[0].GetAddr())
	}
	println("Bootstraped ", numNodes, " nodes")
	time.Sleep(time.Second * 1)

	fileB := []byte("lorem ipsum dolor sit ametlorem ipsum dolor sit ametlorem ipsum dolo")
	mhB, err := nodes[0].UploadDHT(bytes.NewBuffer(fileB))
	require.NoError(t, err)

	// everyone can download now
	for i := range nodes {
		res, err := nodes[i].DownloadDHT(mhB)
		require.NoError(t, err)
		require.Equal(t, fileB, res)
	}
}

// upload file and pointer record associated with it, edit file and publish new pointer record under same address
// fetch edited file under same address as before
func Test_MUTABLE_KademliaEditFileUnderPointerRecord(t *testing.T) {
	numNodes := 10
	if numNodes < 2 {
		return
	}

	transp := channel.NewTransport()
	nodes := make([]z.TestNode, numNodes)

	for i := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
		defer node.Stop()

		nodes[i] = node
	}

	// bootstrap all nodes
	for i := 1; i < numNodes; i++ {
		nodes[i].Bootstrap(nodes[0].GetAddr())
	}
	println("Bootstraped ", numNodes, " nodes")
	time.Sleep(time.Second * 1)

	// node 0 uploads file
	fileB := []byte("A lorem ipsum dolor sit ametlorem ipsum dolor sit ametlorem ipsum dolo")
	mhB, err := nodes[0].UploadDHT(bytes.NewBuffer(fileB))
	require.NoError(t, err)

	// node 0 generates pointer record
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	record, err := nodes[0].CreatePointerRecord(privateKey, mhB, 0, 100) // sequence 0
	require.NoError(t, err)
	// node 0 publishes record
	recordHash, err := nodes[0].PublishPointerRecord(record)
	require.NoError(t, err)

	// everyone can fetch pointer record
	for i := range nodes {
		fetchedRecord, ok := nodes[i].FetchPointerRecord(recordHash)
		require.Equal(t, true, ok)
		require.Equal(t, mhB, fetchedRecord.Value)

		// everyone can download file pointer to by record
		res, err := nodes[i].DownloadDHT(fetchedRecord.Value)
		require.NoError(t, err)
		require.Equal(t, fileB, res)
	}

	//Edit file: aka. upload new file, and publish updated pointer record (using same key as previous one)

	// node 0 uploads new file
	fileC := []byte("NEW NEW NEW lorem ipsum dolor sit ametlorem ipsum dolor sit ametlorem ipsum dolo")
	mhC, err := nodes[0].UploadDHT(bytes.NewBuffer(fileC))
	require.NoError(t, err)

	// node 0 edits pointer record by creating a new pointer record using same private key
	// note that only owner of private key can edit a record, otherwise validation will fail
	editedRecord, err := nodes[0].CreatePointerRecord(privateKey, mhC, record.Sequence+1, 10)
	require.NoError(t, err)

	// node 0 publishes upadted record
	editedRecordHash, err := nodes[0].PublishPointerRecord(editedRecord)
	require.NoError(t, err)

	// Pointer address stays the same
	require.Equal(t, recordHash, editedRecordHash)

	time.Sleep(time.Second * 2) // allow record to be fully stored in DHT

	// everyone can fetch new file under old address
	for i := range nodes {
		fetchedRecord, ok := nodes[i].FetchPointerRecord(recordHash)
		require.Equal(t, true, ok)
		require.Equal(t, mhC, fetchedRecord.Value)

		// everyone can download edited file
		res, err := nodes[i].DownloadDHT(fetchedRecord.Value)
		require.NoError(t, err)
		require.Equal(t, fileC, res)
	}
}

func Test_MUTABLE_KademliaTagFolderUnderPointerRecord(t *testing.T) {
	numNodes := uint(10)
	if numNodes < 2 {
		return
	}

	transp := channel.NewTransport()
	nodes := make([]z.TestNode, numNodes)

	for i := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(numNodes))
		defer node.Stop()

		nodes[i] = node
	}

	// bootstrap all nodes
	for i := uint(1); i < numNodes; i++ {
		nodes[i].Bootstrap(nodes[0].GetAddr())
	}
	println("Bootstraped ", numNodes, " nodes")
	time.Sleep(time.Second * 1)

	fileNames := make([]string, 0)
	// upload multiple files that will be in the folder
	for i := 0; i < 2; i++ {
		// node 0 uploads file
		fileB := []byte(fmt.Sprintf("File %d content", i))
		mhB, err := nodes[0].UploadDHT(bytes.NewBuffer(fileB))
		require.NoError(t, err)

		// node 0 generates pointer record
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		record, err := nodes[0].CreatePointerRecord(privateKey, mhB, 0, 100) // sequence 0
		require.NoError(t, err)
		// node 0 publishes record
		recordHash, err := nodes[0].PublishPointerRecord(record)
		require.NoError(t, err)

		fileName := fmt.Sprintf("File%d", i)
		err = nodes[0].Tag(fileName, recordHash)
		require.NoError(t, err)
		time.Sleep(time.Second * 2)
		fileNames = append(fileNames, fileName)
	}

	// also add some subfolders
	for i := 0; i < 3; i++ {
		// node 0 generates pointer record
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		record, err := nodes[0].CreateFolderPointerRecord(privateKey, make([]string, 0), 0, 100) // sequence 0
		require.NoError(t, err)
		// node 0 publishes record
		recordHash, err := nodes[0].PublishPointerRecord(record)
		require.NoError(t, err)

		folderName := fmt.Sprintf("Folder%d", i)
		err = nodes[0].Tag(folderName, recordHash)
		require.NoError(t, err)
		time.Sleep(time.Second * 2)

		fileNames = append(fileNames, folderName)
	}

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	record, err := nodes[0].CreateFolderPointerRecord(privateKey, fileNames, 0, 100) // sequence 0
	require.NoError(t, err)

	// node 0 publishes record
	recordHash, err := nodes[0].PublishPointerRecord(record)
	require.NoError(t, err)

	err = nodes[0].Tag("topFolder", recordHash)
	require.NoError(t, err)
	time.Sleep(time.Second * 5)

	// everyone can fetch pointer record
	for i := range nodes {
		ad := nodes[i].Resolve("topFolder")
		require.Equal(t, recordHash, ad)
		fetchedRecord, ok := nodes[i].FetchPointerRecord(ad)
		require.Equal(t, true, ok)
		require.Equal(t, "", fetchedRecord.Value)
		require.Equal(t, 5, len(fetchedRecord.Links))

		// everyone can download file pointer inside folder of this record
		addr := nodes[i].Resolve("File0")
		newFetchedRecord, ok := nodes[i].FetchPointerRecord(addr)
		require.Equal(t, true, ok)
		require.Equal(t, false, nodes[i].IsFolderRecord(newFetchedRecord))
		res, err := nodes[i].DownloadDHT(newFetchedRecord.Value)
		require.NoError(t, err)
		require.Equal(t, []byte("File 0 content"), res)

		// everyone can access subfolders
		addrFolder := nodes[i].Resolve("Folder0")
		fetchedFolder, ok := nodes[i].FetchPointerRecord(addrFolder)
		require.Equal(t, true, nodes[i].IsFolderRecord(fetchedFolder))
		require.Equal(t, true, ok)
		require.Equal(t, "", fetchedFolder.Value)
	}
}

// upload file and pointer record associated with it, tag pointer record,
// edit file and publish new pointer record under same address
// fetch edited file under same address as before
func Test_MUTABLE_KademliaTagPointerRecord(t *testing.T) {
	numNodes := uint(10)
	if numNodes < 2 {
		return
	}

	transp := channel.NewTransport()
	nodes := make([]z.TestNode, numNodes)

	for i := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(numNodes))
		defer node.Stop()

		nodes[i] = node
	}

	// bootstrap all nodes
	for i := uint(1); i < numNodes; i++ {
		nodes[i].Bootstrap(nodes[0].GetAddr())
	}
	println("Bootstraped ", numNodes, " nodes")
	time.Sleep(time.Second * 1)

	// node 0 uploads file
	fileB := []byte("A lorem ipsum dolor sit ametlorem ipsum dolor sit ametlorem ipsum dolo")
	mhB, err := nodes[0].UploadDHT(bytes.NewBuffer(fileB))
	require.NoError(t, err)

	// node 0 generates pointer record
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	record, err := nodes[0].CreatePointerRecord(privateKey, mhB, 0, 100) // sequence 0
	require.NoError(t, err)
	// node 0 publishes record
	recordHash, err := nodes[0].PublishPointerRecord(record)
	require.NoError(t, err)

	err = nodes[0].Tag("MyFile", recordHash)
	require.NoError(t, err)
	time.Sleep(time.Second * 2)

	// everyone can fetch pointer record
	for i := range nodes {
		addr := nodes[i].Resolve("MyFile")
		require.Equal(t, recordHash, addr)
		fetchedRecord, ok := nodes[i].FetchPointerRecord(addr)
		require.Equal(t, true, ok)
		require.Equal(t, mhB, fetchedRecord.Value)

		// everyone can download file pointer to by record
		res, err := nodes[i].DownloadDHT(fetchedRecord.Value)
		require.NoError(t, err)
		require.Equal(t, fileB, res)
	}

	//Edit file: aka. upload new file, and publish updated pointer record (using same key as previous one)

	// node 0 uploads new file
	fileC := []byte("NEW NEW NEW lorem ipsum dolor sit ametlorem ipsum dolor sit ametlorem ipsum dolo")
	mhC, err := nodes[0].UploadDHT(bytes.NewBuffer(fileC))
	require.NoError(t, err)

	// node 0 edits pointer record by creating a new pointer record using same private key
	// note that only owner of private key can edit a record, otherwise validation will fail
	editedRecord, err := nodes[0].CreatePointerRecord(privateKey, mhC, record.Sequence+1, 10)
	require.NoError(t, err)

	// node 0 publishes upadted record
	editedRecordHash, err := nodes[0].PublishPointerRecord(editedRecord)
	require.NoError(t, err)

	// Pointer address stays the same
	require.Equal(t, recordHash, editedRecordHash)

	time.Sleep(time.Second * 3) // allow record to be fully stored in DHT

	// everyone can fetch new file under old address
	for i := range nodes {
		addr := nodes[i].Resolve("MyFile")
		require.Equal(t, editedRecordHash, addr)
		fetchedRecord, ok := nodes[i].FetchPointerRecord(addr)
		require.Equal(t, true, ok)
		require.Equal(t, mhC, fetchedRecord.Value)

		// everyone can download edited file
		res, err := nodes[i].DownloadDHT(fetchedRecord.Value)
		require.NoError(t, err)
		require.Equal(t, fileC, res)
	}
}

// one node stores, everyone should be able to fetch
func Test_MUTABLE_KademliaStressTest1(t *testing.T) {
	numNodes := 30
	if numNodes < 2 {
		return
	}

	transp := channel.NewTransport()
	nodes := make([]z.TestNode, numNodes)

	for i := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
		defer node.Stop()

		nodes[i] = node
	}

	// bootstrap all nodes
	for i := 1; i < numNodes; i++ {
		nodes[i].Bootstrap(nodes[0].GetAddr())
	}
	time.Sleep(time.Second * 1)

	nodes[0].Store("key1", []byte("val1"))
	time.Sleep(time.Second * 2)

	// one node stores, everyone should be able to fetch it
	for i := range nodes {
		val1, ok := nodes[i].FindValue("key1")
		// queries other nodes to fetch value
		require.Equal(t, true, ok)
		require.Equal(t, "val1", string(val1[:]))
	}
}

// everyone stores k {key,val} pairs, everyone should be able to fetch all stored pairs
func Test_MUTABLE_KademliaStressTest2(t *testing.T) {
	numNodes := 30
	numStores := 3
	if numNodes < 2 {
		return
	}

	transp := channel.NewTransport()
	nodes := make([]z.TestNode, numNodes)

	for i := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
		defer node.Stop()

		nodes[i] = node
	}

	// bootstrap all nodes
	for i := 1; i < numNodes; i++ {
		nodes[i].Bootstrap(nodes[0].GetAddr())
	}
	time.Sleep(time.Second * 1)

	for i := range nodes {
		for j := 0; j < numStores; j++ {
			key := "key" + fmt.Sprint(i) + fmt.Sprint(j)
			val := "val" + fmt.Sprint(i) + fmt.Sprint(j)
			nodes[i].Store(key, []byte(val))
			// node i stores {key,val} j as {'keyij','valij'}
		}
	}
	time.Sleep(time.Second * 2)

	// everyone should be able to fetch all stored key,val pairs
	for k := range nodes {
		// for each stored message
		for i := range nodes {
			for j := 0; j < numStores; j++ {
				key := "key" + fmt.Sprint(i) + fmt.Sprint(j)
				val := "val" + fmt.Sprint(i) + fmt.Sprint(j)

				val1, ok := nodes[k].FindValue(key)
				// queries other nodes to fetch value
				require.Equal(t, true, ok)
				require.Equal(t, val, string(val1[:]))
			}
		}
	}
}

// everyone stores k {key,val} pairs, everyone should be able to fetch all stored pairs
// everyone overwrites the {key,val} pairs they written, everyone should be able to fetch new pairs
func Test_MUTABLE_KademliaStressTest3(t *testing.T) {
	numNodes := 30
	numStores := 3
	if numNodes < 2 {
		return
	}

	transp := channel.NewTransport()
	nodes := make([]z.TestNode, numNodes)

	for i := range nodes {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
		defer node.Stop()

		nodes[i] = node
	}

	// bootstrap all nodes
	for i := 1; i < numNodes; i++ {
		nodes[i].Bootstrap(nodes[0].GetAddr())
	}
	time.Sleep(time.Second * 1)

	for i := range nodes {
		for j := 0; j < numStores; j++ {
			key := "key" + fmt.Sprint(i) + fmt.Sprint(j)
			val := "val" + fmt.Sprint(i) + fmt.Sprint(j)
			nodes[i].Store(key, []byte(val))
			// node i stores {key,val} j as {'keyij','valij'}
		}
	}
	time.Sleep(time.Second * 3)

	// everyone should be able to fetch all stored key,val pairs
	for k := range nodes {
		// for each stored message
		for i := range nodes {
			for j := 0; j < numStores; j++ {
				key := "key" + fmt.Sprint(i) + fmt.Sprint(j)
				val := "val" + fmt.Sprint(i) + fmt.Sprint(j)

				val1, ok := nodes[k].FindValue(key)
				// queries other nodes to fetch value
				require.Equal(t, true, ok)
				require.Equal(t, val, string(val1[:]))
			}
		}
	}

	for i := range nodes {
		for j := 0; j < numStores; j++ {
			key := "key2" + fmt.Sprint(i) + fmt.Sprint(j)
			val := "val2" + fmt.Sprint(i) + fmt.Sprint(j)
			nodes[i].Store(key, []byte(val))
			// node i stores {key,val} j as {'keyij','valij'}
		}
	}
	time.Sleep(time.Second * 2)

	// everyone should be able to fetch all stored key,val pairs
	for k := range nodes {
		// for each stored message
		for i := range nodes {
			for j := 0; j < numStores; j++ {
				key := "key2" + fmt.Sprint(i) + fmt.Sprint(j)
				val := "val2" + fmt.Sprint(i) + fmt.Sprint(j)

				val1, ok := nodes[k].FindValue(key)
				// queries other nodes to fetch value
				require.Equal(t, true, ok)
				require.Equal(t, val, string(val1[:]))
			}
		}
	}
}
