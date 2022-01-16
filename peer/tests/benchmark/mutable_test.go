package benchmark

import (
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/peer/impl"
	"go.dedis.ch/cs438/transport/channel"
)

var peerFac peer.Factory = impl.NewPeer

// Add multiple websites in environment of many websites and assess performance
func Benchmark_MUTABLE_KademliaStressTestAddWebsiteInMultiWebsiteEnv(b *testing.B) {
	numNodes := uint(5)
	if numNodes < 2 {
		return
	}

	tmpFolder := filepath.Join(os.TempDir(), "tmpFolder")
	os.RemoveAll(tmpFolder)
	os.Mkdir(tmpFolder, 0777)
	os.Mkdir(filepath.Join(tmpFolder, "subfolder1"), 0777)
	os.Mkdir(filepath.Join(tmpFolder, "subfolder2"), 0777)
	ioutil.WriteFile(filepath.Join(tmpFolder, "test.txt"), []byte("File test content"), 0666)
	ioutil.WriteFile(filepath.Join(tmpFolder, "subfolder1", "test2.txt"), []byte("File test2 content"), 0666)

	testSize := []int{1, 2, 3, 4, 5, 10, 20}
	for _, size := range testSize {
		transp := channel.NewTransport()
		nodes := make([]z.BenchmarkTestNode, numNodes)

		for i := range nodes {
			node := z.NewBenchmarkTestNode(b, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(numNodes))
			nodes[i] = node
		}

		// bootstrap all nodes
		for i := uint(1); i < numNodes; i++ {
			nodes[i].Bootstrap(nodes[0].GetAddr())
		}
		println("Bootstraped ", numNodes, " nodes")
		time.Sleep(time.Second * 1)
		b.Run(fmt.Sprintf("size = %d", size), func(b *testing.B) {

			for i := range nodes {
				for j := 0; j < size; j++ {
					privateKey, _ := rsa.GenerateKey(rand.Reader, 2048)
					recordHash, _ := nodes[i].CreateAndPublishFolderRecord(tmpFolder, "tmpFolder", privateKey, 0, 10)

					nodes[i].Tag(fmt.Sprintf("website%d %d", i, j), recordHash)

					for {
						if nodes[i].Resolve(fmt.Sprintf("website%d %d", i, j)) != "" {
							break
						}
					}
				}
			}
		})
		for i := range nodes {
			nodes[i].Stop()
		}
	}
}

// Fetch website in environment of many websites and assess performance
func Benchmark_MUTABLE_KademliaStressTestFetchFullWebsiteInMultiWebsiteEnv(b *testing.B) {
	numNodes := uint(5)
	if numNodes < 2 {
		return
	}

	tmpFolder := filepath.Join(os.TempDir(), "tmpFolder")
	os.RemoveAll(tmpFolder)
	os.Mkdir(tmpFolder, 0777)
	os.Mkdir(filepath.Join(tmpFolder, "subfolder1"), 0777)
	os.Mkdir(filepath.Join(tmpFolder, "subfolder2"), 0777)
	ioutil.WriteFile(filepath.Join(tmpFolder, "test.txt"), []byte("File test content"), 0666)
	ioutil.WriteFile(filepath.Join(tmpFolder, "subfolder1", "test2.txt"), []byte("File test2 content"), 0666)

	testSize := []int{1, 2, 3, 4, 5, 10, 20}
	for _, size := range testSize {
		transp := channel.NewTransport()
		nodes := make([]z.BenchmarkTestNode, numNodes)

		for i := range nodes {
			node := z.NewBenchmarkTestNode(b, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(numNodes))
			nodes[i] = node
		}

		// bootstrap all nodes
		for i := uint(1); i < numNodes; i++ {
			nodes[i].Bootstrap(nodes[0].GetAddr())
		}
		println("Bootstraped ", numNodes, " nodes")
		time.Sleep(time.Second * 1)

		for i := range nodes {
			for j := 0; j < size; j++ {
				privateKey, _ := rsa.GenerateKey(rand.Reader, 2048)
				recordHash, _ := nodes[i].CreateAndPublishFolderRecord(tmpFolder, "tmpFolder", privateKey, 0, 10)

				nodes[i].Tag(fmt.Sprintf("website%d %d", i, j), recordHash)

				for {
					if nodes[i].Resolve(fmt.Sprintf("website%d %d", i, j)) != "" {
						break
					}
				}
			}
		}
		b.Run(fmt.Sprintf("size = %d", size), func(b *testing.B) {
			mh := nodes[0].Resolve(fmt.Sprintf("website%d %d", 2, 4))
			record, _ := nodes[0].FetchPointerRecord(mh)
			tmpFolderResult := filepath.Join(os.TempDir(), "tmpFolderResult")
			os.RemoveAll(tmpFolderResult)
			nodes[0].ReconstructFolderFromRecord(tmpFolderResult, record, false)
		})
		for i := range nodes {
			nodes[i].Stop()
		}
	}
}
