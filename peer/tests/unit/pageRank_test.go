package unit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
)

// Creates and validates a PointerRecord object using a RSA keypair, validation should not throw error
func Test_PAGERANK_basic_graph(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")

	node1.AddLink("A", "B")
	node1.AddLink("C", "B")
	node1.AddLink("D", "B")

	ranking := node1.GetRanking()

	require.Equal(t, 4, len(ranking))
	require.Equal(t, ranking["A"], ranking["C"])
	require.Equal(t, ranking["A"], ranking["D"])
	require.Greater(t, ranking["B"], ranking["D"])

}

func Test_PAGERANK_node_to_node(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")

	node1.AddLink("A", "A")

	ranking := node1.GetRanking()

	require.Equal(t, 0, len(ranking))

}

func Test_PAGERANK_2_peers(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(2), z.WithPaxosID(1))
	defer node1.Stop()
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(2), z.WithPaxosID(2))
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())

	err := node1.AddLink("A", "C")

	require.NoError(t, err)

	time.Sleep(time.Second * 1)

	ranking1 := node1.GetRanking()
	ranking2 := node2.GetRanking()
	require.Equal(t, 2, len(ranking2))
	require.Equal(t, ranking1, ranking2)

}

func Test_PAGERANK_stress(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(5), z.WithPaxosID(1))
	defer node1.Stop()
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(5), z.WithPaxosID(2))
	defer node2.Stop()
	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(5), z.WithPaxosID(3))
	defer node2.Stop()
	node4 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(5), z.WithPaxosID(4))
	defer node2.Stop()
	node5 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithTotalPeers(5), z.WithPaxosID(5))
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())
	node1.AddPeer(node4.GetAddr())
	node1.AddPeer(node5.GetAddr())

	node2.AddPeer(node3.GetAddr())
	node2.AddPeer(node4.GetAddr())
	node2.AddPeer(node5.GetAddr())

	node3.AddPeer(node4.GetAddr())
	node3.AddPeer(node5.GetAddr())

	node4.AddPeer(node5.GetAddr())

	err := node1.AddLink("A", "C")
	require.NoError(t, err)

	err = node2.AddLink("B", "C")
	require.NoError(t, err)

	err = node3.AddLink("C", "B")
	require.NoError(t, err)

	err = node4.AddLink("D", "B")
	require.NoError(t, err)

	err = node5.AddLink("A", "B")
	require.NoError(t, err)

	time.Sleep(time.Second * 2)

	ranking1 := node1.GetRanking()
	ranking2 := node2.GetRanking()
	ranking3 := node3.GetRanking()
	ranking4 := node4.GetRanking()
	ranking5 := node5.GetRanking()
	require.Equal(t, 4, len(ranking1))
	require.Equal(t, ranking1, ranking2)
	require.Equal(t, ranking1, ranking3)
	require.Equal(t, ranking1, ranking4)
	require.Equal(t, ranking1, ranking5)

}
