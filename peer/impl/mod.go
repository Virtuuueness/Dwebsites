package impl

import (
	"crypto"
	"crypto/sha1"
	"math/big"
	"sort"

	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// NewPeer creates a new peer
func NewPeer(conf peer.Configuration) peer.Peer {
	table := make(peer.RoutingTable)
	table[conf.Socket.GetAddress()] = conf.Socket.GetAddress()

	n := &node{
		conf:             conf,
		running:          false,
		stop:             make(chan bool),
		socketTimeout:    time.Second * 1,
		router:           SafeRoutingTable{table: table},
		peers:            ThreadSafeSet{peersSet: *NewSet()},
		rumorHistory:     RumorHistory{rumorMap: make(map[string][]types.Rumor)},
		ackChannels:      AckChannels{channelsMap: make(map[string]chan bool)},
		dataChannels:     DataChannels{channelsMap: make(map[string]chan *types.DataReplyMessage)},
		searchChannels:   SearchChannels{channelsMap: make(map[string]chan string)},
		peersCatalog:     PeersCatalog{catalog: make(peer.Catalog)},
		currStep:         SafeUint{val: 0},
		maxID:            SafeUint{val: 0},
		acceptedProposal: AcceptedProposal{acceptedID: 0, acceptedValue: nil},
		proposer: Proposer{
			currID:                        0,
			promiseThresholdReached:       make(chan bool, 10),
			acceptThresholdReached:        make(chan types.PaxosValue, 10),
			isListeningToAcceptThreshold:  false,
			isListeningToPromiseThreshold: false,
			paxosPhase:                    0,
			recvPromises:                  0,
			highestAcceptID:               0,
			highestAcceptVal:              nil,
			acceptedVals:                  make(map[string]*Set),
			tlcBroadcasted:                false,
		},
		tlcCount:         TlcCounter{counter: make(map[uint][]*types.TLCMessage)},
		tagBlocked:       SafeBool{val: false},
		consensusReached: SafeBool{val: false},
		tagChan:          make(chan types.PaxosValue),

		// Kademlia DHT
		kademliaRouter:   KademliaRoutingTable{KBuckets: make([]KBucket, 160)},
		id:               *big.NewInt(0), // later set to sha1(address)
		localHashMap:     SafeByteMap{byteMap: make(map[string][]byte)},
		contactsChannels: ContactsChannels{channelsMap: make(map[string]chan *types.FindNodeReply)},
		valueChannels:    ValueChannels{channelsMap: make(map[string]chan *types.FindValueReply)},

		// Page rank
		pageRank: pageRank{nodes: make(map[string]struct{}),
			incommingLinks:      make(map[string][]string),
			nbOfOutcommingLinks: make(map[string]int)},
		pageRankRanking:          ranking{ranking: make(map[string]float64)},
		pageRankAcceptedProposal: AcceptedProposal{acceptedID: 0, acceptedValue: nil},
		pageRankcurrStep:         SafeUint{val: 0},
		pageRankProposer: Proposer{
			currID:                        0,
			promiseThresholdReached:       make(chan bool, 10),
			acceptThresholdReached:        make(chan types.PaxosValue, 10), //PageRankPaosValue casted into PaxosValue to avoid useless code duplication
			isListeningToAcceptThreshold:  false,
			isListeningToPromiseThreshold: false,
			paxosPhase:                    0,
			recvPromises:                  0,
			highestAcceptID:               0,
			highestAcceptVal:              nil,
			acceptedVals:                  make(map[string]*Set),
			tlcBroadcasted:                false,
		},
		pageRankTlcCount:         PageRankTlcCounter{counter: make(map[uint][]*types.PageRankTLCMessage)}, //PageRankTLCMessage casted into TLCMessage to avoid useless code duplication
		pageRankBlocked:          SafeBool{val: false},
		pageRankConsensusReached: SafeBool{val: false},
		pageRankChan:             make(chan types.PageRankPaxosValue),
		pageRankMaxID:            SafeUint{val: 0},

		//Search Engine
		searchEngineResponseStorage: SearchEngineResponseStorage{responses: make(map[string]map[string]struct{})},
	}

	conf.MessageRegistry.RegisterMessageCallback(&types.ChatMessage{}, n.ChatMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.RumorsMessage{}, n.RumorsMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.StatusMessage{}, n.StatusMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.AckMessage{}, n.AckMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.EmptyMessage{}, n.EmptyMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.PrivateMessage{}, n.PrivateMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.DataReplyMessage{}, n.DataReplyMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.DataRequestMessage{}, n.DataRequestMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.SearchReplyMessage{}, n.SearchReplyMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.SearchRequestMessage{}, n.SearchRequestMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.PaxosPrepareMessage{}, n.PaxosPrepareMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.PaxosProposeMessage{}, n.PaxosProposeMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.PaxosPromiseMessage{}, n.PaxosPromiseMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.PaxosAcceptMessage{}, n.PaxosAcceptMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.TLCMessage{}, n.TLCMessageExec)

	// Kademlia DHT
	hash := sha1.Sum([]byte(conf.Socket.GetAddress()))
	n.id.SetBytes(hash[:])
	conf.MessageRegistry.RegisterMessageCallback(&types.FindNodeReply{}, n.FindNodeReplyExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.FindNodeRequest{}, n.FindNodeRequestExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.FindValueReply{}, n.FindValueReplyExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.FindValueRequest{}, n.FindValueRequestExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.StoreRequest{}, n.StoreRequestExec)

	// PageRank
	conf.MessageRegistry.RegisterMessageCallback(&types.PageRankPaxosPrepareMessage{}, n.PageRankPaxosPrepareMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.PageRankPaxosProposeMessage{}, n.PageRankPaxosProposeMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.PageRankPaxosPromiseMessage{}, n.PageRankPaxosPromiseMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.PageRankPaxosAcceptMessage{}, n.PageRankPaxosAcceptMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.PageRankTLCMessage{}, n.PageRankTLCMessageExec)

	// SearchEngine
	conf.MessageRegistry.RegisterMessageCallback(&types.SearchEngineReplyMessage{}, n.SearchEngineReplyMessageExec)
	conf.MessageRegistry.RegisterMessageCallback(&types.SearchEngineRequestMessage{}, n.SearchEngineRequestMessageExec)

	return n
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer

	conf           peer.Configuration
	running        bool
	stop           chan bool
	socketTimeout  time.Duration
	router         SafeRoutingTable
	peers          ThreadSafeSet
	rumorHistory   RumorHistory
	ackChannels    AckChannels
	dataChannels   DataChannels
	searchChannels SearchChannels
	peersCatalog   PeersCatalog
	currStep       SafeUint
	// Acceptor fields
	maxID SafeUint // FIXME: maybe map[TLC step] -> maxID
	// maybe map[TLC step/paxos instance] -> paxosInstanceData{maxID, acceptedID, acceptedVal, bool accepted}
	acceptedProposal AcceptedProposal
	proposer         Proposer // contains proposer fields
	tlcCount         TlcCounter
	tagBlocked       SafeBool
	tagChan          chan types.PaxosValue
	consensusReached SafeBool

	// Kademlia DHT
	kademliaRouter   KademliaRoutingTable
	id               big.Int
	localHashMap     SafeByteMap
	contactsChannels ContactsChannels
	valueChannels    ValueChannels

	// PageRank
	pageRank                 pageRank
	pageRankRanking          ranking
	pageRankcurrStep         SafeUint
	pageRankAcceptedProposal AcceptedProposal
	pageRankProposer         Proposer
	pageRankTlcCount         PageRankTlcCounter
	pageRankBlocked          SafeBool
	pageRankChan             chan types.PageRankPaxosValue
	pageRankConsensusReached SafeBool
	pageRankMaxID            SafeUint

	// SearchEngine
	searchEngineResponseStorage SearchEngineResponseStorage
}

// Broadcasts HeartbeatMessage at regular interval
func (n *node) StartHeartbeat() {
	if n.conf.HeartbeatInterval == 0 {
		return
	}

	heartbeatMsg, err := n.conf.MessageRegistry.MarshalMessage(types.EmptyMessage{})
	if err != nil {
		log.Error().Msgf("<[peer.Peer.StartHeartbeat] Marshal error >: <%s>", err.Error())
		return
	}

	ticker := time.NewTicker(n.conf.HeartbeatInterval)
	for {
		select {
		case <-n.stop:
			ticker.Stop()
			return
		case <-ticker.C:
			err = n.Broadcast(heartbeatMsg)
			if err != nil {
				log.Error().Msgf("<[peer.Peer.StartHeartbeat] Broadcast >: <%s>", err.Error())
			}
		}
	}

}

// sends StatusMessage to one random neighbour at regular interval
func (n *node) StartAntiEntropy() {
	if n.conf.AntiEntropyInterval == 0 {
		return
	}

	ticker := time.NewTicker(n.conf.AntiEntropyInterval)
	for {
		select {
		case <-n.stop:
			ticker.Stop()
			return
		case <-ticker.C:
			statusMsg := types.StatusMessage(n.rumorHistory.GetView())
			dest := n.peers.GetRandomPeer(NewSet(n.conf.Socket.GetAddress()))
			if dest == "" {
				continue
			}

			err := n.DirectSend(dest, statusMsg)
			if err != nil {
				log.Error().Msgf("<[peer.Peer.Start] Send to random neighbour error>: <%s>", err.Error())
			}
		}
	}
}

// Start implements peer.Service
func (n *node) Start() error {
	n.running = true

	if n.conf.HeartbeatInterval != 0 { // Heartbeat
		heartbeatMsg, err := n.conf.MessageRegistry.MarshalMessage(types.EmptyMessage{})
		if err != nil {
			log.Error().Msgf("<[peer.Peer.Start] Heartbeat Marshal error >: <%s>", err.Error())
		} else {
			err = n.Broadcast(heartbeatMsg)
			if err != nil {
				log.Error().Msgf("<[peer.Peer.Start] Heartbeat Broadcast error >: <%s>", err.Error())
			}
		}

		go n.StartHeartbeat()
	}

	if n.conf.AntiEntropyInterval != 0 {
		go n.StartAntiEntropy()
	}

	go func() { // recv loop
		for {
			select {
			case <-n.stop:
				return
			default:
				pkt, err := n.conf.Socket.Recv(n.socketTimeout)
				if errors.Is(err, transport.TimeoutErr(0)) {
					continue
				}

				go func() { // process packet
					dest := pkt.Header.Destination

					if dest == n.conf.Socket.GetAddress() { // for this node
						err := n.conf.MessageRegistry.ProcessPacket(pkt)
						if err != nil {
							log.Error().Msgf("<[peer.Peer.Start] ProcessPacket error>: <%s>", err.Error())
						}
					} else { // relay
						pkt.Header.RelayedBy = n.conf.Socket.GetAddress()
						pkt.Header.TTL--

						relayTo := n.router.Get(dest)
						if relayTo == "" {
							log.Error().Msg("<[peer.Peer.Start] Relay error>: <destination not found in routing table>")
							return
						}

						err := n.conf.Socket.Send(relayTo, pkt, n.socketTimeout)
						if err != nil {
							log.Error().Msgf("<[peer.Peer.Start] Relay error>: <%s>", err.Error())
						}
					}
				}()
			}
		}
	}()
	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	if n.running {
		n.stop <- true
	}
	n.running = false
	return nil
}

// Marshals and sends given message directly to dest using Socket.Send
// (doesn't check routing table)
func (n *node) DirectSend(dest string, msg types.Message) error {
	transportMsg, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return err
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
	pkt := transport.Packet{Header: &header, Msg: &transportMsg}
	return n.conf.Socket.Send(dest, pkt, n.socketTimeout)
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
	pkt := transport.Packet{Header: &header, Msg: &msg}

	relayTo := n.router.Get(dest)
	if relayTo == "" {
		return errors.New("[Unicast] peer not found")
	}
	return n.conf.Socket.Send(relayTo, pkt, n.socketTimeout)
}

// sends rumor to given dest
// returns pktID of the packet sent through the socket(used for ACK)
func (n *node) SendRumor(dest string, rumor types.Rumor) (string, error) {
	rumorsMsg := types.RumorsMessage{Rumors: []types.Rumor{rumor}}
	transportRumorsMsg, err := n.conf.MessageRegistry.MarshalMessage(rumorsMsg)
	if err != nil {
		return "", err
	}

	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(),
		dest, 0)
	pkt := transport.Packet{Header: &header, Msg: &transportRumorsMsg}

	err = n.conf.Socket.Send(dest, pkt, n.socketTimeout)
	if err != nil {
		return "", err
	}

	return pkt.Header.PacketID, nil
}

func (n *node) Broadcast(msg transport.Message) error {
	rumor := n.rumorHistory.AddNewRumor(msg, n.conf.Socket.GetAddress())

	go func() { // process rumor
		header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(),
			n.conf.Socket.GetAddress(), 0)
		pkt := transport.Packet{Header: &header, Msg: &msg}
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if err != nil {
			log.Error().Msgf("<[peer.Peer.Broadcast] Process rumor >: <%s> ", err)
		}
	}()

	dest := n.peers.GetRandomPeer(NewSet(n.conf.Socket.GetAddress()))
	if dest == "" {
		log.Info().Msgf("[peer.peer.Broadcast] No random peers to send to")
		return nil
	}

	pktID, err := n.SendRumor(dest, rumor)
	if err != nil {
		return err
	}

	go n.WaitForAck(pktID, rumor, NewSet(n.conf.Socket.GetAddress(), dest))

	return nil
}

// Waits for Ack of pktID. If timeout is reached,
// sends rumor to one random peer outside the ignorePeers set
func (n *node) WaitForAck(pktID string, rumor types.Rumor, ignorePeers *Set) {
	channel := n.ackChannels.Set(pktID, make(chan bool, 1))
	defer close(channel)
	defer n.ackChannels.Delete(pktID)

	if n.conf.AckTimeout == 0 {
		<-channel // wait forever
	} else {
		timer := time.NewTimer(n.conf.AckTimeout)
		defer timer.Stop()

		select {
		case <-channel: // Ack received!
			return
		case <-timer.C:
			newDest := n.peers.GetRandomPeer(ignorePeers)
			if newDest == "" {
				log.Info().Msgf("[peer.peer.Broadcast][Ack] No random peers to send to")
				return
			}

			newPktID, err := n.SendRumor(newDest, rumor)
			if err != nil {
				log.Error().Msgf("<[peer.Peer.Broadcast][Ack] Send to random neighbour error>: %s <%s>",
					newDest, err.Error())
				return
			}

			n.WaitForAck(newPktID, rumor, ignorePeers.Add(newDest))
		}
	}
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	for _, s := range addr {
		n.router.Set(s, s)
		n.peers.Add(s)
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	copyTable := make(peer.RoutingTable)

	n.router.Lock()
	for k, v := range n.router.table {
		copyTable[k] = v
	}
	n.router.Unlock()

	return copyTable
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	if relayAddr == "" {
		n.router.Delete(origin)
	} else {
		n.router.Set(origin, relayAddr)
	}
}

func (n *node) Upload(data io.Reader) (metahash string, err error) {
	chunk := make([]byte, n.conf.ChunkSize)
	var metafile []byte
	var metafileKey []byte
	h := crypto.SHA256.New()

	for {
		num_bytes, err := data.Read(chunk)
		if err == io.EOF {
			break
		}

		_, err = h.Write(chunk[:num_bytes])
		if err != nil {
			return "", err
		}

		chunkHash := h.Sum(nil)
		chunkKey := hex.EncodeToString(chunkHash)

		slice_copy := make([]byte, num_bytes)
		copy(slice_copy, chunk[:num_bytes])

		n.conf.Storage.GetDataBlobStore().Set(chunkKey, slice_copy)

		metafileKey = append(metafileKey, chunkHash...)

		// append to metafile
		if len(metafile) == 0 {
			metafile = append(metafile, chunkKey...)
		} else {
			metafile = append(metafile, []byte(peer.MetafileSep)...)
			metafile = append(metafile, chunkKey...)
		}

		h.Reset()
	}

	_, err = h.Write(metafileKey)
	if err != nil {
		return "", err
	}
	metahash = hex.EncodeToString(h.Sum(nil))
	n.conf.Storage.GetDataBlobStore().Set(metahash, metafile)

	return metahash, nil
}

func (n *node) GetCatalog() peer.Catalog {
	n.peersCatalog.Lock()
	defer n.peersCatalog.Unlock()
	copy := make(peer.Catalog)

	for chunkKey, set := range n.peersCatalog.catalog {
		copy[chunkKey] = make(map[string]struct{})
		for peer := range set {
			copy[chunkKey][peer] = struct{}{}
		}
	}

	return copy
}

func (n *node) UpdateCatalog(key string, peer string) {
	n.peersCatalog.Lock()
	defer n.peersCatalog.Unlock()

	if n.peersCatalog.catalog[key] == nil {
		n.peersCatalog.catalog[key] = make(map[string]struct{})
	}
	n.peersCatalog.catalog[key][peer] = struct{}{}
}

// Waits for reply whose ID is associated with given DataRequestMessage.
// Uses back-off strategy if peer is unresponsive.
func (n *node) WaitForReply(dataReqMsg types.DataRequestMessage, dest string) (*types.DataReplyMessage, error) {
	channel := n.dataChannels.Set(dataReqMsg.RequestID, make(chan *types.DataReplyMessage, 1))
	defer close(channel)
	defer n.ackChannels.Delete(dataReqMsg.RequestID)

	transportDataReqMsg, err := n.conf.MessageRegistry.MarshalMessage(dataReqMsg)
	if err != nil {
		return nil, err
	}

	var r uint = 0
	var f uint = 1

	for r < n.conf.BackoffDataRequest.Retry {
		timer := time.NewTimer(n.conf.BackoffDataRequest.Initial * time.Duration(f))

		select {
		case reply := <-channel:
			timer.Stop()
			return reply, nil
		case <-timer.C:
			// retransmit
			err = n.Unicast(dest, transportDataReqMsg)
			if err != nil {
				return nil, err
			}
		}
		timer.Stop()
		r++
		f *= n.conf.BackoffDataRequest.Factor
	}

	return nil, fmt.Errorf("[peer.WaitForReply] backoff timeout: can't reach peer %s", dest)
}

// Fetches value associated with given key from a random peer known to have it.
// Uses back-off strategy if peer is unresponsive.
func (n *node) FetchFromPeers(key string) ([]byte, error) {
	dest := n.peersCatalog.GetRandomPeer(key)
	if dest == "" {
		return nil, fmt.Errorf("[peer.FetchFromPeers] no known peers hold key %s", key)
	}

	// Create DataRequestMessage
	dataReqMsg := types.DataRequestMessage{RequestID: xid.New().String(), Key: key}
	transportDataReqMsg, err := n.conf.MessageRegistry.MarshalMessage(dataReqMsg)
	if err != nil {
		return nil, err
	}

	// Send DataRequestMessage to random peer from Catalog
	err = n.Unicast(dest, transportDataReqMsg)
	if err != nil {
		return nil, err
	}

	reply, err := n.WaitForReply(dataReqMsg, dest)
	if err != nil {
		return nil, err
	}

	if len(reply.Value) == 0 {
		return nil, fmt.Errorf("[peer.FetchFromPeers] remote peer returned empty value")
	}

	if reply.Key != key {
		return nil, fmt.Errorf(`[peer.FetchFromPeers] key mishmatch:
								received reply with key %s, expeceted key: %s`, reply.Key, key)
	}

	return reply.Value, nil
}

// Fetches resource identified by given key, either from local store or from peers.
// Updates local DataBlobStore in case the resource is fetched from peers.
func (n *node) FetchResource(key string) ([]byte, error) {
	// check locally
	resource := n.conf.Storage.GetDataBlobStore().Get(key)

	if resource == nil { // fetch from peers
		fetchedResource, err := n.FetchFromPeers(key)
		if err != nil {
			return nil, err
		}
		n.conf.Storage.GetDataBlobStore().Set(key, fetchedResource)
		resource = fetchedResource
	}

	return resource, nil
}

func (n *node) Download(metahash string) ([]byte, error) {
	metafile, err := n.FetchResource(metahash)
	if err != nil {
		return nil, err
	}

	chunkKeys := strings.Split(string(metafile), peer.MetafileSep)

	var file []byte

	for _, chunkKey := range chunkKeys {
		chunk, err := n.FetchResource(chunkKey)
		if err != nil {
			return nil, err
		}

		file = append(file, chunk...)
	}

	return file, nil
}

func (n *node) Tag(name string, mh string) error {
	if n.conf.Storage.GetNamingStore().Get(name) != nil {
		return errors.New("name already exists")
	}

	n.tagBlocked.Set(true)

	if n.conf.TotalPeers <= 1 {
		n.conf.Storage.GetNamingStore().Set(name, []byte(mh))
		//TODO: Persist blockchainBlock
		return nil
	}

PAXOS:
	prepareID := n.conf.PaxosID
	var acceptedVal types.PaxosValue

PAXOS_LOOP:
	for {
		// PAXOS PHASE 1 //
		// Broadcast PaxosPrepareMessage
		prepareMsg := types.PaxosPrepareMessage{
			Step:   n.currStep.Get(),
			ID:     prepareID,
			Source: n.conf.Socket.GetAddress(),
		}
		transportPrepareMsg, err := n.conf.MessageRegistry.MarshalMessage(prepareMsg)
		if err != nil {
			return err
		}
		err = n.Broadcast(transportPrepareMsg)
		if err != nil {
			return err
		}

		ticker := time.NewTicker(n.conf.PaxosProposerRetry)
		n.proposer.SetPhase(1) // listen to Promise msgs
		n.proposer.SetCurrID(prepareID)

		n.proposer.SetListeningToPromiseThreshold(true)
		select {
		case <-n.proposer.promiseThresholdReached:
			n.proposer.SetPhase(0) // ignore Promise/Accept msgs
			n.proposer.SetListeningToPromiseThreshold(false)
			ticker.Stop()
		case <-ticker.C:
			n.proposer.SetPhase(0) // ignore Promise/Accept msgs
			n.proposer.SetListeningToPromiseThreshold(false)
			ticker.Stop()
			prepareID += n.conf.TotalPeers
			n.proposer.ResetRecvPromises()
			continue
		}

		// PAXOS PHASE 2 //
		// Broadcast PaxosProposeMessage
		paxosValue := types.PaxosValue{
			UniqID:   xid.New().String(),
			Filename: name,
			Metahash: mh,
		}

		n.proposer.Lock()
		if n.proposer.highestAcceptVal != nil {
			paxosValue = *n.proposer.highestAcceptVal
		}
		n.proposer.Unlock()

		proposeMsg := types.PaxosProposeMessage{
			Step:  n.currStep.Get(),
			ID:    prepareID,
			Value: paxosValue,
		}
		transportProposeMsg, err := n.conf.MessageRegistry.MarshalMessage(proposeMsg)
		if err != nil {
			return err
		}

		err = n.Broadcast(transportProposeMsg)
		if err != nil {
			return err
		}

		ticker = time.NewTicker(n.conf.PaxosProposerRetry)
		n.proposer.SetPhase(2) // listen to Accept msgs
		n.proposer.SetListeningToAcceptThreshold(true)

		select {
		case acceptedVal = <-n.proposer.acceptThresholdReached:
			n.proposer.SetPhase(0) // ignore Promise/Accept msgs
			n.proposer.SetListeningToAcceptThreshold(false)
			ticker.Stop()
			break PAXOS_LOOP // CONSENSUS REACHED
		case <-ticker.C:
			n.proposer.SetPhase(0) // ignore Promise/Accept msgs
			n.proposer.SetListeningToAcceptThreshold(false)
			ticker.Stop()
			prepareID += n.conf.TotalPeers
		}
	}
	// Consensus reached //

	n.proposer.ResetHighestAcceptVal()
	n.acceptedProposal.SetAccepted(0, nil)
	n.proposer.SetPhase(0)

	<-n.tagChan

	if acceptedVal.Filename == name && acceptedVal.Metahash != mh {
		n.tagBlocked.Set(false)
		return errors.New("[Tag]: another metahash has been tagged with given name")
	}
	if acceptedVal.Filename != name || acceptedVal.Metahash != mh {
		goto PAXOS
	}

	n.tagBlocked.Set(false)

	return nil
}

func (n *node) Resolve(name string) string {
	return string(n.conf.Storage.GetNamingStore().Get(name))
}

func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) ([]string, error) {
	var peers []string

	if n.peers.Len() > int(budget) {
		peers = n.peers.GetNRandomPeers(int(budget), NewSet())
	} else {
		peers = n.peers.GetAllPeers(NewSet())
	}

	var wg sync.WaitGroup

	leftPeers := len(peers)
	leftBudget := budget

	for _, peer := range peers {
		peerBudget := leftBudget / uint(leftPeers)
		if peerBudget == 0 {
			continue
		}

		req := types.SearchRequestMessage{
			RequestID: xid.New().String(),
			Origin:    n.conf.Socket.GetAddress(),
			Pattern:   reg.String(),
			Budget:    peerBudget}

		wg.Add(1)
		go func(peer string, req types.SearchRequestMessage) {
			defer wg.Done()

			err := n.DirectSend(peer, req)
			if err != nil {
				log.Error().Msgf("[peer.Peer.SearchAll] Send to peer error>: <%s>", err.Error())
			}

			timer := time.NewTimer(timeout)
			<-timer.C // wait full timeout
			timer.Stop()
		}(peer, req)

		leftPeers--
		leftBudget -= peerBudget
	}

	wg.Wait()

	var matchingFilenames []string

	n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
		if reg.Match([]byte(key)) {
			matchingFilenames = append(matchingFilenames, key)
		}
		return true
	})

	return matchingFilenames, nil
}

func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (string, error) {
	name := ""

	n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
		if pattern.Match([]byte(key)) {
			metafile := n.conf.Storage.GetDataBlobStore().Get(string(val))

			if metafile != nil {
				chunkKeys := strings.Split(string(metafile), peer.MetafileSep)

				for _, chunkKey := range chunkKeys {
					chunk := n.conf.Storage.GetDataBlobStore().Get(chunkKey)

					if chunk == nil {
						return true // missing chunk
					}
				}
				// found file with all chunks
				name = key
				return false // stop iteration
			}
		}
		return true
	})

	if name != "" { // found localy
		return name, nil
	}

	budget := conf.Initial

	for i := 0; i < int(conf.Retry); i++ {
		var peers []string

		if n.peers.Len() > int(budget) {
			peers = n.peers.GetNRandomPeers(int(budget), NewSet())
		} else {
			peers = n.peers.GetAllPeers(NewSet())
		}

		var wg sync.WaitGroup
		fullMatch := make(chan string)

		leftPeers := len(peers)
		leftBudget := budget

		for _, peer := range peers {
			peerBudget := leftBudget / uint(leftPeers)
			if peerBudget == 0 {
				continue
			}

			req := types.SearchRequestMessage{
				RequestID: xid.New().String(),
				Origin:    n.conf.Socket.GetAddress(),
				Pattern:   pattern.String(),
				Budget:    peerBudget}

			wg.Add(1)
			go func(peer string, req types.SearchRequestMessage) {
				defer wg.Done()

				err := n.DirectSend(peer, req)
				if err != nil {
					log.Error().Msgf("[SearchFirst] Send to peer error>: <%s>", err.Error())
				}

				channel := n.searchChannels.Set(req.RequestID, make(chan string, 1))
				defer n.ackChannels.Delete(req.RequestID)
				defer close(channel)

				timer := time.NewTimer(conf.Timeout)

				select {
				case <-timer.C:
				case fullMatchName := <-channel:
					fullMatch <- fullMatchName
				}

				timer.Stop()
			}(peer, req)

			leftPeers--
			leftBudget -= peerBudget
		}

		timeout := make(chan bool)
		go func() {
			wg.Wait()
			timeout <- true
		}()

		select {
		case name := <-fullMatch:
			return name, nil
		case <-timeout: // expand ring
		}

		budget *= conf.Factor
	}

	return name, nil
}

func (n *node) AddLink(from string, to string) error {
	// Link already in the graph
	if n.pageRank.LinkAlreadyExist(from, to) {
		return nil
	}

	n.pageRankBlocked.Set(true)

	// If only one node
	if n.conf.TotalPeers <= 1 {
		//Add new link to graph and compute ranking
		n.pageRank.AddLink(from, to)
		n.pageRankRanking.Lock()
		n.pageRankRanking.ranking = n.pageRank.Rank()
		n.pageRankRanking.Unlock()
		return nil
	}

PAXOS:
	prepareID := n.conf.PaxosID
	var acceptedVal types.PaxosValue

PAXOS_LOOP:
	for {
		// PAXOS PHASE 1 //
		// Broadcast PaxosPrepareMessage
		prepareMsg := types.PageRankPaxosPrepareMessage{
			Step:   n.pageRankcurrStep.Get(),
			ID:     prepareID,
			Source: n.conf.Socket.GetAddress(),
		}
		transportPrepareMsg, err := n.conf.MessageRegistry.MarshalMessage(prepareMsg)
		if err != nil {
			return err
		}
		err = n.Broadcast(transportPrepareMsg)
		if err != nil {
			return err
		}

		ticker := time.NewTicker(n.conf.PaxosProposerRetry)
		n.pageRankProposer.SetPhase(1) // listen to Promise msgs
		n.pageRankProposer.SetCurrID(prepareID)

		n.pageRankProposer.SetListeningToPromiseThreshold(true)
		select {
		case <-n.pageRankProposer.promiseThresholdReached:
			n.pageRankProposer.SetPhase(0) // ignore Promise/Accept msgs
			n.pageRankProposer.SetListeningToPromiseThreshold(false)
			ticker.Stop()
		case <-ticker.C:
			n.pageRankProposer.SetPhase(0) // ignore Promise/Accept msgs
			n.pageRankProposer.SetListeningToPromiseThreshold(false)
			ticker.Stop()
			prepareID += n.conf.TotalPeers
			n.pageRankProposer.ResetRecvPromises()
			continue
		}

		// PAXOS PHASE 2 //

		// Broadcast PaxosProposeMessage
		paxosValue := types.PageRankPaxosValue{
			UniqID: xid.New().String(),
			From:   from,
			To:     to,
		}

		n.pageRankProposer.Lock()
		if n.pageRankProposer.highestAcceptVal != nil {

			//Casting into PageRankpaxosValue
			paxosValue = types.PageRankPaxosValue{
				UniqID: n.pageRankProposer.highestAcceptVal.UniqID,
				From:   n.pageRankProposer.highestAcceptVal.Filename,
				To:     n.pageRankProposer.highestAcceptVal.Metahash,
			}
		}
		n.pageRankProposer.Unlock()

		proposeMsg := types.PageRankPaxosProposeMessage{
			Step:  n.pageRankcurrStep.Get(),
			ID:    prepareID,
			Value: paxosValue,
		}
		transportProposeMsg, err := n.conf.MessageRegistry.MarshalMessage(proposeMsg)
		if err != nil {
			return err
		}

		err = n.Broadcast(transportProposeMsg)
		if err != nil {
			return err
		}

		ticker = time.NewTicker(n.conf.PaxosProposerRetry)
		n.pageRankProposer.SetPhase(2) // listen to Accept msgs
		n.pageRankProposer.SetListeningToAcceptThreshold(true)

		select {
		case acceptedVal = <-n.pageRankProposer.acceptThresholdReached:
			n.pageRankProposer.SetPhase(0) // ignore Promise/Accept msgs
			n.pageRankProposer.SetListeningToAcceptThreshold(false)
			ticker.Stop()
			break PAXOS_LOOP // CONSENSUS REACHED
		case <-ticker.C:
			n.pageRankProposer.SetPhase(0) // ignore Promise/Accept msgs
			n.pageRankProposer.SetListeningToAcceptThreshold(false)
			ticker.Stop()
			prepareID += n.conf.TotalPeers
		}
	}
	// Consensus reached //

	n.pageRankProposer.ResetHighestAcceptVal()
	n.acceptedProposal.SetAccepted(0, nil)
	n.pageRankProposer.SetPhase(0)

	<-n.pageRankChan
	if acceptedVal.Filename != from || acceptedVal.Metahash != to {
		goto PAXOS
	}

	n.pageRankBlocked.Set(false)

	return nil
}

func (n *node) SearchEngineSeach(reg regexp.Regexp, budget uint, timeout time.Duration, contentSearch bool) (peer.PairList, error) {

	var peers []string

	if n.peers.Len() > int(budget) {
		peers = n.peers.GetNRandomPeers(int(budget), NewSet())
	} else {
		peers = n.peers.GetAllPeers(NewSet())
	}

	var wg sync.WaitGroup

	leftPeers := len(peers)
	leftBudget := budget
	requestID := xid.New().String()

	for _, peer := range peers {
		peerBudget := leftBudget / uint(leftPeers)
		if peerBudget == 0 {
			continue
		}

		req := types.SearchEngineRequestMessage{
			RequestID:     requestID,
			Origin:        n.conf.Socket.GetAddress(),
			Pattern:       reg.String(),
			ContentSearch: contentSearch,
			Budget:        peerBudget}

		wg.Add(1)
		go func(peer string, req types.SearchEngineRequestMessage) {
			defer wg.Done()

			err := n.DirectSend(peer, req)
			if err != nil {
				log.Error().Msgf("[peer.Peer.SearchEngineSearch] Send to peer error>: <%s>", err.Error())
			}

			timer := time.NewTimer(timeout)
			<-timer.C // wait full timeout
			timer.Stop()
		}(peer, req)

		leftPeers--
		leftBudget -= peerBudget
	}

	wg.Wait()

	responses := n.searchEngineResponseStorage.GetResponses(requestID)

	if responses != nil {
		pairlist := make(peer.PairList, len(responses))

		n.pageRankRanking.Lock()
		defer n.pageRankRanking.Unlock()
		i := 0
		for website, _ := range responses {
			_, ok := n.pageRankRanking.ranking[website]
			rank := 0.0
			if ok {
				rank = n.pageRankRanking.ranking[website]
			}
			pairlist[i] = peer.Pair{Key: website, Value: rank}
			i++
		}

		sort.Sort(pairlist)

		return pairlist, nil
	}

	return nil, nil
}

func (n *node) GetRanking() map[string]float64 {
	n.pageRankRanking.Lock()
	defer n.pageRankRanking.Unlock()

	copy := make(map[string]float64, len(n.pageRankRanking.ranking))
	for k, v := range n.pageRankRanking.ranking {
		copy[k] = v
	}
	return copy
}
