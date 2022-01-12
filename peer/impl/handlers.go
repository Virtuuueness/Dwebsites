package impl

import (
	"crypto"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"

	"go.dedis.ch/cs438/peer"
)

// ChatMessageExec implements registry.Exec
func (n *node) ChatMessageExec(msg types.Message, pkt transport.Packet) error {
	log.Info().Msgf("[ChatMessageExec] Received packet: <%s>", pkt)
	return nil
}

// RumorsMessageExec implements registry.Exec
func (n *node) RumorsMessageExec(msg types.Message, pkt transport.Packet) error {
	rumors, ok := msg.(*types.RumorsMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	expected := false

	for _, rumor := range rumors.Rumors {
		if expected = n.rumorHistory.AddRumor(rumor); expected {

			if !n.peers.Contains(rumor.Origin) { //update routing table
				n.router.Set(rumor.Origin, pkt.Header.RelayedBy)
				if rumor.Origin == pkt.Header.RelayedBy {
					n.peers.Add(rumor.Origin)
				}
			}

			header := transport.NewHeader(rumor.Origin, pkt.Header.RelayedBy, pkt.Header.Destination, pkt.Header.TTL)
			rumorPkt := transport.Packet{Header: &header, Msg: rumor.Msg}

			go func() { // process rumor
				err := n.conf.MessageRegistry.ProcessPacket(rumorPkt)
				if err != nil {
					log.Error().Msgf("<[impl.RumorsMessageExec] Process rumor error>: <%s>", err.Error())
				}
			}()

		}
		// else {
		// 	 log.Info().Msgf("[%s] unexpected Rumor sequence number %s %d ~> ignore Rumor",
		// 	 	n.conf.Socket.GetAddress(), rumor.Origin, rumor.Sequence)
		// }
	}

	{ // send AckMessage to the source
		ack := types.AckMessage{
			AckedPacketID: pkt.Header.PacketID,
			Status:        types.StatusMessage(n.rumorHistory.GetView())}
		err := n.DirectSend(pkt.Header.Source, ack)
		if err != nil {
			log.Error().Msgf("<[impl.RumorsMessageExec] Send Ack error>: <%s>", err.Error())
		}
	}

	if expected { // send to random neighbour
		dest := n.peers.GetRandomPeer(NewSet(n.conf.Socket.GetAddress(), pkt.Header.Source))
		if dest == "" {
			return nil
		}

		err := n.DirectSend(dest, rumors)
		if err != nil {
			log.Error().Msgf("<[impl.RumorsMessageExec] Send to random neighbour error>: <%s>", err.Error())
		}
	}

	return nil
}

func (n *node) StatusMessageExec(msg types.Message, pkt transport.Packet) error {
	remoteView, ok := msg.(*types.StatusMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	localView := n.rumorHistory.GetView()
	newRemoteRumors := false
	newLocalRumors := false
	rumors := make([]types.Rumor, 0)
	cnt := 0 // how many local rumors are present in remoteView

	for origin, seq := range localView {
		if remoteSeq, exists := (*remoteView)[origin]; exists {
			cnt++

			if seq > remoteSeq { // I have new rumors
				newLocalRumors = true

				newRumors, err := n.rumorHistory.GetRumorsRange(origin, remoteSeq+1)
				if err != nil {
					log.Error().Msgf("<[impl.StatusMessageExec] GetRumorsRange error>: <%s>", err.Error())
					continue
				}
				rumors = append(rumors, newRumors...)
			} else if seq < remoteSeq { // remote has new rumors
				newRemoteRumors = true
			}
		} else { // I have new rumors (from an origin that remote didn't get any rumors from)
			newLocalRumors = true

			newRumors, err := n.rumorHistory.GetRumorsRange(origin, 1)
			if err != nil {
				log.Error().Msgf("[%s]<[impl.StatusMessageExec] GetRumorsRange error>: <%s>", n.conf.Socket.GetAddress(),
					err.Error())
				continue
			}
			rumors = append(rumors, newRumors...)
		}
	}

	if cnt < len(*remoteView) {
		newRemoteRumors = true
	}

	if newRemoteRumors { // send Status (1)
		statusMsg := types.StatusMessage(localView)
		err := n.DirectSend(pkt.Header.Source, statusMsg)
		if err != nil {
			log.Error().Msgf("<[impl.StatusMessageExec] Send status error>: <%s>", err.Error())
		}
	}

	if newLocalRumors { // send rumors, ignore Ack if it arrives (2)
		rumorsMsg := types.RumorsMessage{Rumors: rumors}
		err := n.DirectSend(pkt.Header.Source, rumorsMsg)
		if err != nil {
			log.Error().Msgf("<[impl.StatusMessageExec] Send rumors error>: <%s>", err.Error())
		}
	}

	if !newRemoteRumors && !newLocalRumors { // ContinueMongering (4)
		if rand.Float64() < n.conf.ContinueMongering {
			statusMsg := types.StatusMessage(localView)
			dest := n.peers.GetRandomPeer(NewSet(n.conf.Socket.GetAddress(), pkt.Header.Source))
			if dest == "" {
				return nil
			}

			err := n.DirectSend(dest, statusMsg)
			if err != nil {
				log.Error().Msgf("<[impl.StatusMessageExec] ContinueMongering error>: <%s>", err.Error())
			}
		}
	}

	return nil
}

func (n *node) AckMessageExec(msg types.Message, pkt transport.Packet) error {
	ack, ok := msg.(*types.AckMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	if channel, ok := n.ackChannels.Get(ack.AckedPacketID); ok {
		channel <- true
	}

	transportStatusMsg, err := n.conf.MessageRegistry.MarshalMessage(ack.Status)
	if err != nil {
		return err
	}
	statusPkt := transport.Packet{Header: pkt.Header, Msg: &transportStatusMsg}
	err = n.conf.MessageRegistry.ProcessPacket(statusPkt)
	if err != nil {
		return err
	}

	return nil
}

func (n *node) PrivateMessageExec(msg types.Message, pkt transport.Packet) error {
	privateMsg, ok := msg.(*types.PrivateMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	if _, ok := privateMsg.Recipients[n.conf.Socket.GetAddress()]; ok {
		embeddedPkt := transport.Packet{Header: pkt.Header, Msg: privateMsg.Msg}
		n.conf.MessageRegistry.ProcessPacket(embeddedPkt)
	}

	return nil
}

func (n *node) EmptyMessageExec(msg types.Message, pkt transport.Packet) error {
	return nil
}

func (n *node) DataReplyMessageExec(msg types.Message, pkt transport.Packet) error {
	reply, ok := msg.(*types.DataReplyMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	if channel, ok := n.dataChannels.Get(reply.RequestID); ok {
		channel <- reply
	}

	return nil
}

func (n *node) DataRequestMessageExec(msg types.Message, pkt transport.Packet) error {
	req, ok := msg.(*types.DataRequestMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	val := n.conf.Storage.GetDataBlobStore().Get(req.Key)
	reply := types.DataReplyMessage{RequestID: req.RequestID, Key: req.Key, Value: val}
	return n.DirectSend(pkt.Header.Source, reply)

	// transportMsg, err := n.conf.MessageRegistry.MarshalMessage(reply)
	// if err != nil {
	// 	return err
	// }
	// return n.Unicast(pkt.Header.Source, transportMsg)
}

func (n *node) SearchReplyMessageExec(msg types.Message, pkt transport.Packet) error {
	reply, ok := msg.(*types.SearchReplyMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	noMissingChunks := true
	for _, fileInfo := range reply.Responses {
		n.conf.Storage.GetNamingStore().Set(fileInfo.Name, []byte(fileInfo.Metahash))
		n.UpdateCatalog(fileInfo.Metahash, pkt.Header.Source)
		for _, chunkKey := range fileInfo.Chunks {
			if chunkKey != nil {
				n.UpdateCatalog(string(chunkKey), pkt.Header.Source)
			} else {
				noMissingChunks = false
			}
		}

		if noMissingChunks {
			if channel, ok := n.searchChannels.Get(reply.RequestID); ok {
				channel <- fileInfo.Name // forward full match
			}
		}
	}

	return nil
}

// constructs types.FileInfo object
func (n *node) ConstructFileInfo(name, metahash string) (types.FileInfo, error) {
	metafile := n.conf.Storage.GetDataBlobStore().Get(string(metahash))
	if metafile == nil {
		return types.FileInfo{}, errors.New("[ConstructFileInfo] metafile not found in blobe store")
	}

	chunkKeys := strings.Split(string(metafile), peer.MetafileSep)
	var chunks [][]byte

	for _, chunkKey := range chunkKeys {
		if n.conf.Storage.GetDataBlobStore().Get(chunkKey) != nil {
			chunks = append(chunks, []byte(chunkKey))
		} else {
			chunks = append(chunks, nil)
		}
	}

	return types.FileInfo{Name: name, Metahash: metahash, Chunks: chunks}, nil
}

func (n *node) SearchRequestMessageExec(msg types.Message, pkt transport.Packet) error {
	req, ok := msg.(*types.SearchRequestMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	go func() { // forward request
		// decrease budget
		req.Budget--
		if req.Budget == 0 {
			return
		}

		// get random peers
		var peers []string
		if n.peers.Len() > int(req.Budget) {
			peers = n.peers.GetNRandomPeers(int(req.Budget), NewSet(pkt.Header.Source))
		} else {
			peers = n.peers.GetAllPeers(NewSet(pkt.Header.Source))
		}

		leftPeers := len(peers)
		leftBudget := req.Budget

		// divide budget and send to each peer
		for _, peer := range peers {
			peerBudget := leftBudget / uint(leftPeers)
			if peerBudget == 0 {
				continue
			}

			req.Budget = peerBudget
			err := n.DirectSend(peer, req)
			if err != nil {
				log.Error().Msgf("[SearchRequestMessageExec] Forward search request error>: <%s>", err.Error())
			}
		}
	}()

	// process localy
	var fileInfos []types.FileInfo
	reg := regexp.MustCompile(req.Pattern)

	// construct fileInfos from matching files
	n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
		if reg.Match([]byte(key)) {
			fileInfo, err := n.ConstructFileInfo(key, string(val))
			if err == nil {
				fileInfos = append(fileInfos, fileInfo)
			}
		}
		return true
	})

	// send reply
	reply := types.SearchReplyMessage{RequestID: req.RequestID, Responses: fileInfos}
	transportReply, err := n.conf.MessageRegistry.MarshalMessage(reply)
	if err != nil {
		return err
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), req.Origin, 0)
	replyPkt := transport.Packet{Header: &header, Msg: &transportReply}
	return n.conf.Socket.Send(pkt.Header.Source, replyPkt, n.socketTimeout)
}

func (n *node) PaxosPrepareMessageExec(msg types.Message, pkt transport.Packet) error {
	prepareMsg, ok := msg.(*types.PaxosPrepareMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	if prepareMsg.Step != n.currStep.Get() || prepareMsg.ID <= n.maxID.Get() {
		return nil // ignore
	}

	// Promise
	n.maxID.Set(prepareMsg.ID)

	acceptedID, acceptedVal := n.acceptedProposal.GetAccepted()

	// send PaxosPromiseMessage to source using private broadcast
	promiseMsg := types.PaxosPromiseMessage{
		Step:          n.currStep.Get(),
		ID:            prepareMsg.ID,
		AcceptedID:    acceptedID,
		AcceptedValue: acceptedVal,
	}
	transportPromise, err := n.conf.MessageRegistry.MarshalMessage(promiseMsg)
	if err != nil {
		return err
	}
	privateMsg := types.PrivateMessage{
		Recipients: NewSet(prepareMsg.Source).Get(),
		Msg:        &transportPromise,
	}
	transportPrivate, err := n.conf.MessageRegistry.MarshalMessage(privateMsg)
	if err != nil {
		return err
	}

	return n.Broadcast(transportPrivate)
}

func (n *node) PaxosProposeMessageExec(msg types.Message, pkt transport.Packet) error {
	proposeMsg, ok := msg.(*types.PaxosProposeMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	if proposeMsg.Step != n.currStep.Get() || proposeMsg.ID != n.maxID.Get() {
		return nil
	}

	// n.proposer.SetPhase(2)

	// Accept
	n.acceptedProposal.SetAccepted(proposeMsg.ID, &proposeMsg.Value)

	// broadcast PaxosAcceptMessage
	acceptMsg := types.PaxosAcceptMessage{
		Step:  n.currStep.Get(),
		ID:    proposeMsg.ID,
		Value: proposeMsg.Value,
	}
	transportAccept, err := n.conf.MessageRegistry.MarshalMessage(acceptMsg)
	if err != nil {
		return err
	}
	return n.Broadcast(transportAccept)
}

func (n *node) PaxosPromiseMessageExec(msg types.Message, pkt transport.Packet) error {
	promiseMsg, ok := msg.(*types.PaxosPromiseMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	n.proposer.Lock()

	if promiseMsg.Step != n.currStep.Get() || n.proposer.paxosPhase != 1 || n.proposer.currID != promiseMsg.ID {
		n.proposer.Unlock()
		return nil // ignore
	}

	n.proposer.recvPromises++

	if promiseMsg.AcceptedValue != nil && promiseMsg.AcceptedID > n.proposer.highestAcceptID {
		n.proposer.highestAcceptID = promiseMsg.AcceptedID
		n.proposer.highestAcceptVal = promiseMsg.AcceptedValue
	}

	if n.proposer.recvPromises >= uint(n.conf.PaxosThreshold(n.conf.TotalPeers)) &&
		n.proposer.isListeningToPromiseThreshold {
		n.proposer.Unlock()
		n.proposer.promiseThresholdReached <- true // threshold reached
	} else {
		n.proposer.Unlock()
	}

	return nil
}

func (n *node) PaxosAcceptMessageExec(msg types.Message, pkt transport.Packet) error {
	acceptMsg, ok := msg.(*types.PaxosAcceptMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	if n.consensusReached.Get() {
		return nil
	}
	n.proposer.Lock()

	if acceptMsg.Step != n.currStep.Get() || n.proposer.paxosPhase == 1 {
		n.proposer.Unlock()
		return nil // ignore
	}

	if n.proposer.acceptedVals[acceptMsg.Value.UniqID] == nil {
		n.proposer.acceptedVals[acceptMsg.Value.UniqID] = NewSet()
	}
	n.proposer.acceptedVals[acceptMsg.Value.UniqID].Add(pkt.Header.Source)

	reached := false
	if n.proposer.acceptedVals[acceptMsg.Value.UniqID].Len() >= n.conf.PaxosThreshold(n.conf.TotalPeers) {
		n.consensusReached.Set(true)
		reached = true
		if n.proposer.isListeningToAcceptThreshold {
			n.proposer.Unlock()
			n.proposer.acceptThresholdReached <- acceptMsg.Value // consensus reached
		} else {
			n.proposer.Unlock()
		}
	} else {
		n.proposer.Unlock()
	}

	time.Sleep(time.Second)

	if reached && !n.proposer.IsTlcBroadcasted() {
		var prevBlock types.BlockchainBlock
		prevBlock.Hash = n.conf.Storage.GetBlockchainStore().Get(storage.LastBlockKey)
		if prevBlock.Hash == nil {
			prevBlock.Hash = make([]byte, 32)
		}

		// Create block
		block := types.BlockchainBlock{
			Index:    n.currStep.Get(),
			Hash:     nil,
			Value:    acceptMsg.Value,
			PrevHash: prevBlock.Hash,
		}

		blockConcat := append([]byte(strconv.Itoa(int(block.Index))), []byte(block.Value.UniqID)...)
		blockConcat = append(blockConcat, []byte(block.Value.Filename)...)
		blockConcat = append(blockConcat, []byte(block.Value.Metahash)...)
		blockConcat = append(blockConcat, block.PrevHash...)

		h := crypto.SHA256.New()
		_, err := h.Write([]byte(blockConcat))
		if err != nil {
			return err
		}
		block.Hash = h.Sum(nil)

		// Broadcast TLCMessage
		tlcMsg := types.TLCMessage{
			Step:  acceptMsg.Step,
			Block: block,
		}
		transportTlcMsg, err := n.conf.MessageRegistry.MarshalMessage(tlcMsg)
		if err != nil {
			return err
		}

		n.proposer.SetTlcBroadcasted(true)
		err = n.Broadcast(transportTlcMsg)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *node) TLCMessageExec(msg types.Message, pkt transport.Packet) error {
	tlcMsg, ok := msg.(*types.TLCMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	n.tlcCount.Append(tlcMsg.Step, tlcMsg)

	if tlcMsg.Step < n.currStep.Get() || n.tlcCount.Len(tlcMsg.Step) > n.conf.PaxosThreshold(n.conf.TotalPeers) {
		return nil
	}

	if tlcMsg.Step == n.currStep.Get() {
		for {
			representativeTlcMsg, len := n.tlcCount.Get(n.currStep.Get())
			if len >= n.conf.PaxosThreshold(n.conf.TotalPeers) && representativeTlcMsg.Step == n.currStep.Get() {
				// threshold reached for current Step

				n.currStep.Increment() // increase TLC step
				IsTlcBroadcasted := n.proposer.IsTlcBroadcasted()
				n.proposer.SetTlcBroadcasted(false)
				n.maxID.Set(0)
				n.acceptedProposal.SetAccepted(0, nil)
				n.consensusReached.Set(false)

				log.Info().Msgf("[%s][TLCMessageExec] Increased TLC Step: %d", n.conf.Socket.GetAddress(), n.currStep.Get())

				buf, err := representativeTlcMsg.Block.Marshal()
				if err != nil {
					return err
				}

				// add to blockchain
				n.conf.Storage.GetBlockchainStore().Set(hex.EncodeToString(representativeTlcMsg.Block.Hash), buf)
				n.conf.Storage.GetBlockchainStore().Set(storage.LastBlockKey, representativeTlcMsg.Block.Hash)

				// set name/metahash mapping
				n.conf.Storage.GetNamingStore().Set(representativeTlcMsg.Block.Value.Filename,
					[]byte(representativeTlcMsg.Block.Value.Metahash))

				// broadcast TLC if it hasn't yet been broadcasted
				if !IsTlcBroadcasted {
					transportTlcMsg, err := n.conf.MessageRegistry.MarshalMessage(representativeTlcMsg)
					if err != nil {
						return err
					}
					err = n.Broadcast(transportTlcMsg)
					if err != nil {
						return err
					}
				}
				n.proposer.ResetHighestAcceptVal()

				if n.tagBlocked.Get() {
					n.tagChan <- representativeTlcMsg.Block.Value
				}
			} else {
				break
			}
		}
	}

	return nil
}

func (n *node) PageRankPaxosPrepareMessageExec(msg types.Message, pkt transport.Packet) error {
	prepareMsg, ok := msg.(*types.PageRankPaxosPrepareMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}
	if prepareMsg.Step != n.pageRankcurrStep.Get() || prepareMsg.ID <= n.pageRankMaxID.Get() {
		return nil // ignore
	}

	// Promise
	n.pageRankMaxID.Set(prepareMsg.ID)

	acceptedID, acceptedVal := n.pageRankAcceptedProposal.GetAccepted()
	promiseMsg := types.PageRankPaxosPromiseMessage{
		Step:          n.pageRankcurrStep.Get(),
		ID:            prepareMsg.ID,
		AcceptedID:    acceptedID,
		AcceptedValue: nil, //Cast
	}
	if acceptedVal != nil {
		promiseMsg = types.PageRankPaxosPromiseMessage{
			Step:          n.pageRankcurrStep.Get(),
			ID:            prepareMsg.ID,
			AcceptedID:    acceptedID,
			AcceptedValue: &types.PageRankPaxosValue{UniqID: acceptedVal.UniqID, From: acceptedVal.Filename, To: acceptedVal.Metahash}, //Cast
		}

	}
	// send PaxosPromiseMessage to source using private broadcast
	transportPromise, err := n.conf.MessageRegistry.MarshalMessage(promiseMsg)
	if err != nil {
		return err
	}
	privateMsg := types.PrivateMessage{
		Recipients: NewSet(prepareMsg.Source).Get(),
		Msg:        &transportPromise,
	}
	transportPrivate, err := n.conf.MessageRegistry.MarshalMessage(privateMsg)
	if err != nil {
		return err
	}

	return n.Broadcast(transportPrivate)
}

func (n *node) PageRankPaxosProposeMessageExec(msg types.Message, pkt transport.Packet) error {
	proposeMsg, ok := msg.(*types.PageRankPaxosProposeMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	if proposeMsg.Step != n.pageRankcurrStep.Get() || proposeMsg.ID != n.pageRankMaxID.Get() {
		return nil
	}

	// Accept
	n.acceptedProposal.SetAccepted(proposeMsg.ID,
		&types.PaxosValue{UniqID: proposeMsg.Value.UniqID, Filename: proposeMsg.Value.From, Metahash: proposeMsg.Value.To}) //Casting

	// broadcast PaxosAcceptMessage
	acceptMsg := types.PageRankPaxosAcceptMessage{
		Step:  n.pageRankcurrStep.Get(),
		ID:    proposeMsg.ID,
		Value: proposeMsg.Value,
	}
	transportAccept, err := n.conf.MessageRegistry.MarshalMessage(acceptMsg)
	if err != nil {
		return err
	}
	return n.Broadcast(transportAccept)
}

func (n *node) PageRankPaxosPromiseMessageExec(msg types.Message, pkt transport.Packet) error {
	promiseMsg, ok := msg.(*types.PageRankPaxosPromiseMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	n.pageRankProposer.Lock()

	if promiseMsg.Step != n.pageRankcurrStep.Get() || n.pageRankProposer.paxosPhase != 1 || n.pageRankProposer.currID != promiseMsg.ID {
		n.pageRankProposer.Unlock()
		return nil // ignore
	}

	n.pageRankProposer.recvPromises++

	if promiseMsg.AcceptedValue != nil && promiseMsg.AcceptedID > n.pageRankProposer.highestAcceptID {
		n.pageRankProposer.highestAcceptID = promiseMsg.AcceptedID
		n.pageRankProposer.highestAcceptVal = &types.PaxosValue{UniqID: promiseMsg.AcceptedValue.UniqID, Filename: promiseMsg.AcceptedValue.From, Metahash: promiseMsg.AcceptedValue.To} //Casting
	}

	if n.pageRankProposer.recvPromises >= uint(n.conf.PaxosThreshold(n.conf.TotalPeers)) &&
		n.pageRankProposer.isListeningToPromiseThreshold {
		n.pageRankProposer.Unlock()
		n.pageRankProposer.promiseThresholdReached <- true // threshold reached
	} else {
		n.pageRankProposer.Unlock()
	}

	return nil
}

func (n *node) PageRankPaxosAcceptMessageExec(msg types.Message, pkt transport.Packet) error {
	acceptMsg, ok := msg.(*types.PageRankPaxosAcceptMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	if n.pageRankConsensusReached.Get() {
		return nil
	}
	n.pageRankProposer.Lock()

	if acceptMsg.Step != n.pageRankcurrStep.Get() || n.pageRankProposer.paxosPhase == 1 {
		n.pageRankProposer.Unlock()
		return nil // ignore
	}

	if n.pageRankProposer.acceptedVals[acceptMsg.Value.UniqID] == nil {
		n.pageRankProposer.acceptedVals[acceptMsg.Value.UniqID] = NewSet()
	}
	n.pageRankProposer.acceptedVals[acceptMsg.Value.UniqID].Add(pkt.Header.Source)

	reached := false
	if n.pageRankProposer.acceptedVals[acceptMsg.Value.UniqID].Len() >= n.conf.PaxosThreshold(n.conf.TotalPeers) {
		n.pageRankConsensusReached.Set(true)
		reached = true
		if n.pageRankProposer.isListeningToAcceptThreshold {
			n.pageRankProposer.Unlock()
			n.pageRankProposer.acceptThresholdReached <- types.PaxosValue{UniqID: acceptMsg.Value.UniqID, Filename: acceptMsg.Value.From, Metahash: acceptMsg.Value.To} // consensus reached + casting
		} else {
			n.pageRankProposer.Unlock()
		}
	} else {
		n.pageRankProposer.Unlock()
	}

	time.Sleep(time.Second)

	if reached && !n.pageRankProposer.IsTlcBroadcasted() {
		var prevBlock types.PageRankBlockchainBlock
		prevBlock.Hash = n.conf.Storage.GetPageRankBlockchainStore().Get(storage.LastBlockKey)
		if prevBlock.Hash == nil {
			prevBlock.Hash = make([]byte, 32)
		}

		// Create block
		block := types.PageRankBlockchainBlock{
			Index:    n.pageRankcurrStep.Get(),
			Hash:     nil,
			Value:    acceptMsg.Value,
			PrevHash: prevBlock.Hash,
		}

		blockConcat := append([]byte(strconv.Itoa(int(block.Index))), []byte(block.Value.UniqID)...)
		blockConcat = append(blockConcat, []byte(block.Value.From)...)
		blockConcat = append(blockConcat, []byte(block.Value.To)...)
		blockConcat = append(blockConcat, block.PrevHash...)

		h := crypto.SHA256.New()
		_, err := h.Write([]byte(blockConcat))
		if err != nil {
			return err
		}
		block.Hash = h.Sum(nil)

		// Broadcast PageRankTLCMessage
		tlcMsg := types.PageRankTLCMessage{
			Step:  acceptMsg.Step,
			Block: block,
		}
		transportTlcMsg, err := n.conf.MessageRegistry.MarshalMessage(tlcMsg)
		if err != nil {
			return err
		}

		n.pageRankProposer.SetTlcBroadcasted(true)
		err = n.Broadcast(transportTlcMsg)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *node) PageRankTLCMessageExec(msg types.Message, pkt transport.Packet) error {
	tlcMsg, ok := msg.(*types.PageRankTLCMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	n.pageRankTlcCount.Append(tlcMsg.Step, tlcMsg)

	if tlcMsg.Step < n.pageRankcurrStep.Get() || n.tlcCount.Len(tlcMsg.Step) > n.conf.PaxosThreshold(n.conf.TotalPeers) {
		return nil
	}

	if tlcMsg.Step == n.pageRankcurrStep.Get() {
		for {
			representativeTlcMsg, len := n.pageRankTlcCount.Get(n.pageRankcurrStep.Get())
			if len >= n.conf.PaxosThreshold(n.conf.TotalPeers) && representativeTlcMsg.Step == n.pageRankcurrStep.Get() {
				// threshold reached for current Step

				n.pageRankcurrStep.Increment() // increase TLC step
				IsTlcBroadcasted := n.pageRankProposer.IsTlcBroadcasted()
				n.pageRankProposer.SetTlcBroadcasted(false)
				n.pageRankMaxID.Set(0)
				n.pageRankAcceptedProposal.SetAccepted(0, nil)
				n.pageRankConsensusReached.Set(false)

				log.Info().Msgf("[%s][PageRankTLCMessageExec] Increased PageRankTLC Step: %d", n.conf.Socket.GetAddress(), n.pageRankcurrStep.Get())

				buf, err := representativeTlcMsg.Block.Marshal()
				if err != nil {
					return err
				}

				// add to blockchain
				n.conf.Storage.GetPageRankBlockchainStore().Set(hex.EncodeToString(representativeTlcMsg.Block.Hash), buf)
				n.conf.Storage.GetPageRankBlockchainStore().Set(storage.LastBlockKey, representativeTlcMsg.Block.Hash)

				// Add new Link and compute new ranking
				n.pageRank.AddLink(representativeTlcMsg.Block.Value.From, representativeTlcMsg.Block.Value.To)
				n.pageRankRanking.Lock()
				n.pageRankRanking.ranking = n.pageRank.Rank()
				n.pageRankRanking.Unlock()

				// broadcast TLC if it hasn't yet been broadcasted
				if !IsTlcBroadcasted {
					transportTlcMsg, err := n.conf.MessageRegistry.MarshalMessage(representativeTlcMsg)
					if err != nil {
						return err
					}
					err = n.Broadcast(transportTlcMsg)
					if err != nil {
						return err
					}
				}
				n.pageRankProposer.ResetHighestAcceptVal()

				if n.pageRankBlocked.Get() {
					n.pageRankChan <- representativeTlcMsg.Block.Value
				}
			} else {
				break
			}
		}
	}

	return nil
}

func (n *node) SearchEngineReplyMessageExec(msg types.Message, pkt transport.Packet) error {
	reply, ok := msg.(*types.SearchEngineReplyMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	n.searchEngineResponseStorage.AddResponses(reply.RequestID, reply.Responses)

	return nil
}

func (n *node) SearchEngineRequestMessageExec(msg types.Message, pkt transport.Packet) error {
	req, ok := msg.(*types.SearchEngineRequestMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	go func() { // forward request
		// decrease budget
		req.Budget--
		if req.Budget == 0 {
			return
		}

		// get random peers
		var peers []string
		if n.peers.Len() > int(req.Budget) {
			peers = n.peers.GetNRandomPeers(int(req.Budget), NewSet(pkt.Header.Source))
		} else {
			peers = n.peers.GetAllPeers(NewSet(pkt.Header.Source))
		}

		leftPeers := len(peers)
		leftBudget := req.Budget

		// divide budget and send to each peer
		for _, peer := range peers {
			peerBudget := leftBudget / uint(leftPeers)
			if peerBudget == 0 {
				continue
			}

			req.Budget = peerBudget
			err := n.DirectSend(peer, req)
			if err != nil {
				log.Error().Msgf("[SearchRequestMessageExec] Forward search request error>: <%s>", err.Error())
			}
		}
	}()

	// process localy
	// reg := regexp.MustCompile(req.Pattern)

	var responses []string

	// TODO Compute responses from local website data

	if req.ContentSearch {
		// TODO also add regex match from content
	}

	// send reply
	reply := types.SearchEngineReplyMessage{RequestID: req.RequestID, Responses: responses}
	transportReply, err := n.conf.MessageRegistry.MarshalMessage(reply)
	if err != nil {
		return err
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), req.Origin, 0)
	replyPkt := transport.Packet{Header: &header, Msg: &transportReply}
	return n.conf.Socket.Send(pkt.Header.Source, replyPkt, n.socketTimeout)
}
