package impl

import (
	"fmt"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"math/big"
)

// updates bucket with received contacts, forwards reply to appropriate channel
func (n *node) FindNodeReplyExec(msg types.Message, pkt transport.Packet) error {
	reply, ok := msg.(*types.FindNodeReply)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	n.AddContact(NewContact(pkt.Header.Source))
	// for _, contact := range reply.Contacts {
	// 	n.AddContact(contact)
	// }

	if channel, ok := n.contactsChannels.Get(reply.RequestID); ok {
		channel <- reply
	}

	return nil
}

// returns local k closest nodes to requested id
func (n *node) FindNodeRequestExec(msg types.Message, pkt transport.Packet) error {
	req, ok := msg.(*types.FindNodeRequest)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	n.AddContact(NewContact(pkt.Header.Source))

	reply := types.FindNodeReply{RequestID: req.RequestID, Contacts: n.FindKClosest(req.Id)}
	err := n.DirectSend(pkt.Header.Source, reply)
	if err != nil {
		return err
	}

	return nil
}

// updates bucket with received contacts, forwards reply to appropriate channel
func (n *node) FindValueReplyExec(msg types.Message, pkt transport.Packet) error {
	reply, ok := msg.(*types.FindValueReply) // WRONG TYPE
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	n.AddContact(NewContact(pkt.Header.Source))
	// for _, contact := range reply.Contacts {
	// 	n.AddContact(contact)
	// }

	if channel, ok := n.valueChannels.Get(reply.RequestID); ok {
		channel <- reply
	}

	return nil
}

// returns local k closest nodes to requested id
func (n *node) FindValueRequestExec(msg types.Message, pkt transport.Packet) error {
	req, ok := msg.(*types.FindValueRequest)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	n.AddContact(NewContact(pkt.Header.Source))
	n.AddPeer(pkt.Header.Source)

	keyId := new(big.Int)
	keyId.SetString(req.Key, 16)

	val, _ := n.localHashMap.Get(req.Key)
	reply := types.FindValueReply{RequestID: req.RequestID, Value: val, Contacts: n.FindKClosest(*keyId)}

	err := n.DirectSend(pkt.Header.Source, reply)
	if err != nil {
		return err
	}

	return nil
}

func (n *node) StoreRequestExec(msg types.Message, pkt transport.Packet) error {
	req, ok := msg.(*types.StoreRequest)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	n.AddContact(NewContact(pkt.Header.Source))

	n.localHashMap.Set(req.Key, req.Value)

	// println("[", n.conf.Socket.GetAddress(), "] stored", req.Key, " -> ", string(req.Value))
	return nil
}

// TODO: ping
