package udp

import (
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	pc, err := net.ListenPacket("udp", address)
	if err != nil {
		return &Socket{}, err
	}

	return &Socket{pc: pc}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	pc net.PacketConn

	ins  packets
	outs packets
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	return s.pc.Close()
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	raddr, err := net.ResolveUDPAddr("udp", dest)
	if err != nil {
		return err
	}

	bytes, err := pkt.Marshal()
	if err != nil {
		return err
	}

	if timeout == 0 {
		s.pc.SetWriteDeadline(time.Time{})
	} else {
		s.pc.SetWriteDeadline(time.Now().Add(timeout))
	}

	writtenBytes, err := s.pc.WriteTo(bytes, raddr)
	if err != nil {
		if os.IsTimeout(err) {
			return transport.TimeoutErr(timeout)
		}
		return err
	}
	if writtenBytes < len(bytes) {
		return errors.New("[transport.udp.Socket.Send]: Didn't write all bytes")
	}

	s.outs.add(pkt)

	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	if timeout == 0 {
		s.pc.SetReadDeadline(time.Time{})
	} else {
		s.pc.SetReadDeadline(time.Now().Add(timeout))
	}

	buffer := make([]byte, bufSize)

	n, _, err := s.pc.ReadFrom(buffer)
	if err != nil {
		if os.IsTimeout(err) {
			return transport.Packet{}, transport.TimeoutErr(timeout)
		}
		return transport.Packet{}, err
	}

	var pkt transport.Packet
	pkt.Unmarshal(buffer[0:n])
	s.ins.add(pkt)

	return pkt, nil
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.pc.LocalAddr().String()
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	return s.ins.getAll()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return s.outs.getAll()
}

type packets struct {
	sync.Mutex
	data []transport.Packet
}

func (p *packets) add(pkt transport.Packet) {
	p.Lock()
	defer p.Unlock()

	p.data = append(p.data, pkt)
}

func (p *packets) getAll() []transport.Packet {
	p.Lock()
	defer p.Unlock()

	res := make([]transport.Packet, len(p.data))

	for i, pkt := range p.data {
		res[i] = pkt.Copy()
	}

	return res
}
