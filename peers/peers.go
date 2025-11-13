package peers

import (
	"log"
	"net"

	"github.com/tamararankovic/extrema_propagation/config"
)

type MsgReceived struct {
	Sender   Peer
	MsgBytes []byte
}

type Peers struct {
	peers     []Peer
	conn      *net.UDPConn
	Messages  chan MsgReceived
	PeerAdded chan Peer
}

func NewPeers(config config.Config) (*Peers, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(config.ListenIP), Port: config.ListenPort})
	if err != nil {
		return nil, err
	}
	ps := &Peers{
		conn:      conn,
		Messages:  make(chan MsgReceived, 1),
		PeerAdded: make(chan Peer, 1),
	}
	for i := range config.PeersIDs {
		ps.peers = append(ps.peers, Peer{
			id:     config.PeersIDs[i],
			addr:   &net.UDPAddr{IP: net.ParseIP(config.PeersIPs[i]), Port: config.ListenPort},
			conn:   conn,
			failed: false,
		})
	}
	go ps.listen()
	return ps, nil
}

func (ps *Peers) GetPeers() []Peer {
	active := make([]Peer, 0)
	for _, p := range ps.peers {
		if p.failed {
			continue
		}
		active = append(active, p)
	}
	return active
}

func (ps *Peers) GetFailedPeers() []Peer {
	failed := make([]Peer, 0)
	for _, p := range ps.peers {
		if !p.failed {
			continue
		}
		failed = append(failed, p)
	}
	return failed
}

func (ps *Peers) PeerFailed(id string) {
	p := ps.findPeerById(id)
	if p == nil {
		return
	}
	p.failed = true
}

func (ps *Peers) listen() {
	buf := make([]byte, 1472)
	for {
		n, sender, err := ps.conn.ReadFromUDP(buf)
		if err != nil {
			log.Println("read error:", err)
			continue
		}
		MessagesRcvdLock.Lock()
		MessagesRcvd++
		MessagesRcvdLock.Unlock()
		peer := ps.findPeerByAddr(sender)
		if peer == nil {
			log.Println("no peer found for address", sender)
			continue
		}
		if peer.failed {
			peer.failed = false
			ps.PeerAdded <- *peer
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		ps.Messages <- MsgReceived{
			Sender:   *peer,
			MsgBytes: data,
		}
	}
}

func (ps *Peers) findPeerByAddr(addr *net.UDPAddr) *Peer {
	for _, p := range ps.peers {
		if p.addr.IP.Equal(addr.IP) {
			return &p
		}
	}
	return nil
}

func (ps *Peers) findPeerById(id string) *Peer {
	for _, p := range ps.peers {
		if p.id == id {
			return &p
		}
	}
	return nil
}
