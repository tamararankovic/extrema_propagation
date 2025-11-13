package peers

import (
	"log"
	"net"
)

type Peer struct {
	id     string
	addr   *net.UDPAddr
	conn   *net.UDPConn
	failed bool
}

func (p *Peer) GetID() string {
	return p.id
}

func (p *Peer) Send(data []byte) {
	go func() {
		_, err := p.conn.WriteToUDP(data, p.addr)
		if err != nil {
			log.Println(err)
		}
		MessagesSentLock.Lock()
		MessagesSent++
		MessagesSentLock.Unlock()
	}()
}
