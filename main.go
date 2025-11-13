package main

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"slices"

	"github.com/tamararankovic/extrema_propagation/config"
	"github.com/tamararankovic/extrema_propagation/peers"
)

const AGG_MSG_TYPE int8 = 1

const (
	EXP_BITS = 5
	EXP_MASK = (1 << EXP_BITS) - 1 // 0b11111 = 31
)

func encodeValue(v float64) uint8 {
	if v <= 0 {
		return 0
	}
	exp := int(math.Floor(math.Log2(v)))
	if exp < -28 {
		exp = -28
	}
	if exp > 3 {
		exp = 3
	}
	return uint8(exp + 28) // shift into range 0..31
}

func decodeValue(e uint8) float64 {
	exp := int(e) - 28
	return math.Pow(2, float64(exp))
}

type Msg interface {
	Type() int8
}

type EPMsg struct {
	Epoch       int
	ValuesSum   []float64
	ValuesCount []float64
}

func (m EPMsg) Type() int8 {
	return AGG_MSG_TYPE
}

func EncodeEPMsg(m *EPMsg) []byte {
	K := len(m.ValuesSum)
	totalBits := 8 + 32 + 16 + (K * EXP_BITS * 2)
	out := make([]byte, (totalBits+7)/8)

	// Write type
	out[0] = byte(AGG_MSG_TYPE)
	bitPos := 8

	// Epoch (uint32)
	binary.LittleEndian.PutUint32(out[1:], uint32(m.Epoch))
	bitPos += 32

	// K (uint16)
	binary.LittleEndian.PutUint16(out[5:], uint16(K))
	bitPos += 16

	// Encode values
	write5Bits := func(val uint8) {
		bytePos := bitPos / 8
		bitOff := bitPos % 8

		out[bytePos] |= val << bitOff
		if bitOff > 3 {
			out[bytePos+1] |= val >> (8 - bitOff)
		}
		bitPos += EXP_BITS
	}

	for _, v := range m.ValuesSum {
		write5Bits(encodeValue(v))
	}
	for _, v := range m.ValuesCount {
		write5Bits(encodeValue(v))
	}

	return out
}

func DecodeEPMsg(b []byte) (*EPMsg, error) {
	if len(b) < 7 {
		return nil, fmt.Errorf("too short")
	}
	if b[0] != byte(AGG_MSG_TYPE) {
		return nil, fmt.Errorf("type mismatch")
	}

	epoch := binary.LittleEndian.Uint32(b[1:])
	K := int(binary.LittleEndian.Uint16(b[5:]))

	totalBitsNeeded := 8 + 32 + 16 + (K * EXP_BITS * 2)
	if len(b)*8 < totalBitsNeeded {
		return nil, fmt.Errorf("truncated packet")
	}

	msg := &EPMsg{
		Epoch:       int(epoch),
		ValuesSum:   make([]float64, K),
		ValuesCount: make([]float64, K),
	}

	bitPos := 8 + 32 + 16

	read5Bits := func() uint8 {
		bytePos := bitPos / 8
		bitOff := bitPos % 8
		bitPos += EXP_BITS

		val := (b[bytePos] >> bitOff) & EXP_MASK
		if bitOff > 3 {
			val |= (b[bytePos+1] << (8 - bitOff)) & EXP_MASK
		}
		return val
	}

	for i := 0; i < K; i++ {
		msg.ValuesSum[i] = decodeValue(read5Bits())
	}
	for i := 0; i < K; i++ {
		msg.ValuesCount[i] = decodeValue(read5Bits())
	}

	return msg, nil
}

type Node struct {
	ID          string
	Value       float64
	TAgg        int
	K           int
	MinSum      []float64
	MinCount    []float64
	NoNews      int
	MinNoNews   int
	Converged   bool
	Epoch       int
	Round       int
	EpochLength int
	Peers       *peers.Peers
	Lock        *sync.Mutex
}

func (n *Node) receive(msg *EPMsg) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	if msg.Epoch < n.Epoch {
		return
	}

	if msg.Epoch > n.Epoch {
		log.Printf("[%s] switching to epoch %d", n.ID, msg.Epoch)
		n.Epoch = msg.Epoch
		n.Round = 0
		n.resetMinima()
		n.NoNews = 0
		n.Converged = false
	}

	if len(msg.ValuesSum) != n.K {
		log.Println("length mismatch", len(msg.ValuesSum))
		return
	}
	if len(msg.ValuesCount) != n.K {
		log.Println("length mismatch", len(msg.ValuesCount))
		return
	}

	minChanged := false
	for i := range n.MinSum {
		if n.MinSum[i] > msg.ValuesSum[i] {
			n.MinSum[i] = msg.ValuesSum[i]
			minChanged = true
		}
	}
	for i := range n.MinCount {
		if n.MinCount[i] > msg.ValuesCount[i] {
			n.MinCount[i] = msg.ValuesCount[i]
			minChanged = true
		}
	}

	if !minChanged {
		n.NoNews++
		if n.NoNews > n.MinNoNews {
			n.Converged = true
		}
	} else {
		n.NoNews = 0
	}
}

func (n *Node) send() {
	ticker := time.NewTicker(time.Duration(n.TAgg) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		n.Lock.Lock()
		n.Round++

		if n.Round >= n.EpochLength {
			n.Epoch++
			n.Round = 0
			n.resetMinima()
			n.NoNews = 0
			n.Converged = false
			log.Printf("[%s] starting epoch %d", n.ID, n.Epoch)
		}

		estimate := n.estimate()
		log.Printf("[%s] epoch=%d estimate=%.3f", n.ID, n.Epoch, estimate)

		if n.Converged {
			n.Lock.Unlock()
			continue
		}

		payload := EPMsg{
			Epoch:       n.Epoch,
			ValuesSum:   slices.Clone(n.MinSum),
			ValuesCount: slices.Clone(n.MinCount),
		}
		n.Lock.Unlock()

		msg := EncodeEPMsg(&payload)

		peers := n.Peers.GetPeers()
		for _, peer := range peers {
			peer.Send(msg)
		}
	}
}

func (n *Node) resetMinima() {
	for i := range n.MinSum {
		n.MinSum[i] = rand.ExpFloat64() / n.Value
	}
	for i := range n.MinCount {
		n.MinCount[i] = rand.ExpFloat64()
	}
}

func (n *Node) estimate() float64 {
	sum := 0.0
	for _, x := range n.MinSum {
		sum += x
	}
	count := 0.0
	for _, x := range n.MinCount {
		count += x
	}
	return (float64(n.K-1) / sum) / (float64(n.K-1) / count)
}

func main() {
	time.Sleep(10 * time.Second)

	cfg := config.LoadConfigFromEnv()
	params := config.LoadParamsFromEnv()

	ps, err := peers.NewPeers(cfg)
	if err != nil {
		log.Fatal(err)
	}

	val, err := strconv.Atoi(params.ID)
	if err != nil {
		log.Fatal(err)
	}

	node := &Node{
		ID:          params.ID,
		Value:       float64(val),
		TAgg:        params.Tagg,
		K:           params.K,
		MinSum:      make([]float64, params.K),
		MinCount:    make([]float64, params.K),
		Peers:       ps,
		Lock:        &sync.Mutex{},
		EpochLength: params.EpochLength,
		MinNoNews:   params.MinNoNews,
	}

	node.resetMinima()

	lastRcvd := make(map[string]int)
	round := 0

	// handle messages
	go func() {
		for msgRcvd := range ps.Messages {
			msg, err := DecodeEPMsg(msgRcvd.MsgBytes)
			if msg == nil || err != nil {
				continue
			}
			node.receive(msg)
		}
	}()

	// remove failed peers
	go func() {
		for range time.NewTicker(time.Second).C {
			round++
			for _, peer := range ps.GetPeers() {
				if lastRcvd[peer.GetID()]+params.Rmax < round && round > 10 {
					ps.PeerFailed(peer.GetID())
				}
			}
		}
	}()

	go func() {
		for range time.NewTicker(time.Second).C {
			node.exportMsgCount()
			node.Lock.Lock()
			value := node.estimate()
			node.Lock.Unlock()
			node.exportResult(value, 0, time.Now().UnixNano())
		}
	}()

	go node.send()

	r := http.NewServeMux()
	r.HandleFunc("POST /metrics", node.setMetricsHandler)
	log.Println("Metrics server listening")

	log.Fatal(http.ListenAndServe(strings.Split(os.Getenv("LISTEN_ADDR"), ":")[0]+":9200", r))
}

func (n *Node) setMetricsHandler(w http.ResponseWriter, r *http.Request) {
	newMetrics, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	lines := strings.Split(string(newMetrics), "\n")
	valStr := ""
	for _, line := range lines {
		if strings.HasPrefix(line, "app_memory_usage_bytes") {
			valStr = strings.Split(line, " ")[1]
			break
		}
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		log.Println(err)
	} else {
		log.Println("new value", val)
		n.Value = val
	}
	w.WriteHeader(http.StatusOK)
}

var writers map[string]*csv.Writer = map[string]*csv.Writer{}

func (n *Node) exportResult(value float64, reqTimestamp, rcvTimestamp int64) {
	name := "value"
	filename := fmt.Sprintf("/var/log/extrema_propagation/%s.csv", name)
	writer := writers[filename]
	if writer == nil {
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Printf("failed to open/create file: %v", err)
			return
		}
		writer = csv.NewWriter(file)
		writers[filename] = writer
	}
	defer writer.Flush()
	reqTsStr := strconv.Itoa(int(reqTimestamp))
	rcvTsStr := strconv.Itoa(int(rcvTimestamp))
	valStr := strconv.FormatFloat(value, 'f', -1, 64)
	err := writer.Write([]string{"x", reqTsStr, rcvTsStr, valStr})
	if err != nil {
		log.Println(err)
	}
}

func (n *Node) exportMsgCount() {
	filename := "/var/log/extrema_propagation/msg_count.csv"
	writer := writers[filename]
	if writer == nil {
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Printf("failed to open/create file: %v", err)
			return
		}
		writer = csv.NewWriter(file)
		writers[filename] = writer
	}
	defer writer.Flush()
	tsStr := strconv.Itoa(int(time.Now().UnixNano()))
	peers.MessagesSentLock.Lock()
	sent := peers.MessagesSent
	peers.MessagesSentLock.Unlock()
	peers.MessagesRcvdLock.Lock()
	rcvd := peers.MessagesRcvd
	peers.MessagesRcvdLock.Unlock()
	sentStr := strconv.Itoa(sent)
	rcvdStr := strconv.Itoa(rcvd)
	err := writer.Write([]string{tsStr, sentStr, rcvdStr})
	if err != nil {
		log.Println(err)
	}
}
