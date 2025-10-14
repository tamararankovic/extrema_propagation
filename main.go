package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"slices"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
	"github.com/caarlos0/env"
)

const AGG_MSG_TYPE data.MessageType = data.UNKNOWN + 1

type Msg struct {
	Epoch       int
	ValuesSum   []float64
	ValuesCount []float64
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
	Hyparview   *hyparview.HyParView
	Lock        *sync.Mutex
	Logger      *log.Logger
}

func (n *Node) receive(msg Msg) {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	if msg.Epoch < n.Epoch {
		return
	}

	if msg.Epoch > n.Epoch {
		n.Logger.Printf("[%s] switching to epoch %d", n.ID, msg.Epoch)
		n.Epoch = msg.Epoch
		n.Round = 0
		n.resetMinima()
		n.NoNews = 0
		n.Converged = false
	}

	if len(msg.ValuesSum) != n.K {
		n.Logger.Println("length mismatch", len(msg.ValuesSum))
		return
	}
	if len(msg.ValuesCount) != n.K {
		n.Logger.Println("length mismatch", len(msg.ValuesCount))
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
			n.Logger.Printf("[%s] starting epoch %d", n.ID, n.Epoch)
		}

		estimate := n.estimate()
		n.Logger.Printf("[%s] epoch=%d estimate=%.3f", n.ID, n.Epoch, estimate)

		if n.Converged {
			n.Lock.Unlock()
			continue
		}

		payload := Msg{
			Epoch:       n.Epoch,
			ValuesSum:   slices.Clone(n.MinSum),
			ValuesCount: slices.Clone(n.MinCount),
		}
		n.Lock.Unlock()

		hvMsg := data.Message{
			Type:    AGG_MSG_TYPE,
			Payload: payload,
		}

		peers := n.Hyparview.GetPeers(1000)
		for _, peer := range peers {
			if err := peer.Conn.Send(hvMsg); err != nil {
				n.Logger.Println(err)
			}
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
	hvConfig := hyparview.Config{}
	if err := env.Parse(&hvConfig); err != nil {
		log.Fatal(err)
	}

	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		log.Fatal(err)
	}

	self := data.Node{
		ID:            cfg.NodeID,
		ListenAddress: cfg.ListenAddr,
	}

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

	gnConnManager := transport.NewConnManager(
		transport.NewTCPConn,
		transport.AcceptTcpConnsFn(self.ListenAddress),
	)

	hv, err := hyparview.NewHyParView(hvConfig, self, gnConnManager, logger)
	if err != nil {
		log.Fatal(err)
	}

	tAgg, err := strconv.Atoi(cfg.TAgg)
	if err != nil {
		logger.Fatal(err)
	}

	val, err := strconv.Atoi(strings.Split(cfg.NodeID, "_")[2])
	if err != nil {
		logger.Fatal(err)
	}

	node := &Node{
		ID:          cfg.NodeID,
		Value:       float64(val),
		TAgg:        tAgg,
		K:           cfg.K,
		MinSum:      make([]float64, cfg.K),
		MinCount:    make([]float64, cfg.K),
		Hyparview:   hv,
		Lock:        &sync.Mutex{},
		Logger:      logger,
		EpochLength: cfg.EpochLength,
		MinNoNews:   cfg.MinNoNews,
	}

	node.resetMinima()

	hv.AddClientMsgHandler(AGG_MSG_TYPE, func(msgBytes []byte, sender hyparview.Peer) {
		msg := Msg{}
		if err := transport.Deserialize(msgBytes, &msg); err != nil {
			logger.Println(node.ID, "-", "Error unmarshaling message:", err)
			return
		}
		node.receive(msg)
	})

	go func() {
		for range time.NewTicker(time.Second).C {
			node.exportMsgCount()
			node.Lock.Lock()
			value := node.estimate()
			node.Lock.Unlock()
			node.exportResult(value, 0, time.Now().UnixNano())
		}
	}()

	if err := hv.Join(cfg.ContactID, cfg.ContactAddr); err != nil {
		logger.Fatal(err)
	}

	go node.send()

	r := http.NewServeMux()
	r.HandleFunc("POST /metrics", node.setMetricsHandler)
	log.Println("Metrics server listening")

	go func() {
		log.Fatal(http.ListenAndServe(strings.Split(os.Getenv("LISTEN_ADDR"), ":")[0]+":9200", r))
	}()

	r2 := http.NewServeMux()
	r2.HandleFunc("GET /state", node.StateHandler)
	log.Println("State server listening on :5001/state")
	log.Fatal(http.ListenAndServe(strings.Split(os.Getenv("LISTEN_ADDR"), ":")[0]+":5001", r2))
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
		n.Logger.Println(err)
	} else {
		n.Logger.Println("new value", val)
		n.Value = val
	}
	w.WriteHeader(http.StatusOK)
}

var writers map[string]*csv.Writer = map[string]*csv.Writer{}

func (n *Node) exportResult(value float64, reqTimestamp, rcvTimestamp int64) {
	name := "value"
	filename := fmt.Sprintf("/var/log/fu/results/%s.csv", name)
	// defer file.Close()
	writer := writers[filename]
	if writer == nil {
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			n.Logger.Printf("failed to open/create file: %v", err)
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
		n.Logger.Println(err)
	}
}

func (n *Node) exportMsgCount() {
	filename := "/var/log/fu/results/msg_count.csv"
	// defer file.Close()
	writer := writers[filename]
	if writer == nil {
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			n.Logger.Printf("failed to open/create file: %v", err)
			return
		}
		writer = csv.NewWriter(file)
		writers[filename] = writer
	}
	defer writer.Flush()
	tsStr := strconv.Itoa(int(time.Now().UnixNano()))
	transport.MessagesSentLock.Lock()
	sent := transport.MessagesSent - transport.MessagesSentSub
	transport.MessagesSentLock.Unlock()
	transport.MessagesRcvdLock.Lock()
	rcvd := transport.MessagesRcvd - transport.MessagesRcvdSub
	transport.MessagesRcvdLock.Unlock()
	sentStr := strconv.Itoa(sent)
	rcvdStr := strconv.Itoa(rcvd)
	err := writer.Write([]string{tsStr, sentStr, rcvdStr})
	if err != nil {
		n.Logger.Println(err)
	}
}
