package config

import (
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	ListenIP   string
	ListenPort int
	PeersIDs   []string
	PeersIPs   []string
}

func LoadConfigFromEnv() Config {
	listenIP := os.Getenv("LISTEN_IP")
	listenHost := os.Getenv("LISTEN_HOST")
	listenPortStr := os.Getenv("LISTEN_PORT")

	if listenIP == "" && listenHost == "" {
		log.Fatalln("error: must set either LISTEN_IP or LISTEN_HOST")
	}

	if listenIP == "" {
		addrs, err := net.LookupHost(listenHost)
		if err != nil || len(addrs) == 0 {
			log.Fatalf("error resolving host %q: %v\n", listenHost, err)
		}
		listenIP = addrs[0]
	}

	if listenPortStr == "" {
		log.Fatalln("error: LISTEN_PORT is not set")
	}

	listenPort, err := strconv.Atoi(listenPortStr)
	if err != nil {
		log.Fatalf("invalid port %q: %v\n", listenPortStr, err)
	}

	c := Config{
		ListenIP:   listenIP,
		ListenPort: listenPort,
	}

	peerIDsStr := os.Getenv("PEER_IDS")
	peerIPsStr := os.Getenv("PEER_IPS")
	peerHostsStr := os.Getenv("PEER_HOSTS")

	if peerIDsStr == "" {
		log.Fatalln("error: PEER_IDS is not set")
	}

	peerIDs := splitAndTrim(peerIDsStr)
	peerIPs := splitAndTrim(peerIPsStr)
	peerHosts := splitAndTrim(peerHostsStr)

	maxLen := len(peerIDs)
	ensureSameLength(&peerIPs, maxLen)
	ensureSameLength(&peerHosts, maxLen)

	for i := range maxLen {
		id := peerIDs[i]
		ip := peerIPs[i]
		host := peerHosts[i]

		if ip == "" && host == "" {
			log.Fatalf("error: peer %q has neither IP nor host\n", id)
		}
		if ip == "" {
			addrs, err := net.LookupHost(host)
			if err != nil || len(addrs) == 0 {
				log.Fatalf("error resolving host %q for peer %q: %v\n", host, id, err)
			}
			ip = addrs[0]
		}
		c.PeersIDs = append(c.PeersIDs, id)
		c.PeersIPs = append(c.PeersIPs, ip)
	}
	if !c.isValid() {
		log.Fatalln("config invalid", c)
	}
	return c
}

func (c Config) isValid() bool {
	return len(c.PeersIDs) == len(c.PeersIPs)
}

func splitAndTrim(s string) []string {
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func ensureSameLength(slice *[]string, n int) {
	if len(*slice) < n {
		padding := make([]string, n-len(*slice))
		*slice = append(*slice, padding...)
	}
}
