package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"

	cft "github.com/geektr-cloud/go-cloudflare-fastest-transport"
)

func main() {
	port := flag.Int("port", 443, "listen port")
	topN := flag.Int("top", 3, "number of CF nodes to maintain")
	file := flag.String("file", "cf-nodes.csv", "CSV file for node persistence")
	flag.Parse()

	log.Printf("initializing node set from %s...", *file)
	nodeSet := cft.NewCfNodeSetWithFile(*file)

	// Refresh if we have no active nodes
	list := nodeSet.List()
	active := 0
	for _, ns := range list {
		if !ns.IsBanned() {
			active++
		}
	}
	if active < *topN {
		log.Printf("refreshing nodes (have %d, need %d)...", active, *topN)
		if err := nodeSet.Refresh(*topN); err != nil {
			log.Fatalf("refresh failed: %v", err)
		}
	}

	list = nodeSet.List()
	var pool []string
	for _, ns := range list {
		if !ns.IsBanned() {
			pool = append(pool, string(ns.Node))
		}
	}
	if len(pool) == 0 {
		log.Fatal("no available CF nodes after refresh")
	}
	log.Printf("ready with %d nodes: %v", len(pool), pool)

	// Start SNI proxy
	addr := fmt.Sprintf(":%d", *port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
	log.Printf("listening on %s — route HTTPS traffic here to use CF optimized IPs", addr)

	var index atomic.Uint64
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(conn, pool, &index)
	}
}

// handleConn pipes traffic bidirectionally to a CF edge node on port 443.
func handleConn(clientConn net.Conn, pool []string, index *atomic.Uint64) {
	defer clientConn.Close()

	// Round-robin pick a CF node
	idx := int(index.Add(1)-1) % len(pool)
	ip := pool[idx]

	// Dial CF edge on port 443
	remoteAddr := fmt.Sprintf("%s:443", ip)
	remoteConn, err := net.DialTimeout("tcp", remoteAddr, 5*time.Second)
	if err != nil {
		log.Printf("dial %s failed: %v", remoteAddr, err)
		return
	}
	defer remoteConn.Close()

	// Pipe bidirectionally with proper half-close
	done := make(chan struct{}, 2)

	go func() {
		io.Copy(remoteConn, clientConn)
		if tc, ok := remoteConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
		done <- struct{}{}
	}()

	go func() {
		io.Copy(clientConn, remoteConn)
		if tc, ok := clientConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
		done <- struct{}{}
	}()

	<-done
	<-done
}
