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
	poolSize := flag.Int("pool", 20, "number of CF nodes to maintain in reservoir")
	file := flag.String("file", "cf-nodes.csv", "CSV file for node persistence")
	flag.Parse()

	log.Printf("initializing from %s (pool size %d)...", *file, *poolSize)
	mgr := cft.NewPoolManagerWithFile(*poolSize, *file)

	// Refresh if pool is underfilled
	mgr.RefreshPool()

	if mgr.PoolLen() == 0 {
		log.Fatal("no available CF nodes after refresh")
	}
	log.Printf("ready with %d nodes in pool", mgr.PoolLen())

	// Start TCP proxy
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
		go handleConn(conn, mgr, &index)
	}
}

// handleConn pipes traffic bidirectionally to a CF edge node on port 443.
func handleConn(clientConn net.Conn, mgr *cft.PoolManager, index *atomic.Uint64) {
	defer clientConn.Close()

	// Get one IP from pool
	ips := mgr.Take(1, index)
	if len(ips) == 0 {
		log.Printf("no available nodes")
		return
	}
	ip := ips[0]

	// Dial CF edge on port 443
	remoteAddr := fmt.Sprintf("%s:443", ip)
	remoteConn, err := net.DialTimeout("tcp", remoteAddr, 5*time.Second)
	if err != nil {
		log.Printf("dial %s failed: %v", remoteAddr, err)
		mgr.RecordFailure(cft.CfNode(ip))
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
