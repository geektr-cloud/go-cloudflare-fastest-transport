package cftransport

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const testURL = "https://cdn.jsdelivr.net/npm/jquery@3.7.1/dist/jquery.min.js"

func TestTransport_RealRoundRobin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	m := NewPoolManager(3)
	if err := m.Refresh(); err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	tr := m.Transport(TransportOptions{
		UpstreamCount:  2,
		EliminateDelay: 10 * time.Second,
	})

	client := &http.Client{Transport: tr, Timeout: 15 * time.Second}

	var successes int
	for i := 0; i < 4; i++ {
		resp, err := client.Get(testURL)
		if err != nil {
			t.Logf("request %d failed: %v", i, err)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 {
			successes++
		}
	}

	if successes == 0 {
		t.Fatal("all requests failed")
	}
	t.Logf("round-robin: %d/4 succeeded, pool=%d, upstreams=%d",
		successes, tr.PoolSize(), tr.UpstreamSize())
}

func TestTransport_FailoverAndBan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	m := NewPoolManager(3)
	m.setPool(newPool([]string{"192.0.2.1"}))

	tr := m.Transport(TransportOptions{
		UpstreamCount:  1,
		EliminateDelay: 1 * time.Second,
	})

	client := &http.Client{Transport: tr, Timeout: 2 * time.Second}

	for i := 0; i < 3; i++ {
		_, err := client.Get(testURL)
		if err == nil {
			t.Fatal("expected error to unreachable IP")
		}
	}

	if tr.PoolSize() != 0 {
		t.Errorf("expected empty pool, got %d", tr.PoolSize())
	}
}

func TestTransport_LocalRoundRobin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()

	m := NewPoolManager(5)
	m.setPool(newPool([]string{"127.0.0.1"}))

	tr := m.Transport(TransportOptions{
		UpstreamCount:  1,
		EliminateDelay: 5 * time.Second,
	})

	client := &http.Client{Transport: tr}
	for i := 0; i < 4; i++ {
		resp, err := client.Get(server.URL)
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		resp.Body.Close()
	}
}

func TestTransport_LocalBanOnFailure(t *testing.T) {
	closedListener, _ := net.Listen("tcp", "127.0.0.1:0")
	closedPort := closedListener.Addr().(*net.TCPAddr).Port
	closedListener.Close()

	m := NewPoolManager(5)
	m.setPool(newPool([]string{"127.0.0.1"}))

	tr := m.Transport(TransportOptions{
		UpstreamCount:  1,
		EliminateDelay: 2 * time.Second,
	})

	client := &http.Client{Transport: tr, Timeout: 1 * time.Second}
	url := fmt.Sprintf("http://127.0.0.1:%d/", closedPort)

	for i := 0; i < 3; i++ {
		client.Get(url)
	}

	if tr.PoolSize() != 0 {
		t.Errorf("expected empty pool after 3 failures, got %d", tr.PoolSize())
	}
}
