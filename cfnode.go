package cftransport

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/VividCortex/ewma"
)

const (
	defaultPort      = 443
	defaultPingTimes = 4
	downloadBufSize  = 1024
	// HTTPPingURL is used for HTTP ping — works on all Cloudflare edge IPs.
	defaultHTTPPingURL = "https://cloudflare.com/cdn-cgi/trace"
	// SpeedTestURL is used for download speed measurement.
	defaultSpeedTestURL = "https://speed.cloudflare.com/__down?bytes=10000000"
)

// CfNode represents a Cloudflare edge node IP address.
type CfNode string

// addr returns the dial address with port.
func (n CfNode) addr() string {
	ip := string(n)
	if strings.Contains(ip, ":") && !strings.HasPrefix(ip, "[") {
		return fmt.Sprintf("[%s]:%d", ip, defaultPort)
	}
	if strings.Contains(ip, "]") {
		return ip // already has brackets and port
	}
	return fmt.Sprintf("%s:%d", ip, defaultPort)
}

// TCPPing performs multiple TCP connection tests and returns
// the average delay, packet loss rate, and any error.
func (n CfNode) TCPPing(timeout time.Duration) (delay time.Duration, lossRate float64, err error) {
	addr := n.addr()
	var totalDelay time.Duration
	var received int

	for i := 0; i < defaultPingTimes; i++ {
		start := time.Now()
		conn, dialErr := net.DialTimeout("tcp", addr, timeout)
		if dialErr != nil {
			continue
		}
		conn.Close()
		received++
		totalDelay += time.Since(start)
	}

	if received == 0 {
		return 0, 1.0, fmt.Errorf("all %d TCP pings to %s failed", defaultPingTimes, addr)
	}

	delay = totalDelay / time.Duration(received)
	lossRate = float64(defaultPingTimes-received) / float64(defaultPingTimes)
	return delay, lossRate, nil
}

// HTTPPing performs multiple HTTP HEAD requests through the node and returns
// the average delay, packet loss rate, and any error.
func (n CfNode) HTTPPing(timeout time.Duration) (delay time.Duration, lossRate float64, err error) {
	ip := string(n)
	ipAddr := &net.IPAddr{IP: net.ParseIP(ip)}
	if ipAddr.IP == nil {
		return 0, 1.0, fmt.Errorf("invalid IP: %s", ip)
	}

	dialCtx := makeDialContext(ipAddr)
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: dialCtx,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	defer client.CloseIdleConnections()

	// First request to validate connectivity
	req, reqErr := http.NewRequest(http.MethodGet, defaultHTTPPingURL, nil)
	if reqErr != nil {
		return 0, 1.0, reqErr
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, doErr := client.Do(req)
	if doErr != nil {
		return 0, 1.0, doErr
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 301 && resp.StatusCode != 302 {
		return 0, 1.0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Measure latency over multiple requests
	var totalDelay time.Duration
	var received int
	for i := 0; i < defaultPingTimes; i++ {
		req, _ := http.NewRequest(http.MethodGet, defaultHTTPPingURL, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0")
		if i == defaultPingTimes-1 {
			req.Header.Set("Connection", "close")
		}
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		received++
		totalDelay += time.Since(start)
	}

	if received == 0 {
		return 0, 1.0, fmt.Errorf("all HTTP pings to %s failed", ip)
	}

	delay = totalDelay / time.Duration(received)
	lossRate = float64(defaultPingTimes-received) / float64(defaultPingTimes)
	return delay, lossRate, nil
}

// SpeedTest measures download speed through the node.
// Returns speed in bytes/sec.
func (n CfNode) SpeedTest(timeout time.Duration) (speed float64, err error) {
	ip := string(n)
	ipAddr := &net.IPAddr{IP: net.ParseIP(ip)}
	if ipAddr.IP == nil {
		return 0, fmt.Errorf("invalid IP: %s", ip)
	}

	dialCtx := makeDialContext(ipAddr)
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: dialCtx,
		},
	}
	defer client.CloseIdleConnections()

	req, reqErr := http.NewRequest(http.MethodGet, defaultSpeedTestURL, nil)
	if reqErr != nil {
		return 0, reqErr
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")

	resp, doErr := client.Do(req)
	if doErr != nil {
		return 0, doErr
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	timeStart := time.Now()
	timeEnd := timeStart.Add(timeout)
	contentLength := resp.ContentLength
	buffer := make([]byte, downloadBufSize)

	var (
		contentRead     int64
		timeSlice             = timeout / 100
		timeCounter           = 1
		lastContentRead int64
	)

	nextTime := timeStart.Add(timeSlice * time.Duration(timeCounter))
	e := ewma.NewMovingAverage()

	for contentLength != contentRead {
		now := time.Now()
		if now.After(nextTime) {
			timeCounter++
			nextTime = timeStart.Add(timeSlice * time.Duration(timeCounter))
			e.Add(float64(contentRead - lastContentRead))
			lastContentRead = contentRead
		}
		if now.After(timeEnd) {
			break
		}
		bufRead, readErr := resp.Body.Read(buffer)
		if readErr != nil {
			if readErr != io.EOF {
				break
			}
			if contentLength == -1 {
				break
			}
			lastSlice := timeStart.Add(timeSlice * time.Duration(timeCounter - 1))
			elapsed := float64(now.Sub(lastSlice)) / float64(timeSlice)
			if elapsed > 0 {
				e.Add(float64(contentRead-lastContentRead) / elapsed)
			}
		}
		contentRead += int64(bufRead)
	}

	// Convert EWMA value to bytes/sec
	speed = e.Value() / (timeout.Seconds() / 120)
	return speed, nil
}

func makeDialContext(ip *net.IPAddr) func(ctx context.Context, network, address string) (net.Conn, error) {
	var addr string
	if strings.Contains(ip.String(), ":") {
		addr = fmt.Sprintf("[%s]:%d", ip.String(), defaultPort)
	} else {
		addr = fmt.Sprintf("%s:%d", ip.String(), defaultPort)
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
}
