# go-cloudflare-fastest-transport

Go library for Cloudflare IP optimization. Automatically discovers the fastest Cloudflare edge nodes from your location and provides an `http.RoundTripper` that load-balances traffic across them with automatic failover.

## Install

```bash
go get github.com/geektr-cloud/go-cloudflare-fastest-transport
```

## Usage

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "time"

    cft "github.com/geektr-cloud/go-cloudflare-fastest-transport"
)

func main() {
    // 1. Create a node set and discover best nodes
    nodeSet := cft.NewCfNodeSet()
    if err := nodeSet.Refresh(10); err != nil {
        panic(err)
    }

    // 2. Create a Transport with filtering options
    transport := nodeSet.Transport(cft.FilterOptions{
        EliminateDelay: 500 * time.Millisecond,
        TargetDelay:    200 * time.Millisecond,
        EliminateSpeed: 1 * 1024 * 1024,  // 1 MB/s
        TargetSpeed:    5 * 1024 * 1024,   // 5 MB/s
        TopN:           3,
    })

    // 3. Use as http.Client transport
    client := &http.Client{Transport: transport}
    resp, err := client.Get("https://your-cf-site.com/api/data")
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()
    body, _ := io.ReadAll(resp.Body)
    fmt.Println(string(body))
}
```

## How It Works

### Node Discovery (`Refresh`)

1. Generates ~6000 candidate IPs from Cloudflare's published IPv4 CIDR ranges
2. TCP ping all candidates concurrently (200 goroutines) to find reachable nodes
3. HTTP ping the top 5×N nodes to measure real latency
4. Returns the best N nodes, respecting any existing ban list

### Node Selection (`Filter`)

Tests nodes against speed/delay thresholds:
- Nodes below `EliminateSpeed` or above `EliminateDelay` are banned for 1 hour
- Nodes above `TargetSpeed` or below `TargetDelay` are accepted immediately
- If not enough nodes meet targets, applies a golden-ratio (0.618) fallback threshold

### Transport (`http.RoundTripper`)

- Round-robin load balancing across selected nodes
- Tracks consecutive failures per node
- 3 consecutive failures → ban for 1 hour, remove from pool
- When the last node fails, automatically re-filters for fresh nodes

## API

### CfNode

```go
type CfNode string

func (n CfNode) TCPPing(timeout time.Duration) (delay time.Duration, lossRate float64, err error)
func (n CfNode) HTTPPing(timeout time.Duration) (delay time.Duration, lossRate float64, err error)
func (n CfNode) SpeedTest(timeout time.Duration) (speed float64, err error)
```

### CfNodeSet

```go
func NewCfNodeSet() *CfNodeSet
func (s *CfNodeSet) Refresh(topN int) error
func (s *CfNodeSet) Filter(opts FilterOptions) []string
func (s *CfNodeSet) Transport(opts FilterOptions) *Transport
func (s *CfNodeSet) Ban(node CfNode)
func (s *CfNodeSet) List() []CfNodeStatus
```

### FilterOptions

```go
type FilterOptions struct {
    EliminateSpeed float64       // bytes/s — below this, ban
    TargetSpeed    float64       // bytes/s — above this, accept
    EliminateDelay time.Duration // above this, ban
    TargetDelay    time.Duration // below this, accept
    TopN           int
}
```

## Testing

```bash
go test ./...           # full suite with real network tests (~80s)
go test -short ./...    # local-only tests (fast)
```

## License

MIT
