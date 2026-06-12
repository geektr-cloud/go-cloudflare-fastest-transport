# CLAUDE.md

## Project Overview

Go library for Cloudflare IP optimization (优选 IP). Discovers fastest Cloudflare edge nodes and provides an `http.RoundTripper` that routes traffic through them with automatic failover.

## Build & Test

```bash
go build ./...          # build
go test ./...           # full test suite (includes network tests, ~80s per phase)
go test -short ./...    # fast local-only tests (skips network)
go vet ./...            # lint
```

## Architecture

- `cfnode.go` — `CfNode` type with `TCPPing`, `HTTPPing`, `SpeedTest` methods
- `iprange.go` — Cloudflare IPv4 CIDR ranges, candidate IP generation
- `cfnodeset.go` — `CfNodeSet` managing node discovery (`Refresh`) and selection (`Filter`)
- `transport.go` — `http.RoundTripper` implementation with roundrobin + failover

## Key Design Decisions

- HTTP ping uses `https://cloudflare.com/cdn-cgi/trace` (works on all CF edge IPs)
- Speed test uses `https://speed.cloudflare.com/__down?bytes=10000000` (works on a subset of IPs, failures handled gracefully)
- Transport tests use `https://cdn.jsdelivr.net/npm/jquery@3.7.1/dist/jquery.min.js` (CF-fronted, broad anycast coverage)
- Ban duration: 1 hour. Consecutive failure threshold: 3.
- Refresh generates ~6000 candidate IPs from CF CIDR ranges, TCP pings all, HTTP pings top 5N, returns topN.

## Conventions

- Package name: `cftransport`
- No global state; all config via struct fields / method params
- `testing.Short()` guards network-dependent tests
- Keep TopN small in tests to avoid wasting time
