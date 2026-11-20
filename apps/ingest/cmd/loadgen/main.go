// Command loadgen drives POST /track at a receiver and reports throughput and
// latency, for comparing the Bun API with the Go receiver on one machine.
// Each request has its own client address and event name, so neither the
// dedupe lock nor one hot partition flatters the result.
//
//	go run ./cmd/loadgen -url http://127.0.0.1:3340 -client <id> -secret <secret> \
//	  -concurrency 100 -duration 20s
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"

func main() {
	target := flag.String("url", "http://127.0.0.1:3340", "receiver base URL")
	client := flag.String("client", "", "client id")
	secret := flag.String("secret", "", "client secret (server-style auth); empty sends an Origin instead")
	origin := flag.String("origin", "https://loadgen.example", "Origin for browser-style auth")
	concurrency := flag.Int("concurrency", 100, "parallel connections")
	duration := flag.Duration("duration", 20*time.Second, "how long to send")
	pid := flag.Int("pid", 0, "receiver process id, to report its CPU time")
	flag.Parse()
	if *client == "" {
		fmt.Fprintln(os.Stderr, "loadgen: -client is required")
		os.Exit(2)
	}

	transport := &http.Transport{MaxIdleConnsPerHost: *concurrency, MaxConnsPerHost: *concurrency}
	httpClient := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	var sent, failed atomic.Int64
	statuses := sync.Map{}
	latencies := make([][]time.Duration, *concurrency)
	cpuBefore := cpuSeconds(*pid)
	deadline := time.Now().Add(*duration)
	start := time.Now()
	var wg sync.WaitGroup
	for worker := range *concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; time.Now().Before(deadline); n++ {
				ip := fmt.Sprintf("81.%d.%d.%d", 2+worker%200, (n/250)%250, n%250)
				body := fmt.Sprintf(`{"type":"track","payload":{"name":"loadgen","properties":{"__path":"/page/%d","worker":%d,"n":%d,"tags":["a","b","c"]}}}`, n%50, worker, n)
				req, _ := http.NewRequest(http.MethodPost, *target+"/track", bytes.NewReader([]byte(body)))
				req.Header.Set("content-type", "application/json")
				req.Header.Set("openpanel-client-id", *client)
				req.Header.Set("user-agent", userAgent)
				req.Header.Set("x-client-ip", ip)
				if *secret != "" {
					req.Header.Set("openpanel-client-secret", *secret)
				} else {
					req.Header.Set("origin", *origin)
				}
				began := time.Now()
				resp, err := httpClient.Do(req)
				if err != nil {
					failed.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				latencies[worker] = append(latencies[worker], time.Since(began))
				count, _ := statuses.LoadOrStore(resp.StatusCode, new(atomic.Int64))
				count.(*atomic.Int64).Add(1)
				sent.Add(1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	cpuAfter := cpuSeconds(*pid)

	var all []time.Duration
	for _, list := range latencies {
		all = append(all, list...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[int(float64(len(all)-1)*p)]
	}
	fmt.Printf("requests %d in %.1fs = %.0f req/s, transport errors %d\n", sent.Load(), elapsed.Seconds(), float64(sent.Load())/elapsed.Seconds(), failed.Load())
	statuses.Range(func(key, value any) bool {
		fmt.Printf("  status %v: %d\n", key, value.(*atomic.Int64).Load())
		return true
	})
	fmt.Printf("latency p50 %v p90 %v p99 %v max %v\n", pct(0.5), pct(0.9), pct(0.99), pct(1))
	if *pid > 0 && cpuAfter >= cpuBefore {
		cpu := cpuAfter - cpuBefore
		fmt.Printf("receiver cpu %.1fs (%.0f%% of a core), %.2f ms cpu per request\n", cpu, 100*cpu/elapsed.Seconds(), 1000*cpu/float64(max(1, sent.Load())))
	}
}

// cpuSeconds reads a process's user+system time from ps, which works on
// macOS and Linux alike.
func cpuSeconds(pid int) float64 {
	if pid <= 0 {
		return 0
	}
	out, err := exec.Command("ps", "-o", "time=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	// [[dd-]hh:]mm:ss.cc
	text := strings.TrimSpace(string(out))
	days := 0.0
	if dash := strings.Index(text, "-"); dash >= 0 {
		days, _ = strconv.ParseFloat(text[:dash], 64)
		text = text[dash+1:]
	}
	total := 0.0
	for _, part := range strings.Split(text, ":") {
		value, _ := strconv.ParseFloat(part, 64)
		total = total*60 + value
	}
	return total + days*86400
}
