// +build ignore

// loadtest fires concurrent order creation requests against the order-service
// and reports throughput, success rate, and latency percentiles.
//
// Usage:
//   go run scripts/loadtest.go -orders 1000 -concurrency 50 -url http://localhost:8081

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	numOrders   = flag.Int("orders", 100, "Total number of orders to create")
	concurrency = flag.Int("concurrency", 10, "Number of concurrent workers")
	baseURL     = flag.String("url", "http://localhost:8081", "Order service base URL")
)

var skus = []string{"SKU-WIDGET-A", "SKU-WIDGET-B", "SKU-GADGET-X", "SKU-GADGET-Y"}

type result struct {
	status  int
	latency time.Duration
	err     error
}

func main() {
	flag.Parse()

	fmt.Printf("🔥 SagaForge AI Load Test\n")
	fmt.Printf("   Orders:      %d\n", *numOrders)
	fmt.Printf("   Concurrency: %d\n", *concurrency)
	fmt.Printf("   Target:      %s\n\n", *baseURL)

	client := &http.Client{Timeout: 30 * time.Second}
	results := make([]result, *numOrders)

	var (
		wg        sync.WaitGroup
		succeeded int64
		failed    int64
		idx       int64
	)

	sem := make(chan struct{}, *concurrency)
	start := time.Now()

	for i := 0; i < *numOrders; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			i := int(atomic.AddInt64(&idx, 1) - 1)

			body := randomOrder()
			reqStart := time.Now()

			resp, err := client.Post(*baseURL+"/orders", "application/json", bytes.NewReader(body))
			latency := time.Since(reqStart)

			if err != nil {
				results[i] = result{err: err, latency: latency}
				atomic.AddInt64(&failed, 1)
				return
			}
			resp.Body.Close()

			results[i] = result{status: resp.StatusCode, latency: latency}
			if resp.StatusCode == 201 {
				atomic.AddInt64(&succeeded, 1)
			} else {
				atomic.AddInt64(&failed, 1)
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Collect latencies
	var latencies []time.Duration
	for _, r := range results {
		if r.err == nil {
			latencies = append(latencies, r.latency)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	// Report
	fmt.Printf("──────────────────────────────────────\n")
	fmt.Printf("📊 Results\n")
	fmt.Printf("──────────────────────────────────────\n")
	fmt.Printf("   Total:       %d orders\n", *numOrders)
	fmt.Printf("   Succeeded:   %d (%.1f%%)\n", succeeded, float64(succeeded)/float64(*numOrders)*100)
	fmt.Printf("   Failed:      %d (%.1f%%)\n", failed, float64(failed)/float64(*numOrders)*100)
	fmt.Printf("   Duration:    %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("   Throughput:  %.1f orders/sec\n", float64(*numOrders)/elapsed.Seconds())
	fmt.Printf("\n")

	if len(latencies) > 0 {
		fmt.Printf("⏱️  Latency\n")
		fmt.Printf("   p50:  %s\n", percentile(latencies, 50))
		fmt.Printf("   p90:  %s\n", percentile(latencies, 90))
		fmt.Printf("   p95:  %s\n", percentile(latencies, 95))
		fmt.Printf("   p99:  %s\n", percentile(latencies, 99))
		fmt.Printf("   max:  %s\n", latencies[len(latencies)-1].Round(time.Millisecond))
	}

	// Exit with error if >5% failure
	if float64(failed)/float64(*numOrders) > 0.05 {
		fmt.Printf("\n❌ Failure rate exceeds 5%% threshold\n")
		os.Exit(1)
	}
	fmt.Printf("\n✅ Load test passed\n")
}

func randomOrder() []byte {
	numItems := rand.Intn(3) + 1
	items := make([]map[string]any, numItems)
	for i := 0; i < numItems; i++ {
		items[i] = map[string]any{
			"sku":      skus[rand.Intn(len(skus))],
			"quantity": rand.Intn(5) + 1,
			"price":    float64(rand.Intn(100)+10) + 0.99,
		}
	}
	body, _ := json.Marshal(map[string]any{
		"customer_id": fmt.Sprintf("cust-load-%04d", rand.Intn(1000)),
		"items":       items,
	})
	return body
}

func percentile(sorted []time.Duration, pct int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := len(sorted) * pct / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx].Round(time.Millisecond)
}
