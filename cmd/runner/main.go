// runner starts all SagaForge AI microservices as child processes in a single
// container. This is the entrypoint for Railway (or any single-container)
// deployment. On SIGTERM/SIGINT it forwards the signal to every child and
// waits for graceful shutdown.
//
// The dashboard starts FIRST so the /health endpoint is available immediately
// for Railway's healthcheck. Kafka-dependent services start after the broker
// is reachable.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	fmt.Println("=== SagaForge AI — Runner ===")

	// Railway sets PORT for the public-facing service. The dashboard must bind to this.
	if port := os.Getenv("PORT"); port != "" {
		os.Setenv("DASHBOARD_PORT", port)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var cmds []*exec.Cmd

	startService := func(name string) {
		binPath := fmt.Sprintf("/app/bin/%s", name)
		cmd := exec.CommandContext(ctx, binPath)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Env = os.Environ()
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "[runner] WARN: start %s: %v\n", name, err)
			return
		}
		fmt.Printf("[runner] started %s (pid %d)\n", name, cmd.Process.Pid)

		mu.Lock()
		cmds = append(cmds, cmd)
		mu.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cmd.Wait(); err != nil {
				fmt.Fprintf(os.Stderr, "[runner] %s exited: %v\n", name, err)
			} else {
				fmt.Printf("[runner] %s exited cleanly\n", name)
			}
		}()
	}

	// ── Phase 1: Start dashboard + order-service immediately ──
	// Dashboard serves /health for Railway's healthcheck.
	// Order-service serves the HTTP API the dashboard calls.
	fmt.Println("[runner] phase 1: starting dashboard + order-service")
	startService("dashboard")
	startService("order-service")

	// ── Phase 2: Wait for Kafka, then start event-driven services ──
	go func() {
		waitForKafka()

		fmt.Println("[runner] phase 2: starting Kafka-dependent services")
		kafkaServices := []string{
			"inventory-service",
			"payment-service",
			"fulfillment-service",
			"saga-orchestrator",
			"ai-insights-service",
			"notification-service",
		}
		for _, svc := range kafkaServices {
			startService(svc)
		}
	}()

	// Block until we receive a termination signal.
	<-ctx.Done()
	fmt.Println("[runner] shutting down children…")

	// Forward SIGTERM to all children.
	mu.Lock()
	snapshot := make([]*exec.Cmd, len(cmds))
	copy(snapshot, cmds)
	mu.Unlock()

	for _, cmd := range snapshot {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
	}

	// Give children a grace window then force-kill.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		fmt.Println("[runner] all children stopped")
	case <-time.After(10 * time.Second):
		fmt.Println("[runner] force-killing remaining children")
		for _, cmd := range snapshot {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		}
	}
}

// waitForKafka blocks until the first Kafka broker is reachable (TCP handshake).
// Times out after 60 seconds and proceeds anyway.
func waitForKafka() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:19092"
	}
	broker := brokers
	for i, c := range brokers {
		if c == ',' {
			broker = brokers[:i]
			break
		}
	}

	fmt.Printf("[runner] waiting for Kafka at %s …\n", broker)
	for i := 0; i < 30; i++ {
		conn, err := net.DialTimeout("tcp", broker, 2*time.Second)
		if err == nil {
			conn.Close()
			fmt.Println("[runner] Kafka is reachable")
			return
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Fprintln(os.Stderr, "[runner] WARNING: Kafka not reachable after 60s — starting services anyway")
}
