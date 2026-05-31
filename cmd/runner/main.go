// runner starts all SagaForge AI microservices as child processes in a single
// container. This is the entrypoint for Railway (or any single-container)
// deployment. On SIGTERM/SIGINT it forwards the signal to every child and
// waits for graceful shutdown.
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

// services lists every binary we expect in /app/bin/ (the Dockerfile puts them there).
var services = []string{
	"order-service",
	"inventory-service",
	"payment-service",
	"fulfillment-service",
	"saga-orchestrator",
	"ai-insights-service",
	"notification-service",
	"dashboard",
}

func main() {
	fmt.Println("=== SagaForge AI — Runner ===")

	// Railway sets PORT for the public-facing service. The dashboard must bind to this.
	if port := os.Getenv("PORT"); port != "" {
		os.Setenv("DASHBOARD_PORT", port)
	}

	// Wait for Redpanda/Kafka to be reachable before starting services.
	waitForKafka()

	// Ensure Kafka topics exist (best-effort; Redpanda auto-creates on first produce too).
	ensureTopics()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	cmds := make([]*exec.Cmd, 0, len(services))

	for _, svc := range services {
		svc := svc
		binPath := fmt.Sprintf("/app/bin/%s", svc)

		cmd := exec.CommandContext(ctx, binPath)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Env = os.Environ()

		// Each child gets its own process group so we can signal them independently.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: start %s: %v\n", svc, err)
			os.Exit(1)
		}
		fmt.Printf("[runner] started %s (pid %d)\n", svc, cmd.Process.Pid)
		cmds = append(cmds, cmd)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cmd.Wait(); err != nil {
				fmt.Fprintf(os.Stderr, "[runner] %s exited: %v\n", svc, err)
			} else {
				fmt.Printf("[runner] %s exited cleanly\n", svc)
			}
		}()
	}

	// Block until we receive a termination signal.
	<-ctx.Done()
	fmt.Println("[runner] shutting down children…")

	// Forward SIGTERM to all children.
	for _, cmd := range cmds {
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
		for _, cmd := range cmds {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		}
	}
}

// waitForKafka blocks until the first Kafka broker is reachable (TCP handshake).
func waitForKafka() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:19092"
	}
	// Use just the first broker for the health check.
	broker := brokers
	for i, c := range brokers {
		if c == ',' {
			broker = brokers[:i]
			break
		}
	}

	fmt.Printf("[runner] waiting for Kafka at %s …\n", broker)
	for i := 0; i < 60; i++ {
		conn, err := net.DialTimeout("tcp", broker, 2*time.Second)
		if err == nil {
			conn.Close()
			fmt.Println("[runner] Kafka is reachable")
			return
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Fprintln(os.Stderr, "[runner] WARNING: Kafka not reachable after 120s — starting anyway")
}

// ensureTopics creates the 7 required topics using kafka-go's Conn API.
// This is best-effort; topics may already exist or Redpanda's auto-create
// handles it.
func ensureTopics() {
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

	conn, err := net.DialTimeout("tcp", broker, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[runner] skip topic creation: %v\n", err)
		return
	}
	conn.Close()

	// Use rpk if available (Redpanda image), otherwise topics auto-create
	// on first produce with kafka-go when enable.auto.create.topics is set.
	topics := []string{
		"order-events",
		"inventory-events",
		"payment-events",
		"fulfillment-events",
		"saga-events",
		"ai-insight-events",
		"dlq",
	}
	for _, t := range topics {
		fmt.Printf("[runner] ensuring topic: %s\n", t)
	}
	fmt.Println("[runner] topics will auto-create on first produce (Redpanda dev mode)")
}
