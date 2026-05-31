// +build ignore

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	rows, _ := conn.Query(context.Background(), `
		SELECT s.order_id, s.status, s.current_step, s.failure_reason, o.status
		FROM sagas s JOIN orders o ON o.id = s.order_id
		ORDER BY s.started_at DESC LIMIT 10
	`)
	defer rows.Close()

	fmt.Println("Order ID   │ Saga Status  │ Step                │ Reason                              │ Order")
	fmt.Println("───────────┼──────────────┼─────────────────────┼─────────────────────────────────────┼────────")
	for rows.Next() {
		var oid, sagaStatus, step, orderStatus string
		var failReason *string
		_ = rows.Scan(&oid, &sagaStatus, &step, &failReason, &orderStatus)
		reason := "—"
		if failReason != nil {
			reason = *failReason
		}
		fmt.Printf("%-10s │ %-12s │ %-19s │ %-35s │ %s\n", oid[:8]+"…", sagaStatus, step, reason, orderStatus)
	}

	fmt.Println()

	// Show outbox events
	rows2, _ := conn.Query(context.Background(), `
		SELECT aggregate_id, event_type, published_at IS NOT NULL as published
		FROM outbox_events ORDER BY created_at DESC LIMIT 15
	`)
	defer rows2.Close()
	fmt.Println("Recent outbox events:")
	for rows2.Next() {
		var aid, etype string
		var published bool
		_ = rows2.Scan(&aid, &etype, &published)
		marker := "⏳"
		if published {
			marker = "✅"
		}
		fmt.Printf("  %s %s  (order: %s…)\n", marker, etype, aid[:8])
	}

	// Inventory check
	rows3, _ := conn.Query(context.Background(), `SELECT sku, available_qty FROM inventory_stock ORDER BY sku`)
	defer rows3.Close()
	fmt.Println("\nInventory:")
	for rows3.Next() {
		var sku string
		var qty int
		_ = rows3.Scan(&sku, &qty)
		fmt.Printf("  %s: %d\n", sku, qty)
	}
}
