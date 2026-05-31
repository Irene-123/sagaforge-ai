// +build ignore

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	conn, _ := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	defer conn.Close(context.Background())

	var total, completed, failed, compensated, inProgress int
	_ = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM sagas").Scan(&total)
	_ = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM sagas WHERE status='completed'").Scan(&completed)
	_ = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM sagas WHERE status='failed'").Scan(&failed)
	_ = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM sagas WHERE status='compensated'").Scan(&compensated)
	_ = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM sagas WHERE status IN ('started','in_progress','compensating')").Scan(&inProgress)

	fmt.Printf("📊 Saga Summary\n")
	fmt.Printf("   Total:        %d\n", total)
	fmt.Printf("   ✅ Completed:  %d\n", completed)
	fmt.Printf("   ❌ Failed:     %d\n", failed)
	fmt.Printf("   🔄 Compensated: %d\n", compensated)
	fmt.Printf("   ⏳ In Progress: %d\n", inProgress)

	rows, _ := conn.Query(context.Background(), "SELECT order_id, status, failure_reason FROM sagas WHERE status IN ('failed','compensated') LIMIT 5")
	defer rows.Close()
	for rows.Next() {
		var oid, s string
		var r *string
		_ = rows.Scan(&oid, &s, &r)
		reason := "—"
		if r != nil {
			reason = *r
		}
		fmt.Printf("\n   🔥 %s: %s — %s\n", oid[:8], s, reason)
	}
}
