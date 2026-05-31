// +build ignore

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		fmt.Println("DATABASE_URL not set")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		fmt.Printf("❌ Connection failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	fmt.Println("✅ Connected to Supabase")

	sql, err := os.ReadFile("migrations/001_init.sql")
	if err != nil {
		fmt.Printf("❌ Read migration: %v\n", err)
		os.Exit(1)
	}

	if _, err := conn.Exec(ctx, string(sql)); err != nil {
		fmt.Printf("❌ Migration failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ Migration complete — all tables created")

	// Quick verify
	rows, _ := conn.Query(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'public'
		ORDER BY table_name
	`)
	defer rows.Close()
	fmt.Println("\nPublic tables:")
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		fmt.Printf("  • %s\n", name)
	}
}
