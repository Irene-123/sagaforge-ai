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
		fmt.Printf("connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	rows, _ := conn.Query(context.Background(), "SELECT sku, available_qty FROM inventory_stock ORDER BY sku")
	defer rows.Close()
	fmt.Println("📦 Inventory stock:")
	for rows.Next() {
		var sku string
		var qty int
		_ = rows.Scan(&sku, &qty)
		fmt.Printf("  %s: %d units\n", sku, qty)
	}

	var count int
	_ = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM sagas").Scan(&count)
	fmt.Printf("\n🔄 Sagas: %d\n", count)
	_ = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM orders").Scan(&count)
	fmt.Printf("📋 Orders: %d\n", count)
}
