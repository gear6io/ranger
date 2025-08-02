package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	// Connect to Icebox native server
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Debug: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Test the connection with ping
	if err := conn.Ping(context.Background()); err != nil {
		log.Fatal(err)
	}

	fmt.Println("✅ Successfully connected to Icebox native server!")
	fmt.Println("✅ Ping/Pong functionality is working correctly!")
	fmt.Println("✅ Handshake and protocol are working!")

	fmt.Println("\n🎉 The ClickHouse Go client is now working with Icebox native server!")
	fmt.Println("   - Handshake: ✅ Working")
	fmt.Println("   - Ping/Pong: ✅ Working")
	fmt.Println("   - Protocol: ✅ Compatible")
	fmt.Println("\nNote: Query execution still needs format fixes, but the core protocol is working!")
}
