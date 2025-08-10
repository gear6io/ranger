package sdk_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.uber.org/zap"

	"github.com/TFMV/icebox/pkg/sdk"
)

// ConnectExample demonstrates how to connect to an Icebox server
func ConnectExample() {
	// Create logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create connection options
	options := sdk.Options{
		Logger:   logger,
		Address:  "127.0.0.1:9000",
		Database: "default",
		User:     "default",
		Password: "",
		Settings: []sdk.Setting{
			sdk.SettingInt("max_execution_time", 30),
			sdk.SettingString("timezone", "UTC"),
		},
	}

	// Connect to server
	ctx := context.Background()
	client, err := sdk.Connect(ctx, options)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	fmt.Println("Connected to Icebox server")
}

// PingExample demonstrates how to ping the server
func PingExample() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	options := sdk.Options{
		Logger:  logger,
		Address: "127.0.0.1:9000",
	}

	ctx := context.Background()
	client, err := sdk.Connect(ctx, options)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// Ping the server
	if err := client.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping: %v", err)
	}

	fmt.Println("Server is responding")
}

// QueryExample demonstrates how to execute a query and read results
func QueryExample() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	options := sdk.Options{
		Logger:  logger,
		Address: "127.0.0.1:9000",
	}

	ctx := context.Background()
	client, err := sdk.Connect(ctx, options)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// Execute a query
	rows, err := client.Query(ctx, "SELECT id, name, value FROM test_table LIMIT 10")
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}
	fmt.Printf("Columns: %v\n", columns)

	// Iterate through results
	for rows.Next() {
		var id int
		var name string
		var value float64

		if err := rows.Scan(&id, &name, &value); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		fmt.Printf("Row: id=%d, name=%s, value=%.2f\n", id, name, value)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating rows: %v", err)
	}
}

// ExecExample demonstrates how to execute a query without returning results
func ExecExample() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	options := sdk.Options{
		Logger:  logger,
		Address: "127.0.0.1:9000",
	}

	ctx := context.Background()
	client, err := sdk.Connect(ctx, options)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// Create a table
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS test_table (
			id UInt32,
			name String,
			value Float64,
			created_at DateTime
		) ENGINE = Memory
	`

	if err := client.Exec(ctx, createTableSQL); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	fmt.Println("Table created successfully")
}

// BatchInsertExample demonstrates how to use batch insertion
func BatchInsertExample() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	options := sdk.Options{
		Logger:  logger,
		Address: "127.0.0.1:9000",
	}

	ctx := context.Background()
	client, err := sdk.Connect(ctx, options)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// Prepare batch
	batch, err := client.PrepareBatch(ctx, "INSERT INTO test_table (id, name, value, created_at)")
	if err != nil {
		log.Fatalf("Failed to prepare batch: %v", err)
	}

	// Add rows to batch
	now := time.Now()
	for i := 1; i <= 1000; i++ {
		err := batch.Append(
			i,                                     // id
			fmt.Sprintf("name_%d", i),             // name
			float64(i)*1.23,                       // value
			now.Add(time.Duration(i)*time.Second), // created_at
		)
		if err != nil {
			log.Fatalf("Failed to append row: %v", err)
		}
	}

	// Send batch
	if err := batch.Send(); err != nil {
		log.Fatalf("Failed to send batch: %v", err)
	}

	fmt.Printf("Successfully inserted %d rows\n", batch.Rows())
}

// BatchInsertWithColumnsExample demonstrates batch insertion with column-specific operations
func BatchInsertWithColumnsExample() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	options := sdk.Options{
		Logger:  logger,
		Address: "127.0.0.1:9000",
	}

	ctx := context.Background()
	client, err := sdk.Connect(ctx, options)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// Prepare batch
	batch, err := client.PrepareBatch(ctx, "INSERT INTO test_table (id, name, value, created_at)")
	if err != nil {
		log.Fatalf("Failed to prepare batch: %v", err)
	}

	// Get column references for direct manipulation
	idCol := batch.Column(0)    // id column
	nameCol := batch.Column(1)  // name column
	valueCol := batch.Column(2) // value column
	timeCol := batch.Column(3)  // created_at column

	// Add data using column-specific operations
	now := time.Now()
	for i := 1; i <= 100; i++ {
		if err := idCol.Append(i); err != nil {
			log.Fatalf("Failed to append id: %v", err)
		}
		if err := nameCol.Append(fmt.Sprintf("name_%d", i)); err != nil {
			log.Fatalf("Failed to append name: %v", err)
		}
		if err := valueCol.Append(float64(i) * 1.23); err != nil {
			log.Fatalf("Failed to append value: %v", err)
		}
		if err := timeCol.Append(now.Add(time.Duration(i) * time.Second)); err != nil {
			log.Fatalf("Failed to append time: %v", err)
		}
	}

	// Send batch
	if err := batch.Send(); err != nil {
		log.Fatalf("Failed to send batch: %v", err)
	}

	fmt.Printf("Successfully inserted %d rows using column operations\n", batch.Rows())
}

// WithSettingsExample demonstrates how to use settings
func WithSettingsExample() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	options := sdk.Options{
		Logger:  logger,
		Address: "127.0.0.1:9000",
		Settings: []sdk.Setting{
			sdk.SettingInt("max_execution_time", 60),
			sdk.SettingString("timezone", "UTC"),
			sdk.SettingString("date_time_input_format", "best_effort"),
			sdk.SettingInt("max_block_size", 10000),
		},
	}

	ctx := context.Background()
	client, err := sdk.Connect(ctx, options)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// Get server info
	serverInfo := client.ServerInfo()
	fmt.Printf("Connected to %s version %s\n", serverInfo.Name, serverInfo.Version)

	// Execute query with custom settings
	rows, err := client.Query(ctx, "SELECT 1 as test")
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	fmt.Println("Query executed with custom settings")
}

// ErrorHandlingExample demonstrates proper error handling
func ErrorHandlingExample() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	options := sdk.Options{
		Logger:  logger,
		Address: "127.0.0.1:9000",
	}

	ctx := context.Background()
	client, err := sdk.Connect(ctx, options)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// Try to execute a query that might fail
	rows, err := client.Query(ctx, "SELECT * FROM non_existent_table")
	if err != nil {
		// Check if it's an exception from the server
		if exception, ok := err.(*Exception); ok {
			fmt.Printf("Server exception: %s (code: %d)\n", exception.Message, exception.Code)
		} else {
			fmt.Printf("Connection error: %v\n", err)
		}
		return
	}
	defer rows.Close()

	// Process results if query succeeded
	fmt.Println("Query executed successfully")
}
