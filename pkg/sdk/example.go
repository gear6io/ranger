package sdk

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.uber.org/zap"
)

// ExampleBasicUsage demonstrates basic SDK usage
func ExampleBasicUsage() {
	// Create logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create options
	opt := &Options{
		Logger: logger,
		Addr:   []string{"127.0.0.1:9000"},
		Auth: Auth{
			Username: "default",
			Password: "",
			Database: "default",
		},
		Settings: Settings{
			"max_execution_time": 30,
			"timezone":           "UTC",
		},
	}

	// Create client
	client, err := NewClient(opt)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test connection
	if err := client.Ping(context.Background()); err != nil {
		log.Fatalf("Failed to ping: %v", err)
	}

	fmt.Println("Connected to Icebox server")
}

// ExampleDSN demonstrates DSN parsing
func ExampleDSN() {
	// Parse DSN
	dsn := "icebox://user:password@localhost:9000/mydb?max_execution_time=60&timezone=UTC"
	opt, err := ParseDSN(dsn)
	if err != nil {
		log.Fatalf("Failed to parse DSN: %v", err)
	}

	// Create client from DSN
	client, err := Open(opt)
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer client.Close()

	fmt.Printf("Connected to %s\n", opt.Addr[0])
}

// ExampleConnectionPooling demonstrates connection pooling
func ExampleConnectionPooling() {
	opt := &Options{
		Addr:             []string{"127.0.0.1:9000"},
		MaxOpenConns:     20,
		MaxIdleConns:     10,
		ConnMaxLifetime:  30 * time.Minute,
		ConnOpenStrategy: ConnOpenRoundRobin,
	}

	client, err := NewClient(opt)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Get connection stats
	stats := client.Stats()
	fmt.Printf("Connection pool stats: %+v\n", stats)
}

// ExampleCompression demonstrates compression usage
func ExampleCompression() {
	opt := &Options{
		Addr: []string{"127.0.0.1:9000"},
		Compression: &Compression{
			Method: CompressionLZ4,
			Level:  1,
		},
	}

	client, err := NewClient(opt)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	fmt.Println("Client created with LZ4 compression")
}

// ExampleQuery demonstrates query execution
func ExampleQuery() {
	client, err := NewClient(&Options{
		Addr: []string{"127.0.0.1:9000"},
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Execute query
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
}

// ExampleQueryRow demonstrates single row query
func ExampleQueryRow() {
	client, err := NewClient(&Options{
		Addr: []string{"127.0.0.1:9000"},
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Query single row
	row := client.QueryRow(ctx, "SELECT COUNT(*) FROM test_table")

	var count int
	if err := row.Scan(&count); err != nil {
		log.Fatalf("Failed to scan row: %v", err)
	}

	fmt.Printf("Total rows: %d\n", count)
}

// ExampleExec demonstrates command execution
func ExampleExec() {
	client, err := NewClient(&Options{
		Addr: []string{"127.0.0.1:9000"},
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Create table
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

// ExampleBatchInsert demonstrates batch insertion
func ExampleBatchInsert() {
	client, err := NewClient(&Options{
		Addr: []string{"127.0.0.1:9000"},
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

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

// ExampleBatchInsertWithOptions demonstrates batch insertion with options
func ExampleBatchInsertWithOptions() {
	client, err := NewClient(&Options{
		Addr: []string{"127.0.0.1:9000"},
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Prepare batch with options
	batch, err := client.PrepareBatch(ctx, "INSERT INTO test_table (id, name, value, created_at)",
		WithCloseOnFlush(),
		WithReleaseConnection(),
	)
	if err != nil {
		log.Fatalf("Failed to prepare batch: %v", err)
	}

	// Add data
	now := time.Now()
	for i := 1; i <= 100; i++ {
		batch.Append(i, fmt.Sprintf("name_%d", i), float64(i)*1.23, now)
	}

	// Flush batch
	if err := batch.Flush(); err != nil {
		log.Fatalf("Failed to flush batch: %v", err)
	}

	fmt.Printf("Successfully inserted %d rows with options\n", batch.Rows())
}

// ExampleColumnOperations demonstrates column-level batch operations
func ExampleColumnOperations() {
	client, err := NewClient(&Options{
		Addr: []string{"127.0.0.1:9000"},
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

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

// ExampleAsyncInsert demonstrates asynchronous insertion
func ExampleAsyncInsert() {
	client, err := NewClient(&Options{
		Addr: []string{"127.0.0.1:9000"},
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Async insert without waiting
	err = client.AsyncInsert(ctx, "INSERT INTO test_table (id, name) VALUES (?, ?)", false, 1, "async_test")
	if err != nil {
		log.Fatalf("Failed to async insert: %v", err)
	}

	fmt.Println("Async insert completed")
}

// ExampleSettings demonstrates settings usage
func ExampleSettings() {
	opt := &Options{
		Addr: []string{"127.0.0.1:9000"},
		Settings: Settings{
			"max_execution_time":                   60,
			"timezone":                             "UTC",
			"date_time_input_format":               "best_effort",
			"max_block_size":                       10000,
			"enable_optimize_predicate_expression": true,
		},
	}

	client, err := NewClient(opt)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Get server version
	version, err := client.ServerVersion()
	if err != nil {
		log.Fatalf("Failed to get server version: %v", err)
	}

	fmt.Printf("Connected to %s version %s\n", version.Name, version.Version)
}

// ExampleDatabaseSQL demonstrates database/sql compatibility
func ExampleDatabaseSQL() {
	// Open database using DSN
	db, err := OpenDB(&Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: Auth{
			Username: "default",
			Database: "default",
		},
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	// Execute query
	rows, err := db.Query("SELECT id, name FROM test_table LIMIT 5")
	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	// Iterate results
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatalf("Failed to scan: %v", err)
		}
		fmt.Printf("Row: id=%d, name=%s\n", id, name)
	}

	fmt.Println("Database/sql compatibility test completed")
}

// ExampleErrorHandling demonstrates error handling
func ExampleErrorHandling() {
	client, err := NewClient(&Options{
		Addr: []string{"127.0.0.1:9000"},
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

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

// ExampleContext demonstrates context usage
func ExampleContext() {
	client, err := NewClient(&Options{
		Addr: []string{"127.0.0.1:9000"},
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Execute query with context
	rows, err := client.Query(ctx, "SELECT * FROM test_table")
	if err != nil {
		if err == context.DeadlineExceeded {
			fmt.Println("Query timed out")
		} else {
			fmt.Printf("Query failed: %v\n", err)
		}
		return
	}
	defer rows.Close()

	fmt.Println("Query completed within timeout")
}

// ExampleMultipleAddresses demonstrates multiple server addresses
func ExampleMultipleAddresses() {
	opt := &Options{
		Addr: []string{
			"server1:9000",
			"server2:9000",
			"server3:9000",
		},
		ConnOpenStrategy: ConnOpenRoundRobin,
		MaxOpenConns:     10,
		MaxIdleConns:     5,
	}

	client, err := NewClient(opt)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Get connection stats
	stats := client.Stats()
	fmt.Printf("Connection pool stats: %+v\n", stats)
}
