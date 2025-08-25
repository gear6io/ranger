package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/gear6io/ranger/pkg/sdk"
	"go.uber.org/zap"
)

// UserRecord represents a sample user record for ingestion
type UserRecord struct {
	ID        int     `json:"id"`
	Name      string  `json:"name"`
	Email     string  `json:"email"`
	Age       int     `json:"age"`
	City      string  `json:"city"`
	CreatedAt string  `json:"created_at"`
	IsActive  bool    `json:"is_active"`
	Score     float64 `json:"score"`
}

func main() {
	// Initialize logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	logger.Info("Starting Ranger SDK ingestion script")

	// Create client options
	options := &sdk.Options{
		Protocol: sdk.Native,
		Addr:     []string{"127.0.0.1:2849"},
		Auth: sdk.Auth{
			Username: "default",
			Password: "",
			Database: "default",
		},
		DialTimeout:  30 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		Logger:       logger,
	}

	// Create and connect to client
	client, err := sdk.Open(options)
	if err != nil {
		logger.Fatal("Failed to connect to Ranger server", zap.Error(err))
	}
	defer client.Close()

	logger.Info("Successfully connected to Ranger server")

	// Test connection
	if err := client.Ping(context.Background()); err != nil {
		logger.Fatal("Failed to ping server", zap.Error(err))
	}
	logger.Info("Server ping successful")

	// Create table
	tableName := "users"
	if err := createTable(client, tableName); err != nil {
		logger.Fatal("Failed to create table", zap.Error(err))
	}
	logger.Info("Table created successfully", zap.String("table", tableName))

	// Generate and ingest 1k records
	recordCount := 1000
	if err := ingestRecords(client, tableName, recordCount); err != nil {
		logger.Fatal("Failed to ingest records", zap.Error(err))
	}
	logger.Info("Successfully ingested records", zap.Int("count", recordCount))

	// Verify the data
	if err := verifyData(client, tableName, recordCount); err != nil {
		logger.Fatal("Failed to verify data", zap.Error(err))
	}
	logger.Info("Data verification successful")

	logger.Info("Ingestion script completed successfully")
}

// createTable creates a new table for user records
func createTable(client *sdk.Client, tableName string) error {
	ctx := context.Background()

	// Drop table if it exists (for testing purposes)
	dropQuery := fmt.Sprintf("DROP TABLE %s;", tableName)
	if err := client.Exec(ctx, dropQuery); err != nil {
		return fmt.Errorf("failed to drop existing table: %w", err)
	}

	// Create table with appropriate schema
	createQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			id UInt32,
			name String,
			email String,
			age UInt8,
			city String,
			created_at DateTime,
			is_active Boolean,
			score Float64
		) ENGINE = FILESYSTEM
		ORDER BY (id, created_at)
	`, tableName)

	if err := client.Exec(ctx, createQuery); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// generateUserRecord generates a single user record
func generateUserRecord(id int) UserRecord {
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"}
	names := []string{"John", "Jane", "Mike", "Sarah", "David", "Lisa", "Tom", "Emma", "Alex", "Maria"}

	// Generate random data
	age := 18 + rand.Intn(62) // 18-80 years old
	city := cities[rand.Intn(len(cities))]
	name := names[rand.Intn(len(names))]
	score := rand.Float64() * 100 // 0-100 score

	// Generate email
	email := fmt.Sprintf("%s%d@example.com", name, id)

	// Generate random creation date within last year
	daysAgo := rand.Intn(365)
	createdAt := time.Now().AddDate(0, 0, -daysAgo)

	return UserRecord{
		ID:        id,
		Name:      name,
		Email:     email,
		Age:       age,
		City:      city,
		CreatedAt: createdAt.Format("2006-01-02 15:04:05"),
		IsActive:  rand.Float64() > 0.3, // 70% active users
		Score:     score,
	}
}

// ingestRecords generates and ingests the specified number of records
func ingestRecords(client *sdk.Client, tableName string, count int) error {
	ctx := context.Background()

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Prepare batch insert
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (id, name, email, age, city, created_at, is_active, score)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, tableName)

	batch, err := client.PrepareBatch(ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	defer batch.Close()

	// Set batch columns
	batch.Columns = []string{"id", "name", "email", "age", "city", "created_at", "is_active", "score"}

	log.Printf("Generating %d records...", count)

	// Generate and append records
	for i := 1; i <= count; i++ {
		record := generateUserRecord(i)

		// Parse the created_at string back to time for proper insertion
		createdAt, err := time.Parse("2006-01-02 15:04:05", record.CreatedAt)
		if err != nil {
			return fmt.Errorf("failed to parse created_at for record %d: %w", i, err)
		}

		// Append to batch
		if err := batch.Append(
			record.ID,
			record.Name,
			record.Email,
			record.Age,
			record.City,
			createdAt,
			record.IsActive,
			record.Score,
		); err != nil {
			return fmt.Errorf("failed to append record %d to batch: %w", i, err)
		}

		// Progress reporting every 100 records
		if i%100 == 0 {
			log.Printf("Generated %d records...", i)
		}
	}

	log.Printf("Sending batch with %d records...", count)

	// Send the batch
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	log.Printf("Successfully sent batch with %d records", count)
	return nil
}

// verifyData verifies that the data was inserted correctly
func verifyData(client *sdk.Client, tableName string, expectedCount int) error {
	ctx := context.Background()

	// Count total records
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	rows, err := client.Query(ctx, countQuery)
	if err != nil {
		return fmt.Errorf("failed to query record count: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return fmt.Errorf("no results from count query")
	}

	var actualCount int
	if err := rows.Scan(&actualCount); err != nil {
		return fmt.Errorf("failed to scan count result: %w", err)
	}

	if actualCount != expectedCount {
		return fmt.Errorf("record count mismatch: expected %d, got %d", expectedCount, actualCount)
	}

	log.Printf("Verified record count: %d", actualCount)

	// Sample a few records to verify data quality
	sampleQuery := fmt.Sprintf("SELECT id, name, email, age, city, is_active, score FROM %s ORDER BY id LIMIT 5", tableName)
	sampleRows, err := client.Query(ctx, sampleQuery)
	if err != nil {
		return fmt.Errorf("failed to query sample records: %w", err)
	}
	defer sampleRows.Close()

	log.Println("Sample records:")
	recordCount := 0
	for sampleRows.Next() {
		var id int
		var name, email, city string
		var age int
		var isActive bool
		var score float64

		if err := sampleRows.Scan(&id, &name, &email, &age, &city, &isActive, &score); err != nil {
			return fmt.Errorf("failed to scan sample record: %w", err)
		}

		log.Printf("  ID: %d, Name: %s, Email: %s, Age: %d, City: %s, Active: %t, Score: %.2f",
			id, name, email, age, city, isActive, score)
		recordCount++
	}

	if recordCount == 0 {
		return fmt.Errorf("no sample records found")
	}

	log.Printf("Successfully verified %d sample records", recordCount)
	return nil
}
