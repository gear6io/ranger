package protocol

import (
	"fmt"
	"log"
)

// Example usage of UnifiedResponse for different scenarios

// ExampleQueryEngineUsage shows how the main query engine would use UnifiedResponse
func ExampleQueryEngineUsage() {
	// Build a response with sample data
	builder := NewResponseBuilder()

	// Set column metadata
	builder.SetColumns(
		[]string{"id", "name", "age", "salary"},
		[]string{"int64", "string", "int32", "float64"},
	)

	// Add sample rows
	builder.AddRow([]interface{}{int64(1), "Alice", int32(30), 75000.50})
	builder.AddRow([]interface{}{int64(2), "Bob", int32(25), 65000.00})
	builder.AddRow([]interface{}{int64(3), "Charlie", int32(35), 85000.75})

	// Build the final response
	response := builder.Build("query_123", "OK", nil)

	fmt.Printf("Response size: %d bytes\n", response.Size())
	fmt.Printf("Row count: %d\n", response.RowCount)
	fmt.Printf("Column count: %d\n", response.ColumnCount)

	// Extract column information
	names, types, err := response.GetColumns()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Columns: %v\n", names)
	fmt.Printf("Types: %v\n", types)

	// Iterate through rows efficiently
	iterator := response.GetRowIterator()
	for iterator.Next() {
		var id int64
		var name string
		var age int32
		var salary float64

		if err := iterator.ScanRow(&id, &name, &age, &salary); err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Row: id=%d, name=%s, age=%d, salary=%.2f\n", id, name, age, salary)
	}
}

// ExampleProtocolConversion shows how to convert from legacy QueryResult types
func ExampleProtocolConversion() {
	// Simulate legacy JDBC QueryResult
	type LegacyJDBCResult struct {
		Columns []string
		Rows    [][]interface{}
	}

	legacy := &LegacyJDBCResult{
		Columns: []string{"user_id", "username", "active"},
		Rows: [][]interface{}{
			{int64(100), "admin", true},
			{int64(101), "user1", false},
			{int64(102), "user2", true},
		},
	}

	// Convert to UnifiedResponse
	builder := NewResponseBuilder()
	builder.SetColumns(legacy.Columns, []string{"int64", "string", "bool"})

	for _, row := range legacy.Rows {
		builder.AddRow(row)
	}

	unified := builder.Build("legacy_query", "Converted from legacy", nil)

	fmt.Printf("Converted response size: %d bytes\n", unified.Size())
	fmt.Printf("Original would have used approximately %d bytes in interface{} overhead\n",
		len(legacy.Rows)*len(legacy.Columns)*24) // Rough estimate of interface{} overhead
}

// ExampleMemoryComparison demonstrates memory efficiency gains
func ExampleMemoryComparison() {
	const numRows = 10000
	const numCols = 10

	// Traditional approach with [][]interface{}
	traditionalData := make([][]interface{}, numRows)
	for i := 0; i < numRows; i++ {
		row := make([]interface{}, numCols)
		for j := 0; j < numCols; j++ {
			row[j] = fmt.Sprintf("value_%d_%d", i, j)
		}
		traditionalData[i] = row
	}

	// Calculate approximate memory usage of traditional approach
	// Each interface{} = 16 bytes (8 bytes type info + 8 bytes value pointer)
	// Each string = ~20 bytes average + string data
	traditionalMemory := numRows * numCols * (16 + 20 + 10) // Rough estimate

	// UnifiedResponse approach
	builder := NewResponseBuilder()
	columns := make([]string, numCols)
	types := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		columns[i] = fmt.Sprintf("col_%d", i)
		types[i] = "string"
	}
	builder.SetColumns(columns, types)

	for i := 0; i < numRows; i++ {
		row := make([]interface{}, numCols)
		for j := 0; j < numCols; j++ {
			row[j] = fmt.Sprintf("value_%d_%d", i, j)
		}
		builder.AddRow(row)
	}

	unified := builder.Build("memory_test", "Memory comparison", nil)

	fmt.Printf("Traditional approach (estimated): %d bytes\n", traditionalMemory)
	fmt.Printf("UnifiedResponse approach: %d bytes\n", unified.Size())
	fmt.Printf("Memory savings: %.1f%%\n",
		float64(traditionalMemory-unified.Size())/float64(traditionalMemory)*100)
}

// ExampleStreamingUsage shows how to use UnifiedResponse for streaming
func ExampleStreamingUsage() {
	// Build a large response
	builder := NewResponseBuilder()
	builder.SetColumns(
		[]string{"timestamp", "event", "user_id", "value"},
		[]string{"string", "string", "int64", "float64"},
	)

	// Add many rows to simulate large dataset
	for i := 0; i < 1000; i++ {
		builder.AddRow([]interface{}{
			fmt.Sprintf("2024-01-01T%02d:00:00Z", i%24),
			fmt.Sprintf("event_%d", i%10),
			int64(i % 100),
			float64(i) * 1.5,
		})
	}

	response := builder.Build("streaming_query", "Large dataset", nil)

	fmt.Printf("Built streaming response with %d rows, %d bytes\n",
		response.RowCount, response.Size())

	// Stream processing - only keep current row in memory
	iterator := response.GetRowIterator()
	processedCount := 0

	for iterator.Next() {
		var timestamp, event string
		var userID int64
		var value float64

		if err := iterator.ScanRow(&timestamp, &event, &userID, &value); err != nil {
			log.Fatal(err)
		}

		// Process row (in real usage, this might write to another system,
		// aggregate data, etc.)
		processedCount++

		// Only log every 100th row to avoid spam
		if processedCount%100 == 0 {
			fmt.Printf("Processed %d rows, current: %s, %s, %d, %.2f\n",
				processedCount, timestamp, event, userID, value)
		}
	}

	fmt.Printf("Finished processing %d rows\n", processedCount)
}

// ExampleNativeProtocolIntegration shows how to integrate with existing native protocol
func ExampleNativeProtocolIntegration() {
	// This would be used in connection.go to send query results

	// Build response from query engine result
	builder := NewResponseBuilder()
	builder.SetColumns(
		[]string{"table_name", "row_count"},
		[]string{"string", "int64"},
	)

	builder.AddRow([]interface{}{"users", int64(1000)})
	builder.AddRow([]interface{}{"orders", int64(5000)})
	builder.AddRow([]interface{}{"products", int64(500)})

	response := builder.Build("show_tables", "OK", nil)

	// Convert to format expected by existing protocol signals
	columns, types, err := response.GetColumns()
	if err != nil {
		log.Fatal(err)
	}

	// Convert to legacy format for existing signal compatibility
	legacyRows, err := response.ToLegacyRows()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Protocol integration:\n")
	fmt.Printf("Columns: %v\n", columns)
	fmt.Printf("Types: %v\n", types)
	fmt.Printf("Legacy rows: %v\n", legacyRows)

	// In the actual implementation, this would be sent via:
	// serverData := signals.NewServerData(signalColumns, legacyRows)
	// message, _ := codec.EncodeMessage(serverData)
	// codec.WriteMessage(conn, message)
}

// ExampleBatchProcessing shows efficient batch processing
func ExampleBatchProcessing() {
	// Simulate processing a large batch of data
	builder := NewResponseBuilder()
	builder.SetColumns(
		[]string{"batch_id", "item_id", "quantity", "price"},
		[]string{"int64", "string", "int32", "float64"},
	)

	// Add batch data
	for batchID := int64(1); batchID <= 5; batchID++ {
		for itemNum := 1; itemNum <= 100; itemNum++ {
			builder.AddRow([]interface{}{
				batchID,
				fmt.Sprintf("ITEM_%03d", itemNum),
				int32(itemNum%10 + 1),
				float64(itemNum) * 1.99,
			})
		}
	}

	response := builder.Build("batch_process", "Batch processing complete", nil)

	fmt.Printf("Processing %d rows in batches\n", response.RowCount)

	// Process in batches without loading all into memory
	iterator := response.GetRowIterator()
	currentBatch := int64(-1)
	batchTotal := float64(0)
	batchCount := 0

	for iterator.Next() {
		var batchID int64
		var itemID string
		var quantity int32
		var price float64

		if err := iterator.ScanRow(&batchID, &itemID, &quantity, &price); err != nil {
			log.Fatal(err)
		}

		// New batch detected
		if batchID != currentBatch {
			if currentBatch != -1 {
				fmt.Printf("Batch %d: %d items, total value: $%.2f\n",
					currentBatch, batchCount, batchTotal)
			}
			currentBatch = batchID
			batchTotal = 0
			batchCount = 0
		}

		batchTotal += price * float64(quantity)
		batchCount++
	}

	// Process final batch
	if currentBatch != -1 {
		fmt.Printf("Batch %d: %d items, total value: $%.2f\n",
			currentBatch, batchCount, batchTotal)
	}
}
