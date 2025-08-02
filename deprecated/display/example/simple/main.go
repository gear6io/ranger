package main

import (
	"fmt"
	"time"

	"github.com/TFMV/icebox/deprecated/display"
)

func main() {
	// Create a new display instance
	d := display.New()

	// Demonstrate messages
	d.Info("🚀 Starting Icebox Display System Demo")
	d.Success("✅ Display system initialized successfully")
	d.Warning("⚠️  This is a demonstration - some features may be limited")

	// Demonstrate table display
	d.Info("📊 Demonstrating table display...")

	// Sample data similar to what SQL queries would return
	tableData := display.TableData{
		Headers: []string{"ID", "Name", "Age", "City", "Salary"},
		Rows: [][]interface{}{
			{1, "Alice Johnson", 28, "New York", 75000},
			{2, "Bob Smith", 34, "San Francisco", 95000},
			{3, "Carol Davis", 29, "Chicago", 68000},
			{4, "David Wilson", 42, "Boston", 82000},
			{5, "Eve Brown", 31, "Seattle", 78000},
		},
	}

	// Create and render table
	table := d.Table(tableData)
	if err := table.Render(); err != nil {
		d.Error("Failed to render table: %v", err)
		return
	}

	// Demonstrate pagination
	d.Info("\n📄 Demonstrating pagination (max 3 rows)...")
	paginatedTable := d.Table(tableData).WithPagination(3)
	if err := paginatedTable.Render(); err != nil {
		d.Error("Failed to render paginated table: %v", err)
		return
	}

	// Demonstrate CSV format
	d.Info("\n📋 Demonstrating CSV format...")
	csvTable := d.Table(tableData).WithFormat(display.FormatCSV)
	if err := csvTable.Render(); err != nil {
		d.Error("Failed to render CSV table: %v", err)
		return
	}

	// Demonstrate JSON format
	d.Info("\n🔧 Demonstrating JSON format...")
	jsonTable := d.Table(tableData).WithFormat(display.FormatJSON)
	if err := jsonTable.Render(); err != nil {
		d.Error("Failed to render JSON table: %v", err)
		return
	}

	// Demonstrate progress (simulated)
	d.Info("\n⏳ Demonstrating progress indicator...")
	progress := d.Progress("Processing data")
	indicator := progress.Start()

	for i := 0; i <= 5; i++ {
		indicator.Update(fmt.Sprintf("Step %d/5", i))
		time.Sleep(200 * time.Millisecond)
	}
	indicator.Finish("Processing complete!")

	// Demonstrate interactive features (if terminal supports it)
	d.Info("\n🎯 Demonstrating interactive features...")

	// Note: These would only work in interactive terminals
	// For demo purposes, we'll show what they would look like
	d.Info("Interactive confirmation: Would you like to continue? (y/N)")
	d.Info("Interactive selection: Please choose an option:")
	d.Info("  1. Option A")
	d.Info("  2. Option B")
	d.Info("  3. Option C")
	d.Info("Interactive input: Please enter your name:")

	// Final summary
	d.Success("\n🎉 Display system demonstration complete!")
	d.Info("Key features demonstrated:")
	d.Info("  • Rich message formatting with icons")
	d.Info("  • Table rendering with multiple formats")
	d.Info("  • Pagination support")
	d.Info("  • CSV and JSON output")
	d.Info("  • Progress indicators")
	d.Info("  • Interactive prompts (terminal-dependent)")
}
