package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/TFMV/icebox/deprecated/display"
)

// Sample data structure
type Employee struct {
	ID         int
	Name       string
	Department string
	Age        int
	Salary     float64
	StartDate  time.Time
}

func main() {
	// Load configuration
	config, err := display.LoadConfig()
	if err != nil {
		fmt.Printf("Warning: Could not load config: %v\n", err)
		config = display.DefaultConfig()
	}

	// Create display with configuration
	d := display.NewWithConfig(config)

	// Create context with display
	ctx := display.WithDisplay(context.Background(), d)

	d.Info("🚀 Advanced Icebox Display System Demo")
	d.Success("✅ Display system initialized with configuration")

	// Generate sample data
	employees := generateSampleData()

	// Convert to table data
	tableData := employeesToTableData(employees)

	// Demo 1: Basic table with title
	d.Info("\n📊 Demo 1: Basic Table with Title")
	table := d.Table(tableData).
		WithTitle("Employee Database").
		WithMaxWidth(150)
	if err := table.Render(); err != nil {
		d.Error("Failed to render table: %v", err)
	}

	// Demo 2: Sorted table
	d.Info("\n📊 Demo 2: Sorted Table (by Salary, Descending)")
	sortedTable := d.Table(tableData).
		WithTitle("Employees by Salary").
		WithSorting("Salary", true).
		WithRowNumbers()
	if err := sortedTable.Render(); err != nil {
		d.Error("Failed to render sorted table: %v", err)
	}

	// Demo 3: Filtered table
	d.Info("\n📊 Demo 3: Filtered Table (Department = Engineering)")
	filteredTable := d.Table(tableData).
		WithTitle("Engineering Department").
		WithFiltering("Department", "=", "Engineering").
		WithCompactMode()
	if err := filteredTable.Render(); err != nil {
		d.Error("Failed to render filtered table: %v", err)
	}

	// Demo 4: Combined features
	d.Info("\n📊 Demo 4: Combined Features (Filter + Sort + Pagination)")
	complexTable := d.Table(tableData).
		WithTitle("Senior Employees (Age > 30)").
		WithFiltering("Age", ">", 30).
		WithSorting("Name", false).
		WithPagination(5).
		WithRowNumbers()
	if err := complexTable.Render(); err != nil {
		d.Error("Failed to render complex table: %v", err)
	}

	// Demo 5: Different themes
	d.Info("\n🎨 Demo 5: Theme Demonstration")

	themes := []struct {
		name  string
		theme display.Theme
	}{
		{"Default", display.DefaultTheme},
		{"Dark", display.DarkTheme},
		{"Light", display.LightTheme},
		{"Minimal", display.MinimalTheme},
	}

	smallData := display.TableData{
		Headers: []string{"Theme", "Style", "Features"},
		Rows: [][]interface{}{
			{"Default", "Balanced", "Unicode borders, colors"},
			{"Dark", "Modern", "Dark mode optimized"},
			{"Light", "Clean", "Light backgrounds"},
			{"Minimal", "Simple", "ASCII only, no colors"},
		},
	}

	for _, t := range themes {
		d.Info("\nTheme: %s", t.name)
		themeTable := d.Table(smallData).
			WithTheme(t.theme).
			WithTitle(fmt.Sprintf("%s Theme Example", t.name))
		if err := themeTable.Render(); err != nil {
			d.Error("Failed to render theme table: %v", err)
		}
	}

	// Demo 6: Context usage
	d.Info("\n🔧 Demo 6: Context-Based Display")
	demonstrateContextUsage(ctx)

	// Demo 7: Progress indicators
	d.Info("\n⏳ Demo 7: Progress Indicators")
	progress := d.Progress("Processing records")
	indicator := progress.Start()

	for i := 0; i <= 10; i++ {
		indicator.Update(fmt.Sprintf("Processing batch %d/10", i))
		time.Sleep(100 * time.Millisecond)
	}
	indicator.Finish("All records processed!")

	// Demo 8: Configuration
	d.Info("\n⚙️  Demo 8: Configuration System")
	d.Info("Current configuration:")
	d.Info("  Theme: %s", config.Theme)
	d.Info("  Format: %s", config.Format)
	d.Info("  Max Width: %d", config.Table.MaxWidth)
	d.Info("  Unicode Borders: %v", config.Table.UnicodeBorders)
	d.Info("  Colors: %s", config.Colors.Enabled)

	// Demo 9: Helper functions
	d.Info("\n🛠️  Demo 9: Helper Functions")
	d.Info("Format bytes: %s", display.FormatBytes(1234567890))
	d.Info("Truncate string: '%s'", display.TruncateString("This is a very long string that needs to be truncated", 30))

	wrapped := display.WrapText("This is a long text that needs to be wrapped to fit within a specific width constraint for better display", 40)
	d.Info("Wrapped text:")
	for _, line := range wrapped {
		d.Info("  %s", line)
	}

	// Summary
	d.Success("\n🎉 Advanced Display System Demo Complete!")
	d.Info("Features demonstrated:")
	d.Info("  ✓ Configuration system with themes")
	d.Info("  ✓ Advanced table features (sorting, filtering, pagination)")
	d.Info("  ✓ Multiple themes and styles")
	d.Info("  ✓ Context-based display passing")
	d.Info("  ✓ Progress indicators")
	d.Info("  ✓ Helper utilities")
	d.Info("  ✓ Row numbers and compact mode")
	d.Info("  ✓ Table titles and footers")
}

func generateSampleData() []Employee {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	departments := []string{"Engineering", "Sales", "Marketing", "HR", "Finance"}
	firstNames := []string{"Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack"}
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}

	employees := make([]Employee, 20)
	for i := range employees {
		employees[i] = Employee{
			ID:         1000 + i,
			Name:       fmt.Sprintf("%s %s", firstNames[rng.Intn(len(firstNames))], lastNames[rng.Intn(len(lastNames))]),
			Department: departments[rng.Intn(len(departments))],
			Age:        25 + rng.Intn(20),
			Salary:     50000 + float64(rng.Intn(50000)),
			StartDate:  time.Now().AddDate(-rng.Intn(10), -rng.Intn(12), -rng.Intn(28)),
		}
	}

	return employees
}

func employeesToTableData(employees []Employee) display.TableData {
	headers := []string{"ID", "Name", "Department", "Age", "Salary", "Start Date"}
	rows := make([][]interface{}, len(employees))

	for i, emp := range employees {
		rows[i] = []interface{}{
			emp.ID,
			emp.Name,
			emp.Department,
			emp.Age,
			fmt.Sprintf("$%.2f", emp.Salary),
			emp.StartDate.Format("2006-01-02"),
		}
	}

	return display.TableData{
		Headers: headers,
		Rows:    rows,
		Footer:  []string{fmt.Sprintf("Total employees: %d", len(employees))},
	}
}

func demonstrateContextUsage(ctx context.Context) {
	// This function receives context and can use the display from it
	display.LogOrDisplay(ctx, display.MessageLevelInfo, "This message uses display from context")

	// Get display directly
	if d := display.GetDisplay(ctx); d != nil {
		d.Success("Successfully retrieved display from context")
	}

	// Simulate a function that might not have display
	simulateBackgroundTask(ctx)
}

func simulateBackgroundTask(ctx context.Context) {
	// This represents a background task that can optionally use display
	display.LogOrDisplay(ctx, display.MessageLevelInfo, "Background task started")
	time.Sleep(100 * time.Millisecond)
	display.LogOrDisplay(ctx, display.MessageLevelSuccess, "Background task completed")
}
