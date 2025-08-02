# Icebox Display Package

A modern, feature-rich CLI display system for the Icebox project that provides consistent, customizable, and accessible output across all commands.

## Features

### 🎯 Core Features

- **Unified Display Interface** - Single API for all CLI output needs
- **Progressive Enhancement** - Automatically adapts to terminal capabilities
- **Multiple Output Formats** - Table, CSV, JSON, Markdown
- **Theme Support** - Default, Dark, Light, and Minimal themes
- **Configuration System** - User preferences via YAML/JSON
- **Context Integration** - Pass display through application layers

### 📊 Table Features

- **Sorting** - Sort by any column, ascending or descending
- **Filtering** - Filter rows with various operators (=, !=, >, <, contains, etc.)
- **Pagination** - Automatic pagination with configurable page size
- **Row Numbers** - Optional row numbering
- **Compact Mode** - Condensed display without row separators
- **Titles & Footers** - Add context to tables
- **Column Width Management** - Automatic sizing with max width constraints
- **Text Wrapping** - Intelligent text wrapping for long content

### 🎨 Display Features

- **Message Levels** - Success, Error, Warning, Info with appropriate styling
- **Progress Indicators** - Show progress for long-running operations
- **Interactive Prompts** - Confirmations, selections, and input (when supported)
- **Terminal Detection** - Color, Unicode, width, and interactivity detection
- **Graceful Degradation** - Falls back to simple output in limited environments

## Installation

```go
import "github.com/TFMV/icebox/display"
```

## Quick Start

### Basic Usage

```go
// Create a display instance
d := display.New()

// Show messages
d.Success("Operation completed successfully")
d.Error("Failed to process: %v", err)
d.Warning("This action cannot be undone")
d.Info("Processing %d records", count)

// Display a table
data := display.TableData{
    Headers: []string{"ID", "Name", "Status"},
    Rows: [][]interface{}{
        {1, "Alice", "Active"},
        {2, "Bob", "Inactive"},
        {3, "Carol", "Active"},
    },
}

table := d.Table(data)
table.Render()
```

### Advanced Table Usage

```go
// Create a table with all features
table := d.Table(data).
    WithTitle("User Management").
    WithSorting("Name", false).              // Sort by name ascending
    WithFiltering("Status", "=", "Active").  // Show only active users
    WithPagination(10).                      // Show 10 rows per page
    WithRowNumbers().                        // Add row numbers
    WithMaxWidth(120).                       // Limit table width
    WithTheme(display.DarkTheme)             // Use dark theme

table.Render()
```

### Configuration

Create `~/.icebox/display.yaml`:

```yaml
theme: "dark"
format: "table"
table:
  max_width: 120
  pagination: 50
  unicode_borders: true
  show_row_numbers: false
  compact_mode: false
colors:
  enabled: "auto"  # auto, always, never
interactive:
  confirm_destructive: true
  show_hints: true
verbose: false
timing: true
```

Load and use configuration:

```go
config, err := display.LoadConfig()
if err != nil {
    config = display.DefaultConfig()
}

d := display.NewWithConfig(config)
```

### Context Integration

Pass display through your application:

```go
// Add display to context
ctx := display.WithDisplay(context.Background(), d)

// Use in other functions
func processData(ctx context.Context) {
    // Get display from context
    if d := display.GetDisplay(ctx); d != nil {
        d.Info("Processing data...")
    }
    
    // Or use the helper
    display.LogOrDisplay(ctx, display.MessageLevelSuccess, "Data processed")
}
```

## Output Formats

### Table Format (Default)

```
┌────┬───────┬──────────┐
│ ID │ Name  │ Status   │
├────┼───────┼──────────┤
│ 1  │ Alice │ Active   │
│ 2  │ Bob   │ Inactive │
│ 3  │ Carol │ Active   │
└────┴───────┴──────────┘
```

### CSV Format

```csv
ID,Name,Status
1,Alice,Active
2,Bob,Inactive
3,Carol,Active
```

### JSON Format

```json
[
  {"ID": "1", "Name": "Alice", "Status": "Active"},
  {"ID": "2", "Name": "Bob", "Status": "Inactive"},
  {"ID": "3", "Name": "Carol", "Status": "Active"}
]
```

### Markdown Format

```markdown
| ID | Name  | Status   |
| -- | ----- | -------- |
| 1  | Alice | Active   |
| 2  | Bob   | Inactive |
| 3  | Carol | Active   |
```

## Themes

### Default Theme

- Balanced colors and Unicode borders
- Works well in most terminals

### Dark Theme

- Optimized for dark terminal backgrounds
- Higher contrast colors

### Light Theme

- Optimized for light terminal backgrounds
- Softer colors

### Minimal Theme

- ASCII-only borders
- No colors
- Maximum compatibility

## Terminal Capabilities

The display system automatically detects:

- **Color Support** - Enables/disables colors based on terminal
- **Unicode Support** - Falls back to ASCII borders when needed
- **Terminal Width** - Adjusts table width accordingly
- **Interactive Mode** - Enables/disables prompts
- **CI Environment** - Simplified output for CI/CD

## Helper Functions

```go
// Format bytes into human-readable format
display.FormatBytes(1234567890) // "1.1 GB"

// Truncate strings with ellipsis
display.TruncateString("Long text", 10) // "Long te..."

// Wrap text to fit width
lines := display.WrapText("Long paragraph", 40)
```

## Examples

See the `example/` directory for complete examples:

- `main.go` - Basic usage demonstration
- `advanced/main.go` - Advanced features showcase

Run the examples:

```bash
cd example
go run main.go

cd advanced
go run main.go
```

## Architecture

```
display/
├── display.go       # Main interface and simple fallback renderer
├── types.go         # Core types and table builder
├── capabilities.go  # Terminal capability detection
├── config.go        # Configuration system
├── context.go       # Context integration
├── helpers.go       # Utility functions
├── renderers/       # Pluggable renderer backends
│   ├── interface.go # Renderer interface
│   ├── fallback.go  # Zero-dependency renderer
│   └── pterm.go     # Rich terminal renderer
└── example/         # Usage examples
```

## Best Practices

1. **Use Context** - Pass display through context for better integration
2. **Check Capabilities** - Respect terminal limitations
3. **Provide Fallbacks** - Always have a simple output option
4. **Be Consistent** - Use the same display instance throughout
5. **Configure Once** - Load configuration at startup
6. **Test Output** - Verify output in different environments

## Migration Guide

Replace manual formatting:

```go
// Before
fmt.Printf("┌─────┬─────┐\n")
fmt.Printf("│ %-3s │ %-3s │\n", "ID", "Name")
fmt.Printf("├─────┼─────┤\n")
// ... more manual formatting

// After
d := display.New()
table := d.Table(data)
table.Render()
```

Replace scattered print statements:

```go
// Before
fmt.Printf("✅ Success: %s\n", message)
fmt.Printf("❌ Error: %s\n", err)

// After
d.Success(message)
d.Error("Operation failed: %v", err)
```

## Contributing

When adding new features:

1. Maintain backward compatibility
2. Add tests for new functionality
3. Update documentation
4. Consider terminal limitations
5. Follow existing patterns

## License

Part of the Icebox project. See the main project LICENSE file.
