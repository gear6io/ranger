# Icebox CLI Display System Explanation

## Overview

The older Icebox CLI had a sophisticated, multi-layered display system designed to provide rich, interactive terminal output while maintaining compatibility across different environments. This system was built with the philosophy of "graceful degradation" - providing the best possible experience based on the terminal's capabilities.

## Architecture

### 1. Core Components

#### Display Interface (`display.Display`)
The main interface that CLI commands interact with:

```go
type Display interface {
    // Table operations
    Table(data TableData) *TableBuilder
    
    // Message operations
    Success(message string, args ...interface{})
    Error(message string, args ...interface{})
    Warning(message string, args ...interface{})
    Info(message string, args ...interface{})
    
    // Progress operations
    Progress(title string) *ProgressBuilder
    
    // Interactive operations
    Confirm(message string) bool
    Select(message string, options []string) (int, error)
    Input(message string) (string, error)
    
    // Output format control
    SetFormat(format OutputFormat) Display
    SetTheme(theme Theme) Display
}
```

#### Renderer Interface (`display.Renderer`)
The abstraction layer that handles actual output rendering:

```go
type Renderer interface {
    // Table rendering
    RenderTable(data TableData, options TableOptions) error
    
    // Message rendering
    RenderMessage(level MessageLevel, message string)
    
    // Interactive rendering
    RenderConfirm(message string) bool
    RenderSelect(message string, options []string) (int, error)
    RenderInput(message string) (string, error)
    
    // Progress rendering
    RenderProgress(title string, current, total int) error
}
```

### 2. Renderer Implementations

#### PTerm Renderer (`renderers/pterm.go`)
**Purpose**: Provides rich, colorful terminal output using the PTerm library
**Features**:
- Colored output with themes
- Unicode support for borders and icons
- Interactive prompts with styling
- Progress bars and spinners
- Rich table formatting

**Usage Example**:
```go
// Rich table with colors and borders
tableData := display.TableData{
    Headers: []string{"Name", "Age", "City"},
    Rows: [][]interface{}{
        {"Alice", 25, "New York"},
        {"Bob", 30, "San Francisco"},
    },
}

d.Table(tableData).WithTheme(display.DefaultTheme).Render()
```

#### Fallback Renderer (`renderers/fallback.go`)
**Purpose**: Simple ASCII-based renderer for basic terminals or CI environments
**Features**:
- ASCII table borders (+, -, |)
- Basic text formatting
- CSV and JSON output
- No color dependencies
- Works in any environment

**Usage Example**:
```go
// Simple ASCII table
+--------+-----+---------------+
| Name   | Age | City          |
+--------+-----+---------------+
| Alice  | 25  | New York      |
| Bob    | 30  | San Francisco |
+--------+-----+---------------+
```

### 3. Terminal Capabilities Detection

The system automatically detects terminal capabilities:

```go
type TerminalCapabilities struct {
    SupportsColor   bool  // Can display colors
    SupportsUnicode bool  // Can display Unicode characters
    Width           int   // Terminal width
    Height          int   // Terminal height
    IsInteractive   bool  // Is interactive terminal
    IsPiped         bool  // Output is being piped
}
```

**Detection Logic**:
- **Color Support**: Checks `NO_COLOR`, `FORCE_COLOR`, `TERM` environment variables
- **Unicode Support**: Checks locale settings, disables on Windows Command Prompt
- **CI Detection**: Checks for `CI`, `GITHUB_ACTIONS`, `JENKINS_URL`, etc.
- **Piping Detection**: Checks if stdout is a character device

### 4. Context Integration

The display system integrates with Go's context system for dependency injection:

```go
// In root.go
func getDisplayFromContext(ctx context.Context) display.Display {
    return display.GetDisplayOrDefault(ctx)
}

// In CLI commands
func runSQL(cmd *cobra.Command, args []string) error {
    ctx := cmd.Context()
    d := getDisplayFromContext(ctx)
    
    // Use display for output
    d.Info("Executing query: %s", query)
    d.Error("Query failed: %v", err)
    d.Table(data).Render()
}
```

## Usage Patterns

### 1. Message Display

```go
// Success messages (green)
d.Success("Table created successfully: %s", tableName)

// Error messages (red)
d.Error("Failed to create table: %v", err)

// Warning messages (yellow)
d.Warning("Large result set detected (%d rows)", rowCount)

// Info messages (blue)
d.Info("Query executed in %v", duration)
```

### 2. Table Display

```go
// Basic table
tableData := display.TableData{
    Headers: []string{"Column1", "Column2"},
    Rows: [][]interface{}{
        {"Value1", "Value2"},
        {"Value3", "Value4"},
    },
}

d.Table(tableData).Render()

// Advanced table with options
d.Table(tableData).
    WithFormat(display.FormatCSV).
    WithPagination(10).
    WithSorting("Column1", false).
    WithRowNumbers().
    WithTitle("My Data").
    Render()
```

### 3. Progress Indicators

```go
// Progress bar
progress := d.Progress("Importing data...")
progress.Start()

for i, item := range items {
    progress.Update(fmt.Sprintf("Processing %s", item))
    // ... process item
}

progress.Finish("Import completed successfully")
```

### 4. Interactive Prompts

```go
// Confirmation
if d.Confirm("Do you want to delete this table?") {
    // Delete table
}

// Selection
options := []string{"Option 1", "Option 2", "Option 3"}
choice, err := d.Select("Choose an option:", options)
if err != nil {
    return err
}

// Input
name, err := d.Input("Enter table name:")
if err != nil {
    return err
}
```

## Output Formats

### 1. Table Format (Default)
Rich, formatted tables with borders, colors, and styling.

### 2. CSV Format
Comma-separated values for data export:
```csv
Name,Age,City
Alice,25,New York
Bob,30,San Francisco
```

### 3. JSON Format
Structured JSON output:
```json
[
  {"Name": "Alice", "Age": 25, "City": "New York"},
  {"Name": "Bob", "Age": 30, "City": "San Francisco"}
]
```

### 4. Markdown Format
Markdown tables for documentation:
```markdown
| Name  | Age | City          |
|-------|-----|---------------|
| Alice | 25  | New York      |
| Bob   | 30  | San Francisco |
```

## Theme System

The display system supports customizable themes:

```go
type Theme struct {
    Name       string
    Colors     ColorScheme
    TableStyle TableStyle
    Borders    BorderStyle
    Icons      IconSet
}

type ColorScheme struct {
    Primary   Color
    Secondary Color
    Success   Color
    Warning   Color
    Error     Color
    Info      Color
    Muted     Color
}
```

**Default Theme**:
- Primary: Blue (#0066CC)
- Success: Green (#28A745)
- Warning: Yellow (#FFC107)
- Error: Red (#DC3545)
- Info: Cyan (#17A2B8)

## Integration with CLI Commands

### Example: SQL Command Integration

```go
func runSQL(cmd *cobra.Command, args []string) error {
    ctx := cmd.Context()
    d := getDisplayFromContext(ctx)
    
    // Error handling with display
    if err != nil {
        d.Error("Query failed: %v", err)
        d.Info("Try running 'icebox sql \"SHOW TABLES\"' to see available tables")
        return err
    }
    
    // Success feedback
    d.Success("Query executed successfully")
    d.Info("Query [%s] executed in %v", result.QueryID, duration)
    
    // Table display
    tableData := display.TableData{
        Headers: result.Columns,
        Rows:    result.Rows,
    }
    
    return d.Table(tableData).WithFormat(format).Render()
}
```

## Environment Adaptability

### 1. Rich Terminal (PTerm Renderer)
- Full color support
- Unicode borders and icons
- Interactive prompts
- Progress bars
- Rich table formatting

### 2. Basic Terminal (Fallback Renderer)
- ASCII borders
- No colors
- Simple text formatting
- Basic table layout

### 3. CI Environment
- No colors (respects `NO_COLOR`)
- Simple text output
- CSV/JSON formats for parsing
- No interactive prompts

### 4. Piped Output
- Detects when output is piped
- Uses appropriate format (CSV/JSON)
- No interactive elements
- No progress bars

## Benefits

### 1. User Experience
- **Rich Output**: Beautiful, colored tables and messages
- **Interactive**: Confirmation prompts, selections, input
- **Progress Feedback**: Visual progress indicators
- **Error Handling**: Clear, helpful error messages

### 2. Developer Experience
- **Simple API**: Easy to use in CLI commands
- **Context Integration**: Automatic dependency injection
- **Format Flexibility**: Multiple output formats
- **Theme Support**: Customizable appearance

### 3. Compatibility
- **Graceful Degradation**: Works in any environment
- **CI Friendly**: Respects CI environment variables
- **Piping Support**: Appropriate output for scripts
- **Cross-Platform**: Works on Windows, macOS, Linux

### 4. Maintainability
- **Separation of Concerns**: Display logic separated from business logic
- **Renderer Abstraction**: Easy to add new renderers
- **Context Pattern**: Clean dependency injection
- **Type Safety**: Strong typing throughout

## Migration to New Architecture

The display system was moved to `deprecated/display/` during the restructuring. In the new client-server architecture, this functionality would need to be:

1. **Migrated to Client**: The display system should be moved to the new `icebox-client`
2. **Server Integration**: The server should provide structured data that the client can format
3. **Protocol Adaptation**: Display commands need to work over HTTP/gRPC protocols
4. **Remote Rendering**: Consider how to handle interactive prompts over remote connections

This sophisticated display system was one of the key features that made the old CLI user-friendly and professional, providing a modern terminal experience while maintaining broad compatibility. 