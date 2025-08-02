package display

import (
	"fmt"
	"strings"
)

// OutputFormat represents different output formats
type OutputFormat int

const (
	FormatTable OutputFormat = iota
	FormatCSV
	FormatJSON
	FormatMarkdown
)

// MessageLevel represents different message types
type MessageLevel int

const (
	MessageLevelInfo MessageLevel = iota
	MessageLevelSuccess
	MessageLevelWarning
	MessageLevelError
)

// Color represents a color value
type Color string

// Theme represents a visual theme for the display system
type Theme struct {
	Name       string
	Colors     ColorScheme
	TableStyle TableStyle
	Borders    BorderStyle
	Icons      IconSet
}

// ColorScheme defines colors for different message types
type ColorScheme struct {
	Primary   Color
	Secondary Color
	Success   Color
	Warning   Color
	Error     Color
	Info      Color
	Muted     Color
}

// TableStyle defines table appearance
type TableStyle struct {
	HeaderStyle string
	RowStyle    string
	BorderStyle string
}

// BorderStyle defines border characters
type BorderStyle struct {
	Horizontal  string
	Vertical    string
	TopLeft     string
	TopRight    string
	BottomLeft  string
	BottomRight string
	Cross       string
}

// IconSet defines icons for different message types
type IconSet struct {
	Success string
	Warning string
	Error   string
	Info    string
}

// Predefined themes
var (
	DefaultTheme = Theme{
		Name: "default",
		Colors: ColorScheme{
			Primary:   "#0066CC",
			Secondary: "#6C757D",
			Success:   "#28A745",
			Warning:   "#FFC107",
			Error:     "#DC3545",
			Info:      "#17A2B8",
			Muted:     "#6C757D",
		},
		TableStyle: TableStyle{
			HeaderStyle: "bold",
			RowStyle:    "normal",
			BorderStyle: "rounded",
		},
		Borders: BorderStyle{
			Horizontal:  "─",
			Vertical:    "│",
			TopLeft:     "┌",
			TopRight:    "┐",
			BottomLeft:  "└",
			BottomRight: "┘",
			Cross:       "┼",
		},
		Icons: IconSet{
			Success: "✅",
			Warning: "⚠️",
			Error:   "❌",
			Info:    "ℹ️",
		},
	}

	DarkTheme = Theme{
		Name: "dark",
		Colors: ColorScheme{
			Primary:   "#4A9EFF",
			Secondary: "#8E8E93",
			Success:   "#30D158",
			Warning:   "#FF9F0A",
			Error:     "#FF453A",
			Info:      "#64D2FF",
			Muted:     "#8E8E93",
		},
		TableStyle: TableStyle{
			HeaderStyle: "bold",
			RowStyle:    "normal",
			BorderStyle: "rounded",
		},
		Borders: BorderStyle{
			Horizontal:  "─",
			Vertical:    "│",
			TopLeft:     "┌",
			TopRight:    "┐",
			BottomLeft:  "└",
			BottomRight: "┘",
			Cross:       "┼",
		},
		Icons: IconSet{
			Success: "✅",
			Warning: "⚠️",
			Error:   "❌",
			Info:    "ℹ️",
		},
	}

	LightTheme = Theme{
		Name: "light",
		Colors: ColorScheme{
			Primary:   "#007AFF",
			Secondary: "#5E5CE6",
			Success:   "#34C759",
			Warning:   "#FF9500",
			Error:     "#FF3B30",
			Info:      "#5AC8FA",
			Muted:     "#C7C7CC",
		},
		TableStyle: TableStyle{
			HeaderStyle: "bold",
			RowStyle:    "normal",
			BorderStyle: "rounded",
		},
		Borders: BorderStyle{
			Horizontal:  "─",
			Vertical:    "│",
			TopLeft:     "┌",
			TopRight:    "┐",
			BottomLeft:  "└",
			BottomRight: "┘",
			Cross:       "┼",
		},
		Icons: IconSet{
			Success: "✓",
			Warning: "⚠",
			Error:   "✗",
			Info:    "i",
		},
	}

	MinimalTheme = Theme{
		Name: "minimal",
		Colors: ColorScheme{
			Primary:   "",
			Secondary: "",
			Success:   "",
			Warning:   "",
			Error:     "",
			Info:      "",
			Muted:     "",
		},
		TableStyle: TableStyle{
			HeaderStyle: "normal",
			RowStyle:    "normal",
			BorderStyle: "simple",
		},
		Borders: BorderStyle{
			Horizontal:  "-",
			Vertical:    "|",
			TopLeft:     "+",
			TopRight:    "+",
			BottomLeft:  "+",
			BottomRight: "+",
			Cross:       "+",
		},
		Icons: IconSet{
			Success: "[OK]",
			Warning: "[WARN]",
			Error:   "[ERROR]",
			Info:    "[INFO]",
		},
	}
)

// Display is the main interface for all display operations
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

// Renderer interface that all display renderers must implement
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

// DisplayImpl is the concrete implementation of Display
type DisplayImpl struct {
	renderer Renderer
	theme    Theme
	format   OutputFormat
	caps     TerminalCapabilities
}

// NewPTermRenderer creates a new PTerm renderer (placeholder)
func NewPTermRenderer() Renderer {
	// This will be implemented by importing the renderers package
	return NewFallbackRenderer()
}

// simpleFallbackRenderer is a basic implementation for bootstrapping
type simpleFallbackRenderer struct{}

func (r *simpleFallbackRenderer) RenderTable(data TableData, options TableOptions) error {
	switch options.Format {
	case FormatCSV:
		return r.renderCSV(data)
	case FormatJSON:
		return r.renderJSON(data)
	default:
		return r.renderTable(data, options)
	}
}

func (r *simpleFallbackRenderer) renderTable(data TableData, options TableOptions) error {
	if len(data.Rows) == 0 && len(data.Headers) == 0 {
		return nil
	}

	// Print title if provided
	if options.Title != "" {
		fmt.Printf("\n%s\n", options.Title)
		fmt.Println(strings.Repeat("=", len(options.Title)))
	}

	// Calculate column widths
	widths := make([]int, len(data.Headers))
	for i, header := range data.Headers {
		widths[i] = len(header)
	}

	// Check data widths
	for _, row := range data.Rows {
		for i, cell := range row {
			if i < len(widths) {
				str := fmt.Sprintf("%v", cell)
				if len(str) > widths[i] {
					widths[i] = len(str)
				}
			}
		}
	}

	// Apply max width constraints
	totalWidth := 0
	for i := range widths {
		if options.MaxWidth > 0 && widths[i] > options.MaxWidth/len(widths) {
			widths[i] = options.MaxWidth / len(widths)
		}
		totalWidth += widths[i] + 3 // +3 for padding and separator
	}

	// Determine border style based on theme
	borders := BorderStyle{
		Horizontal:  "-",
		Vertical:    "|",
		TopLeft:     "+",
		TopRight:    "+",
		BottomLeft:  "+",
		BottomRight: "+",
		Cross:       "+",
	}
	if options.Theme != nil {
		borders = options.Theme.Borders
	}

	// Print top border
	fmt.Print(borders.TopLeft)
	for i, width := range widths {
		fmt.Print(strings.Repeat(borders.Horizontal, width+2))
		if i < len(widths)-1 {
			fmt.Print(borders.Cross)
		}
	}
	fmt.Println(borders.TopRight)

	// Print headers
	fmt.Print(borders.Vertical)
	for i, header := range data.Headers {
		fmt.Printf(" %-*s ", widths[i], TruncateString(header, widths[i]))
		fmt.Print(borders.Vertical)
	}
	fmt.Println()

	// Print separator
	fmt.Print(borders.Cross)
	for i, width := range widths {
		fmt.Print(strings.Repeat(borders.Horizontal, width+2))
		if i < len(widths)-1 {
			fmt.Print(borders.Cross)
		}
	}
	fmt.Println(borders.Cross)

	// Print rows
	for rowIdx, row := range data.Rows {
		// Add row number if requested
		if options.ShowRowNumbers {
			fmt.Printf("%4d ", rowIdx+1)
		}

		fmt.Print(borders.Vertical)
		for i, cell := range row {
			if i < len(widths) {
				str := fmt.Sprintf("%v", cell)
				fmt.Printf(" %-*s ", widths[i], TruncateString(str, widths[i]))
				fmt.Print(borders.Vertical)
			}
		}
		fmt.Println()

		// Add separator between rows if not in compact mode
		if !options.CompactMode && rowIdx < len(data.Rows)-1 {
			fmt.Print(borders.Cross)
			for i, width := range widths {
				fmt.Print(strings.Repeat(borders.Horizontal, width+2))
				if i < len(widths)-1 {
					fmt.Print(borders.Cross)
				}
			}
			fmt.Println(borders.Cross)
		}
	}

	// Print bottom border
	fmt.Print(borders.BottomLeft)
	for i, width := range widths {
		fmt.Print(strings.Repeat(borders.Horizontal, width+2))
		if i < len(widths)-1 {
			fmt.Print(borders.Cross)
		}
	}
	fmt.Println(borders.BottomRight)

	// Print footer if provided
	if len(data.Footer) > 0 {
		fmt.Println()
		for _, footer := range data.Footer {
			fmt.Println(footer)
		}
	}

	return nil
}

func (r *simpleFallbackRenderer) renderCSV(data TableData) error {
	// Print headers
	for i, header := range data.Headers {
		if i > 0 {
			fmt.Print(",")
		}
		fmt.Print(header)
	}
	fmt.Println()

	// Print rows
	for _, row := range data.Rows {
		for i, cell := range row {
			if i > 0 {
				fmt.Print(",")
			}
			fmt.Printf("%v", cell)
		}
		fmt.Println()
	}
	return nil
}

func (r *simpleFallbackRenderer) renderJSON(data TableData) error {
	fmt.Println("[")
	for i, row := range data.Rows {
		fmt.Print("  {")
		for j, header := range data.Headers {
			if j > 0 {
				fmt.Print(", ")
			}
			fmt.Printf(`"%s": "%v"`, header, row[j])
		}
		fmt.Print("}")
		if i < len(data.Rows)-1 {
			fmt.Print(",")
		}
		fmt.Println()
	}
	fmt.Println("]")
	return nil
}

func (r *simpleFallbackRenderer) RenderMessage(level MessageLevel, message string) {
	switch level {
	case MessageLevelSuccess:
		fmt.Printf("[SUCCESS] %s\n", message)
	case MessageLevelError:
		fmt.Printf("[ERROR] %s\n", message)
	case MessageLevelWarning:
		fmt.Printf("[WARNING] %s\n", message)
	case MessageLevelInfo:
		fmt.Printf("[INFO] %s\n", message)
	}
}

func (r *simpleFallbackRenderer) RenderConfirm(message string) bool {
	fmt.Printf("%s (y/N): ", message)
	var response string
	_, _ = fmt.Scanln(&response) // Ignore error for user input
	return response == "y" || response == "Y"
}

func (r *simpleFallbackRenderer) RenderSelect(message string, options []string) (int, error) {
	fmt.Println(message)
	for i, option := range options {
		fmt.Printf("  %d. %s\n", i+1, option)
	}
	fmt.Print("Select: ")
	var choice int
	_, _ = fmt.Scanln(&choice) // Ignore error for user input
	if choice < 1 || choice > len(options) {
		return 0, fmt.Errorf("invalid selection")
	}
	return choice - 1, nil
}

func (r *simpleFallbackRenderer) RenderInput(message string) (string, error) {
	fmt.Printf("%s: ", message)
	var response string
	_, _ = fmt.Scanln(&response) // Ignore error for user input
	return response, nil
}

func (r *simpleFallbackRenderer) RenderProgress(title string, current, total int) error {
	fmt.Printf("\r%s: %d/%d", title, current, total)
	if current == total {
		fmt.Println()
	}
	return nil
}

// NewFallbackRenderer creates a new fallback renderer
func NewFallbackRenderer() Renderer {
	return &simpleFallbackRenderer{}
}

// New creates a new Display instance with automatic capability detection
func New() *DisplayImpl {
	caps := DetectCapabilities()

	var renderer Renderer
	if caps.SupportsColor && caps.SupportsUnicode {
		renderer = NewPTermRenderer()
	} else {
		renderer = NewFallbackRenderer()
	}

	return &DisplayImpl{
		renderer: renderer,
		theme:    DefaultTheme,
		format:   FormatTable,
		caps:     caps,
	}
}

// NewWithRenderer creates a new Display instance with a specific renderer
func NewWithRenderer(renderer Renderer) *DisplayImpl {
	return &DisplayImpl{
		renderer: renderer,
		theme:    DefaultTheme,
		format:   FormatTable,
		caps:     DetectCapabilities(),
	}
}

// Table creates a new table builder
func (d *DisplayImpl) Table(data TableData) *TableBuilder {
	return &TableBuilder{
		display: d,
		data:    data,
		options: DefaultTableOptions(),
	}
}

// Success displays a success message
func (d *DisplayImpl) Success(message string, args ...interface{}) {
	formatted := fmt.Sprintf(message, args...)
	d.renderer.RenderMessage(MessageLevelSuccess, formatted)
}

// Error displays an error message
func (d *DisplayImpl) Error(message string, args ...interface{}) {
	formatted := fmt.Sprintf(message, args...)
	d.renderer.RenderMessage(MessageLevelError, formatted)
}

// Warning displays a warning message
func (d *DisplayImpl) Warning(message string, args ...interface{}) {
	formatted := fmt.Sprintf(message, args...)
	d.renderer.RenderMessage(MessageLevelWarning, formatted)
}

// Info displays an info message
func (d *DisplayImpl) Info(message string, args ...interface{}) {
	formatted := fmt.Sprintf(message, args...)
	d.renderer.RenderMessage(MessageLevelInfo, formatted)
}

// Progress creates a new progress builder
func (d *DisplayImpl) Progress(title string) *ProgressBuilder {
	return &ProgressBuilder{
		display: d,
		title:   title,
	}
}

// Confirm shows a confirmation prompt
func (d *DisplayImpl) Confirm(message string) bool {
	if !d.caps.IsInteractive {
		return false
	}
	return d.renderer.RenderConfirm(message)
}

// Select shows a selection menu
func (d *DisplayImpl) Select(message string, options []string) (int, error) {
	if !d.caps.IsInteractive {
		return 0, fmt.Errorf("interactive selection not available in non-interactive mode")
	}
	return d.renderer.RenderSelect(message, options)
}

// Input shows an input prompt
func (d *DisplayImpl) Input(message string) (string, error) {
	if !d.caps.IsInteractive {
		return "", fmt.Errorf("interactive input not available in non-interactive mode")
	}
	return d.renderer.RenderInput(message)
}

// SetFormat sets the output format
func (d *DisplayImpl) SetFormat(format OutputFormat) Display {
	d.format = format
	return d
}

// SetTheme sets the display theme
func (d *DisplayImpl) SetTheme(theme Theme) Display {
	d.theme = theme
	return d
}

// ProgressBuilder for building progress indicators
type ProgressBuilder struct {
	display *DisplayImpl
	title   string
}

// Start starts the progress indicator
func (pb *ProgressBuilder) Start() *ProgressIndicator {
	return &ProgressIndicator{
		display: pb.display,
		title:   pb.title,
	}
}

// ProgressIndicator represents an active progress indicator
type ProgressIndicator struct {
	display *DisplayImpl
	title   string
}

// Update updates the progress
func (pi *ProgressIndicator) Update(message string) {
	pi.display.Info("%s: %s", pi.title, message)
}

// Finish completes the progress
func (pi *ProgressIndicator) Finish(message string) {
	pi.display.Success("%s: %s", pi.title, message)
}
