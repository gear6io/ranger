package renderers

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/TFMV/icebox/deprecated/display"
)

// FallbackRenderer is a simple text-based renderer that works in any environment
type FallbackRenderer struct{}

// NewFallbackRenderer creates a new fallback renderer
func NewFallbackRenderer() *FallbackRenderer {
	return &FallbackRenderer{}
}

// RenderTable renders a table using simple ASCII characters
func (r *FallbackRenderer) RenderTable(data display.TableData, options display.TableOptions) error {
	switch options.Format {
	case display.FormatCSV:
		return r.renderCSV(data)
	case display.FormatJSON:
		return r.renderJSON(data)
	default:
		return r.renderTable(data, options)
	}
}

// renderTable renders a simple ASCII table
func (r *FallbackRenderer) renderTable(data display.TableData, options display.TableOptions) error {
	if len(data.Rows) == 0 {
		return nil
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
				str := formatValue(cell)
				if len(str) > widths[i] {
					widths[i] = len(str)
				}
			}
		}
	}

	// Cap column widths
	maxWidth := options.MaxWidth / len(widths)
	if maxWidth < 10 {
		maxWidth = 10
	}
	for i := range widths {
		if widths[i] > maxWidth {
			widths[i] = maxWidth
		}
	}

	// Print top border
	fmt.Print("+")
	for i, width := range widths {
		fmt.Print(strings.Repeat("-", width+2))
		if i < len(widths)-1 {
			fmt.Print("+")
		}
	}
	fmt.Println("+")

	// Print headers
	fmt.Print("|")
	for i, header := range data.Headers {
		fmt.Printf(" %-*s |", widths[i], truncateString(header, widths[i]))
	}
	fmt.Println()

	// Print separator
	fmt.Print("+")
	for i, width := range widths {
		fmt.Print(strings.Repeat("-", width+2))
		if i < len(widths)-1 {
			fmt.Print("+")
		}
	}
	fmt.Println("+")

	// Print rows
	for _, row := range data.Rows {
		fmt.Print("|")
		for i, cell := range row {
			if i < len(widths) {
				str := formatValue(cell)
				fmt.Printf(" %-*s |", widths[i], truncateString(str, widths[i]))
			}
		}
		fmt.Println()
	}

	// Print bottom border
	fmt.Print("+")
	for i, width := range widths {
		fmt.Print(strings.Repeat("-", width+2))
		if i < len(widths)-1 {
			fmt.Print("+")
		}
	}
	fmt.Println("+")

	return nil
}

// renderCSV renders data as CSV
func (r *FallbackRenderer) renderCSV(data display.TableData) error {
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	// Write headers
	if err := writer.Write(data.Headers); err != nil {
		return err
	}

	// Write rows
	for _, row := range data.Rows {
		record := make([]string, len(row))
		for i, cell := range row {
			record[i] = formatValue(cell)
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

// renderJSON renders data as JSON
func (r *FallbackRenderer) renderJSON(data display.TableData) error {
	var result []map[string]interface{}

	for _, row := range data.Rows {
		record := make(map[string]interface{})
		for i, header := range data.Headers {
			if i < len(row) {
				record[header] = row[i]
			}
		}
		result = append(result, record)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

// RenderMessage renders a message with simple prefixes
func (r *FallbackRenderer) RenderMessage(level display.MessageLevel, message string) {
	switch level {
	case display.MessageLevelSuccess:
		fmt.Printf("[SUCCESS] %s\n", message)
	case display.MessageLevelError:
		fmt.Printf("[ERROR] %s\n", message)
	case display.MessageLevelWarning:
		fmt.Printf("[WARNING] %s\n", message)
	case display.MessageLevelInfo:
		fmt.Printf("[INFO] %s\n", message)
	}
}

// RenderConfirm renders a confirmation prompt
func (r *FallbackRenderer) RenderConfirm(message string) bool {
	fmt.Printf("%s (y/N): ", message)
	reader := bufio.NewReader(os.Stdin)
	response, _ := reader.ReadString('\n')
	response = strings.TrimSpace(strings.ToLower(response))
	return response == "y" || response == "yes"
}

// RenderSelect renders a selection menu
func (r *FallbackRenderer) RenderSelect(message string, options []string) (int, error) {
	fmt.Println(message)
	for i, option := range options {
		fmt.Printf("  %d. %s\n", i+1, option)
	}
	fmt.Print("Select option (1-" + strconv.Itoa(len(options)) + "): ")

	reader := bufio.NewReader(os.Stdin)
	response, _ := reader.ReadString('\n')
	response = strings.TrimSpace(response)

	choice, err := strconv.Atoi(response)
	if err != nil || choice < 1 || choice > len(options) {
		return 0, fmt.Errorf("invalid selection")
	}

	return choice - 1, nil
}

// RenderInput renders an input prompt
func (r *FallbackRenderer) RenderInput(message string) (string, error) {
	fmt.Printf("%s: ", message)
	reader := bufio.NewReader(os.Stdin)
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(response), nil
}

// RenderProgress renders progress (simple implementation)
func (r *FallbackRenderer) RenderProgress(title string, current, total int) error {
	percentage := float64(current) / float64(total) * 100
	fmt.Printf("\r%s: %.1f%% (%d/%d)", title, percentage, current, total)
	if current == total {
		fmt.Println()
	}
	return nil
}

// Helper functions
func formatValue(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}
