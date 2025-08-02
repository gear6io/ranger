package renderers

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"

	"github.com/TFMV/icebox/deprecated/display"
	"github.com/pterm/pterm"
)

// PTermRenderer provides rich terminal output using PTerm
type PTermRenderer struct {
	theme display.Theme
}

// NewPTermRenderer creates a new PTerm renderer
func NewPTermRenderer() *PTermRenderer {
	return &PTermRenderer{
		theme: display.DefaultTheme,
	}
}

// RenderTable renders a table using PTerm's table functionality
func (r *PTermRenderer) RenderTable(data display.TableData, options display.TableOptions) error {
	switch options.Format {
	case display.FormatCSV:
		return r.renderCSV(data)
	case display.FormatJSON:
		return r.renderJSON(data)
	case display.FormatMarkdown:
		return r.renderMarkdown(data)
	default:
		return r.renderTable(data, options)
	}
}

// renderTable renders a rich table using PTerm
func (r *PTermRenderer) renderTable(data display.TableData, options display.TableOptions) error {
	if len(data.Rows) == 0 {
		return nil
	}

	// Convert data to PTerm format
	tableData := make([][]string, len(data.Rows)+1)

	// Headers
	tableData[0] = data.Headers

	// Rows
	for i, row := range data.Rows {
		tableData[i+1] = make([]string, len(row))
		for j, cell := range row {
			tableData[i+1][j] = formatValue(cell)
		}
	}

	// Create PTerm table with styling
	table := pterm.DefaultTable.WithHasHeader().WithData(tableData)

	// Apply theme styling
	if options.Theme != nil {
		table = table.WithHeaderStyle(pterm.NewStyle(pterm.FgLightBlue, pterm.Bold))
		table = table.WithRowSeparator("-")
	}

	// Render the table
	return table.Render()
}

// renderCSV renders data as CSV
func (r *PTermRenderer) renderCSV(data display.TableData) error {
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
func (r *PTermRenderer) renderJSON(data display.TableData) error {
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

// renderMarkdown renders data as Markdown table
func (r *PTermRenderer) renderMarkdown(data display.TableData) error {
	if len(data.Rows) == 0 {
		return nil
	}

	// Print headers
	fmt.Print("|")
	for _, header := range data.Headers {
		fmt.Printf(" %s |", header)
	}
	fmt.Println()

	// Print separator
	fmt.Print("|")
	for range data.Headers {
		fmt.Print(" --- |")
	}
	fmt.Println()

	// Print rows
	for _, row := range data.Rows {
		fmt.Print("|")
		for i, cell := range row {
			if i < len(data.Headers) {
				fmt.Printf(" %s |", formatValue(cell))
			}
		}
		fmt.Println()
	}

	return nil
}

// RenderMessage renders messages with colors and icons
func (r *PTermRenderer) RenderMessage(level display.MessageLevel, message string) {
	switch level {
	case display.MessageLevelSuccess:
		pterm.Success.Println(message)
	case display.MessageLevelError:
		pterm.Error.Println(message)
	case display.MessageLevelWarning:
		pterm.Warning.Println(message)
	case display.MessageLevelInfo:
		pterm.Info.Println(message)
	}
}

// RenderConfirm renders an interactive confirmation prompt
func (r *PTermRenderer) RenderConfirm(message string) bool {
	result, _ := pterm.DefaultInteractiveConfirm.Show(message)
	return result
}

// RenderSelect renders an interactive selection menu
func (r *PTermRenderer) RenderSelect(message string, options []string) (int, error) {
	result, err := pterm.DefaultInteractiveSelect.WithOptions(options).Show(message)
	if err != nil {
		return 0, err
	}

	// Find the index of the selected option
	for i, option := range options {
		if option == result {
			return i, nil
		}
	}

	return 0, fmt.Errorf("selected option not found")
}

// RenderInput renders an interactive input prompt
func (r *PTermRenderer) RenderInput(message string) (string, error) {
	result, err := pterm.DefaultInteractiveTextInput.Show(message)
	return result, err
}

// RenderProgress renders a progress bar
func (r *PTermRenderer) RenderProgress(title string, current, total int) error {
	// Create a progress bar
	progressbar, _ := pterm.DefaultProgressbar.WithTotal(total).WithTitle(title).Start()
	progressbar.Current = current

	if current == total {
		_, _ = progressbar.Stop() // Ignore error for progress bar stop
	}

	return nil
}
