package display

import (
	"fmt"
	"sort"
	"strings"
)

// applyFiltering applies filtering to table data
func (tb *TableBuilder) applyFiltering(data TableData) TableData {
	if tb.options.Filtering == nil {
		return data
	}

	// Find column index
	colIndex := -1
	for i, header := range data.Headers {
		if strings.EqualFold(header, tb.options.Filtering.Column) {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		tb.display.Warning("Column '%s' not found for filtering", tb.options.Filtering.Column)
		return data
	}

	// Filter rows
	var filteredRows [][]interface{}
	for _, row := range data.Rows {
		if colIndex >= len(row) {
			continue
		}

		if matchesFilter(row[colIndex], tb.options.Filtering.Operator, tb.options.Filtering.Value) {
			filteredRows = append(filteredRows, row)
		}
	}

	return TableData{
		Headers: data.Headers,
		Rows:    filteredRows,
		Footer:  data.Footer,
	}
}

// applySorting applies sorting to table data
func (tb *TableBuilder) applySorting(data TableData) TableData {
	if tb.options.Sorting == nil {
		return data
	}

	// Find column index
	colIndex := -1
	for i, header := range data.Headers {
		if strings.EqualFold(header, tb.options.Sorting.Column) {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		tb.display.Warning("Column '%s' not found for sorting", tb.options.Sorting.Column)
		return data
	}

	// Create a copy of rows for sorting
	sortedRows := make([][]interface{}, len(data.Rows))
	copy(sortedRows, data.Rows)

	// Sort rows
	sort.Slice(sortedRows, func(i, j int) bool {
		if colIndex >= len(sortedRows[i]) || colIndex >= len(sortedRows[j]) {
			return false
		}

		result := compareValues(sortedRows[i][colIndex], sortedRows[j][colIndex])
		if tb.options.Sorting.Descending {
			return result > 0
		}
		return result < 0
	})

	return TableData{
		Headers: data.Headers,
		Rows:    sortedRows,
		Footer:  data.Footer,
	}
}

// matchesFilter checks if a value matches the filter criteria
func matchesFilter(value interface{}, operator string, filterValue interface{}) bool {
	// Convert to strings for comparison
	valStr := fmt.Sprintf("%v", value)
	filterStr := fmt.Sprintf("%v", filterValue)

	switch operator {
	case "=", "==":
		return valStr == filterStr
	case "!=", "<>":
		return valStr != filterStr
	case "contains":
		return strings.Contains(strings.ToLower(valStr), strings.ToLower(filterStr))
	case "starts_with":
		return strings.HasPrefix(strings.ToLower(valStr), strings.ToLower(filterStr))
	case "ends_with":
		return strings.HasSuffix(strings.ToLower(valStr), strings.ToLower(filterStr))
	case "<", ">", "<=", ">=":
		// Try numeric comparison
		return compareNumeric(value, operator, filterValue)
	default:
		return false
	}
}

// compareValues compares two values for sorting
func compareValues(a, b interface{}) int {
	// Try numeric comparison first
	if aNum, aOk := toFloat64(a); aOk {
		if bNum, bOk := toFloat64(b); bOk {
			if aNum < bNum {
				return -1
			} else if aNum > bNum {
				return 1
			}
			return 0
		}
	}

	// Fall back to string comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return strings.Compare(aStr, bStr)
}

// compareNumeric performs numeric comparison
func compareNumeric(value interface{}, operator string, filterValue interface{}) bool {
	valNum, valOk := toFloat64(value)
	filterNum, filterOk := toFloat64(filterValue)

	if !valOk || !filterOk {
		return false
	}

	switch operator {
	case "<":
		return valNum < filterNum
	case ">":
		return valNum > filterNum
	case "<=":
		return valNum <= filterNum
	case ">=":
		return valNum >= filterNum
	default:
		return false
	}
}

// toFloat64 attempts to convert a value to float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case string:
		// Try to parse string as number
		var f float64
		_, err := fmt.Sscanf(val, "%f", &f)
		return f, err == nil
	default:
		return 0, false
	}
}

// formatBytes formats bytes into human-readable format
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// TruncateString truncates a string to maxLen with ellipsis
func TruncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// WrapText wraps text to fit within maxWidth
func WrapText(text string, maxWidth int) []string {
	if maxWidth <= 0 || len(text) <= maxWidth {
		return []string{text}
	}

	var lines []string
	words := strings.Fields(text)
	var currentLine strings.Builder

	for _, word := range words {
		if currentLine.Len() == 0 {
			currentLine.WriteString(word)
		} else if currentLine.Len()+1+len(word) <= maxWidth {
			currentLine.WriteString(" ")
			currentLine.WriteString(word)
		} else {
			lines = append(lines, currentLine.String())
			currentLine.Reset()
			currentLine.WriteString(word)
		}
	}

	if currentLine.Len() > 0 {
		lines = append(lines, currentLine.String())
	}

	return lines
}
