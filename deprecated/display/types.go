package display

// TableData represents the data for a table
type TableData struct {
	Headers []string
	Rows    [][]interface{}
	Footer  []string
}

// TableOptions represents configuration options for table rendering
type TableOptions struct {
	Format         OutputFormat
	MaxWidth       int
	Pagination     *PaginationOptions
	Sorting        *SortOptions
	Filtering      *FilterOptions
	Theme          *Theme
	ShowRowNumbers bool
	CompactMode    bool
	Title          string
}

// PaginationOptions represents pagination configuration
type PaginationOptions struct {
	PageSize int
	ShowAll  bool
}

// SortOptions represents sorting configuration
type SortOptions struct {
	Column     string
	Descending bool
}

// FilterOptions represents filtering configuration
type FilterOptions struct {
	Column   string
	Operator string // =, !=, <, >, <=, >=, contains, starts_with, ends_with
	Value    interface{}
}

// DefaultTableOptions returns default table options
func DefaultTableOptions() TableOptions {
	return TableOptions{
		Format:   FormatTable,
		MaxWidth: 120,
	}
}

// TableBuilder for building tables with fluent API
type TableBuilder struct {
	display *DisplayImpl
	data    TableData
	options TableOptions
}

// WithFormat sets the output format
func (tb *TableBuilder) WithFormat(format OutputFormat) *TableBuilder {
	tb.options.Format = format
	return tb
}

// WithPagination sets pagination options
func (tb *TableBuilder) WithPagination(pageSize int) *TableBuilder {
	tb.options.Pagination = &PaginationOptions{
		PageSize: pageSize,
		ShowAll:  false,
	}
	return tb
}

// WithTheme sets the theme
func (tb *TableBuilder) WithTheme(theme Theme) *TableBuilder {
	tb.options.Theme = &theme
	return tb
}

// WithSorting sets sorting options
func (tb *TableBuilder) WithSorting(column string, descending bool) *TableBuilder {
	tb.options.Sorting = &SortOptions{
		Column:     column,
		Descending: descending,
	}
	return tb
}

// WithFiltering sets filtering options
func (tb *TableBuilder) WithFiltering(column, operator string, value interface{}) *TableBuilder {
	tb.options.Filtering = &FilterOptions{
		Column:   column,
		Operator: operator,
		Value:    value,
	}
	return tb
}

// WithRowNumbers enables row number display
func (tb *TableBuilder) WithRowNumbers() *TableBuilder {
	tb.options.ShowRowNumbers = true
	return tb
}

// WithCompactMode enables compact display mode
func (tb *TableBuilder) WithCompactMode() *TableBuilder {
	tb.options.CompactMode = true
	return tb
}

// WithTitle sets the table title
func (tb *TableBuilder) WithTitle(title string) *TableBuilder {
	tb.options.Title = title
	return tb
}

// WithMaxWidth sets the maximum table width
func (tb *TableBuilder) WithMaxWidth(width int) *TableBuilder {
	tb.options.MaxWidth = width
	return tb
}

// Render renders the table
func (tb *TableBuilder) Render() error {
	data := tb.data

	// Apply filtering if needed
	if tb.options.Filtering != nil {
		data = tb.applyFiltering(data)
	}

	// Apply sorting if needed
	if tb.options.Sorting != nil {
		data = tb.applySorting(data)
	}

	// Apply pagination if needed
	if tb.options.Pagination != nil && !tb.options.Pagination.ShowAll {
		totalRows := len(data.Rows)
		if totalRows > tb.options.Pagination.PageSize {
			data.Rows = data.Rows[:tb.options.Pagination.PageSize]
			tb.display.Warning("⚠️  Showing first %d of %d rows (use --max-rows to adjust)",
				tb.options.Pagination.PageSize, totalRows)
		}
	}

	return tb.display.renderer.RenderTable(data, tb.options)
}
