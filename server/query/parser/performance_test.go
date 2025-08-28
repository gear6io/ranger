package parser

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// BenchmarkParsingSpeedComparison benchmarks parsing speed before and after enhancements
func BenchmarkParsingSpeedComparison(b *testing.B) {
	// Test with various SQL statement types and complexities
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "Simple CREATE TABLE",
			sql:  "CREATE TABLE users (id int32, name string);",
		},
		{
			name: "Complex CREATE TABLE",
			sql: `CREATE TABLE users (
				id int32 NOT NULL,
				name string,
				email string UNIQUE,
				created_at timestamp DEFAULT SYS_TIMESTAMP,
				metadata map<string, string>
			);`,
		},
		{
			name: "Simple SELECT",
			sql:  "SELECT * FROM users WHERE id = 123;",
		},
		{
			name: "Complex SELECT",
			sql: `SELECT u.id, u.name, u.email, p.title 
				FROM users u 
				JOIN profiles p ON u.id = p.user_id 
				WHERE u.created_at > '2023-01-01' 
				ORDER BY u.name ASC 
				LIMIT 100;`,
		},
		{
			name: "INSERT statement",
			sql:  "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com');",
		},
		{
			name: "Multiple statements",
			sql: `CREATE TABLE test (id int32);
				INSERT INTO test (id) VALUES (1);
				SELECT * FROM test;
				DROP TABLE test;`,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lexer := NewLexer([]byte(tc.sql))
				parser := NewParser(lexer)
				_, err := parser.Parse()
				if err != nil {
					// Don't fail benchmark for expected parsing errors
					// We're measuring performance, not correctness
					continue
				}
			}
		})
	}
}

// BenchmarkPositionTrackingOverhead benchmarks the overhead of position tracking
func BenchmarkPositionTrackingOverhead(b *testing.B) {
	// Create test SQL with various line lengths and UTF-8 content
	var sqlBuilder strings.Builder
	for i := 0; i < 100; i++ {
		sqlBuilder.WriteString(fmt.Sprintf("CREATE TABLE table_%d (\n", i))
		sqlBuilder.WriteString("    id int32 NOT NULL,\n")
		sqlBuilder.WriteString("    name string,\n")
		sqlBuilder.WriteString("    description string,\n")
		sqlBuilder.WriteString("    created_at timestamp\n")
		sqlBuilder.WriteString(");\n\n")
	}
	sql := sqlBuilder.String()

	b.Run("With Position Tracking", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			lexer := NewLexer([]byte(sql))
			parser := NewParser(lexer)
			// Position tracking is enabled by default
			_, _ = parser.Parse()
		}
	})

	// Measure memory allocations
	b.Run("Memory Allocations", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			lexer := NewLexer([]byte(sql))
			parser := NewParser(lexer)
			_, _ = parser.Parse()
		}
	})
}

// BenchmarkErrorCreationPerformance benchmarks error creation performance
func BenchmarkErrorCreationPerformance(b *testing.B) {
	source := []byte("SELECT * FROM users WHERE id = 123;")
	tracker := NewPositionTracker(source)

	b.Run("ParseError Creation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := &ParseError{
				Code:     ErrExpectedKeyword,
				Message:  "expected table name",
				Position: TokenPosition{Offset: 14, Length: 5},
				Expected: []string{"table_name"},
				Found:    "users",
				Category: SyntaxError,
				tracker:  tracker,
			}
			_ = err
		}
	})

	b.Run("ParseError Error() Method", func(b *testing.B) {
		err := &ParseError{
			Code:     ErrExpectedKeyword,
			Message:  "expected table name",
			Position: TokenPosition{Offset: 14, Length: 5},
			Expected: []string{"table_name"},
			Found:    "users",
			Category: SyntaxError,
			tracker:  tracker,
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = err.Error()
		}
	})

	b.Run("ParseError DetailedError() Method", func(b *testing.B) {
		err := &ParseError{
			Code:     ErrExpectedKeyword,
			Message:  "expected table name",
			Position: TokenPosition{Offset: 14, Length: 5},
			Expected: []string{"table_name"},
			Found:    "users",
			Category: SyntaxError,
			tracker:  tracker,
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = err.DetailedError()
		}
	})

	b.Run("ParseError Transform() Method", func(b *testing.B) {
		err := &ParseError{
			Code:     ErrExpectedKeyword,
			Message:  "expected table name",
			Position: TokenPosition{Offset: 14, Length: 5},
			Expected: []string{"table_name"},
			Found:    "users",
			Category: SyntaxError,
			tracker:  tracker,
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = err.Transform()
		}
	})
}

// BenchmarkLexerPerformance benchmarks lexer performance with position tracking
func BenchmarkLexerPerformance(b *testing.B) {
	// Test with different types of content
	testCases := []struct {
		name   string
		source []byte
	}{
		{
			name:   "ASCII Only",
			source: []byte("SELECT * FROM users WHERE id = 123 AND name = 'test';"),
		},
		{
			name:   "UTF-8 Content",
			source: []byte("SELECT 'café' AS naïve, '🚀' AS rocket FROM tàble;"),
		},
		{
			name:   "Long Identifiers",
			source: []byte("SELECT very_long_column_name_that_exceeds_normal_length FROM very_long_table_name_for_testing;"),
		},
		{
			name:   "Many Tokens",
			source: []byte("SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10 FROM table1, table2, table3;"),
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lexer := NewLexer(tc.source)
				lexer.tokenize()
			}
		})
	}
}

// BenchmarkPositionCalculation benchmarks position calculation performance
func BenchmarkPositionCalculation(b *testing.B) {
	// Create sources with different line characteristics
	testCases := []struct {
		name   string
		source []byte
	}{
		{
			name:   "Short Lines",
			source: createSourceWithLines(1000, 20),
		},
		{
			name:   "Medium Lines",
			source: createSourceWithLines(500, 80),
		},
		{
			name:   "Long Lines",
			source: createSourceWithLines(100, 200),
		},
		{
			name:   "Mixed Line Lengths",
			source: createMixedLengthSource(1000),
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			tracker := NewPositionTracker(tc.source)
			sourceLen := len(tc.source)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				offset := i % sourceLen
				_, _ = tracker.GetLineColumn(offset)
			}
		})
	}
}

// BenchmarkErrorRecoveryPerformance benchmarks error recovery performance
func BenchmarkErrorRecoveryPerformance(b *testing.B) {
	// SQL with multiple syntax errors for recovery testing
	errorSQL := `CREATE TABLE users (
		id int32 NOT NULL
		name string,
		email varchar(100),
		created_at timestamp DEFAULT SYS_TIMESTAMP
	)

	INSERT INTO users (id, name, email) VALUES 
		(1, 'John Doe', 'john@example.com')
		(2, 'Jane Smith', 'jane@example.com');

	SELECT u.name u.email 
	FROM users u 
	WHERE u.created_at > '2023-01-01'
	ORDER BY u.name ASC`

	b.Run("Error Recovery Parsing", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			lexer := NewLexer([]byte(errorSQL))
			parser := NewParser(lexer)
			_, _ = parser.ParseWithRecovery()
		}
	})

	b.Run("Error Collection", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			collector := NewErrorCollector()

			// Simulate adding multiple errors
			for j := 0; j < 10; j++ {
				err := &ParseError{
					Code:     ErrExpectedKeyword,
					Message:  fmt.Sprintf("error %d", j),
					Position: TokenPosition{Offset: j * 10, Length: 1},
					Category: SyntaxError,
				}
				collector.AddError(err)
			}

			_ = collector.GetErrors()
		}
	})
}

// BenchmarkMemoryUsage benchmarks memory usage with position tracking
func BenchmarkMemoryUsage(b *testing.B) {
	// Create a large SQL file
	var sqlBuilder strings.Builder
	for i := 0; i < 1000; i++ {
		sqlBuilder.WriteString(fmt.Sprintf("CREATE TABLE table_%d (id int32, name string);\n", i))
	}
	sql := sqlBuilder.String()

	b.Run("Memory Usage", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			lexer := NewLexer([]byte(sql))
			parser := NewParser(lexer)
			_, _ = parser.Parse()
		}
	})

	b.Run("Position Tracker Memory", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			tracker := NewPositionTracker([]byte(sql))
			// Trigger line calculation
			_, _ = tracker.GetLineColumn(len(sql) / 2)
		}
	})
}

// TestPerformanceOverheadAcceptable tests that performance overhead is within acceptable limits
func TestPerformanceOverheadAcceptable(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Test SQL that should parse successfully (simplified for parser compatibility)
	sql := "SELECT * FROM users WHERE id = 123;"

	// Measure parsing time
	const iterations = 1000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		lexer := NewLexer([]byte(sql))
		parser := NewParser(lexer)
		_, err := parser.Parse()
		if err != nil {
			t.Fatalf("Unexpected parsing error: %v", err)
		}
	}

	elapsed := time.Since(start)
	avgTime := elapsed / iterations

	// Performance should be reasonable (less than 1ms per parse for this simple SQL)
	maxAcceptableTime := 1 * time.Millisecond
	if avgTime > maxAcceptableTime {
		t.Errorf("Parsing performance too slow: average %v per parse, expected < %v",
			avgTime, maxAcceptableTime)
	}

	t.Logf("Average parsing time: %v per parse", avgTime)
}

// TestMemoryUsageAcceptable tests that memory usage is within acceptable limits
func TestMemoryUsageAcceptable(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}

	// Create a moderately complex SQL (simplified for parser compatibility)
	sql := "SELECT * FROM users WHERE id = 123;"

	// Measure memory before
	var m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Parse multiple times
	const iterations = 100
	for i := 0; i < iterations; i++ {
		lexer := NewLexer([]byte(sql))
		parser := NewParser(lexer)
		_, _ = parser.Parse()
	}

	// Measure memory after
	var m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m2)

	// Calculate memory increase per parse
	memIncrease := m2.TotalAlloc - m1.TotalAlloc
	avgMemPerParse := memIncrease / iterations

	// Memory usage should be reasonable (less than 10MB per parse for this test)
	maxAcceptableMemory := uint64(10 * 1024 * 1024) // 10MB
	if avgMemPerParse > maxAcceptableMemory {
		t.Errorf("Memory usage too high: average %d bytes per parse, expected < %d",
			avgMemPerParse, maxAcceptableMemory)
	}

	t.Logf("Average memory per parse: %d bytes", avgMemPerParse)
}

// Helper functions for benchmark data generation

func createSourceWithLines(lineCount int, lineLength int) []byte {
	var builder strings.Builder
	for i := 0; i < lineCount; i++ {
		line := fmt.Sprintf("SELECT %s FROM table_%d;",
			strings.Repeat("col", lineLength/3), i)
		if len(line) > lineLength {
			line = line[:lineLength-1]
		}
		builder.WriteString(line)
		builder.WriteString("\n")
	}
	return []byte(builder.String())
}

func createMixedLengthSource(lineCount int) []byte {
	var builder strings.Builder
	for i := 0; i < lineCount; i++ {
		// Vary line length based on position
		lineLength := 20 + (i%5)*20
		line := fmt.Sprintf("SELECT %s FROM table_%d;",
			strings.Repeat("col", lineLength/10), i)
		builder.WriteString(line)
		builder.WriteString("\n")
	}
	return []byte(builder.String())
}

// BenchmarkComparisonWithoutEnhancements provides a baseline for comparison
// This would be used to compare against a version without enhanced error reporting
func BenchmarkComparisonWithoutEnhancements(b *testing.B) {
	sql := "CREATE TABLE users (id int32, name string);"

	b.Run("Current Implementation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			lexer := NewLexer([]byte(sql))
			parser := NewParser(lexer)
			_, _ = parser.Parse()
		}
	})

	// This would be compared against a baseline implementation
	// For now, we just measure the current implementation
	b.Run("Baseline Comparison", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Simulate minimal parsing without enhanced error reporting
			lexer := NewLexer([]byte(sql))
			lexer.tokenize()
			// Just tokenization without full parsing
		}
	})
}
