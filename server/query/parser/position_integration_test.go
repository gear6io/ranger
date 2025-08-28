package parser

import (
	"testing"
)

// TestTokenPositionIntegration tests that tokens are created with position information
func TestTokenPositionIntegration(t *testing.T) {
	input := "SELECT * FROM users;"
	lexer := NewLexer([]byte(input))
	lexer.tokenize()

	if len(lexer.tokens) == 0 {
		t.Fatal("No tokens generated")
	}

	// Test that each token has position information
	for i, token := range lexer.tokens {
		if token.Position.Offset < 0 {
			t.Errorf("Token %d has invalid offset: %d", i, token.Position.Offset)
		}
		if token.Position.Length <= 0 && token.tokenT != EOF_TOK {
			t.Errorf("Token %d has invalid length: %d", i, token.Position.Length)
		}

		// Verify that the position corresponds to the actual token in the input
		if token.tokenT != EOF_TOK {
			start := token.Position.Offset
			end := start + token.Position.Length
			if end > len(input) {
				t.Errorf("Token %d position extends beyond input: start=%d, length=%d, input_len=%d",
					i, start, token.Position.Length, len(input))
			}
		}
	}

	// Test specific tokens
	expectedTokens := []struct {
		tokenType TokenType
		value     interface{}
		minOffset int
	}{
		{KEYWORD_TOK, "SELECT", 0},
		{ASTERISK_TOK, "*", 6},
		{KEYWORD_TOK, "FROM", 8},
		{IDENT_TOK, "users", 13},
		{SEMICOLON_TOK, ";", 18},
	}

	if len(lexer.tokens) < len(expectedTokens) {
		t.Fatalf("Expected at least %d tokens, got %d", len(expectedTokens), len(lexer.tokens))
	}

	for i, expected := range expectedTokens {
		token := lexer.tokens[i]
		if token.tokenT != expected.tokenType {
			t.Errorf("Token %d: expected type %v, got %v", i, expected.tokenType, token.tokenT)
		}
		if token.Position.Offset < expected.minOffset {
			t.Errorf("Token %d: expected offset >= %d, got %d", i, expected.minOffset, token.Position.Offset)
		}
	}
}

// TestLexerPositionTracker tests that the lexer has a position tracker
func TestLexerPositionTracker(t *testing.T) {
	input := "SELECT * FROM users;"
	lexer := NewLexer([]byte(input))

	if lexer.tracker == nil {
		t.Fatal("Lexer should have a position tracker")
	}

	// Test that the tracker can calculate positions
	line, column := lexer.tracker.GetLineColumn(0)
	if line != 1 || column != 1 {
		t.Errorf("Expected position (1,1) for offset 0, got (%d,%d)", line, column)
	}

	line, column = lexer.tracker.GetLineColumn(7) // Position of '*'
	if line != 1 || column != 8 {
		t.Errorf("Expected position (1,8) for offset 7, got (%d,%d)", line, column)
	}
}

// TestParserPositionTracker tests that the parser has a shared position tracker
func TestParserPositionTracker(t *testing.T) {
	input := "SELECT * FROM users;"
	lexer := NewLexer([]byte(input))
	parser := NewParser(lexer)

	if parser.tracker == nil {
		t.Fatal("Parser should have a position tracker")
	}

	if parser.tracker != lexer.tracker {
		t.Fatal("Parser and lexer should share the same position tracker")
	}
}
