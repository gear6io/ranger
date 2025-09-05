// Package parser
// SQL parser package integrated with Ranger
// Copyright (C) Alex Gaetano Padula
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
package parser

// INTEGRATED WITH RANGER - Some functionality commented out for compatibility

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/types"
)

// Iceberg data types - centralized from types package
// Replace basicDataTypes array with centralized Iceberg types from new package
var icebergDataTypes = types.GetAllSupportedTypes()

var keywords = append([]string{
	"ALL", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "AVG", "ALTER",
	"BEGIN", "BETWEEN", "BY", "CHECK", "CLOSE", "COBOL", "COMMIT",
	"CONTINUE", "COUNT", "CREATE", "CURRENT", "CURSOR", "DECLARE", "DELETE", "DROP", "DESC", "DISTINCT", "DATABASE",
	"END", "ESCAPE", "EXEC", "EXISTS",
	"FETCH", "FOR", "FORTRAN", "FOUND", "FROM",
	"GO", "GOTO", "GRANT", "GROUP", "HAVING",
	"IN", "INDEX", "INDICATOR", "INSERT", "INTO", "IS", "SEQUENCE",
	"LANGUAGE", "LIKE",
	"MAX", "MIN", "MODULE", "NOT", "NULL",
	"OF", "ON", "OPEN", "OPTION", "OR", "ORDER",
	"PASCAL", "PLI", "PRECISION", "PRIVILEGES", "PROCEDURE", "PUBLIC", "ROLLBACK",
	"SCHEMA", "SECTION", "SELECT", "SET", "SOME",
	"SQL", "SQLCODE", "SQLERROR", "SUM",
	"TABLE", "TO", "UNION", "UNIQUE", "UPDATE", "USER",
	"VALUES", "VIEW", "WHENEVER", "WHERE", "WITH", "WORK", "USE", "LIMIT", "OFFSET", "IDENTIFIED", "CONNECT", "REVOKE", "SHOW",
	"PRIMARY", "FOREIGN", "KEY", "REFERENCES", "DEFAULT",
	"UPPER", "LOWER", "CAST", "COALESCE", "REVERSE", "ROUND", "POSITION", "LENGTH", "REPLACE",
	"CONCAT", "SUBSTRING", "TRIM", "GENERATE_UUID", "SYS_DATE", "SYS_TIME", "SYS_TIMESTAMP", "SYS_DATETIME",
	"CASE", "WHEN", "THEN", "ELSE", "END", "IF", "ELSEIF", "DEALLOCATE", "NEXT", "WHILE", "PRINT", "EXPLAIN",
	"COMPRESS", "ENCRYPT", "COLUMN", "IF NOT EXISTS", "STORAGE", "PARTITION", "SETTINGS",
}, icebergDataTypes...)

// Ranger-compatible type definitions to replace SQL parser types

// PrivilegeAction represents a database privilege action
type PrivilegeAction string

const (
	PRIV_ALL        PrivilegeAction = "ALL"
	PRIV_SELECT     PrivilegeAction = "SELECT"
	PRIV_BEGIN      PrivilegeAction = "BEGIN"
	PRIV_COMMIT     PrivilegeAction = "COMMIT"
	PRIV_ROLLBACK   PrivilegeAction = "ROLLBACK"
	PRIV_INSERT     PrivilegeAction = "INSERT"
	PRIV_UPDATE     PrivilegeAction = "UPDATE"
	PRIV_DELETE     PrivilegeAction = "DELETE"
	PRIV_DROP       PrivilegeAction = "DROP"
	PRIV_CREATE     PrivilegeAction = "CREATE"
	PRIV_CONNECT    PrivilegeAction = "CONNECT"
	PRIV_ALTER      PrivilegeAction = "ALTER"
	PRIV_REVOKE     PrivilegeAction = "REVOKE"
	PRIV_GRANT      PrivilegeAction = "GRANT"
	PRIV_SHOW       PrivilegeAction = "SHOW"
	PRIV_EXEC       PrivilegeAction = "EXEC"
	PRIV_EXPLAIN    PrivilegeAction = "EXPLAIN"
	PRIV_PRINT      PrivilegeAction = "PRINT"
	PRIV_DECLARE    PrivilegeAction = "DECLARE"
	PRIV_OPEN       PrivilegeAction = "OPEN"
	PRIV_CLOSE      PrivilegeAction = "CLOSE"
	PRIV_DEALLOCATE PrivilegeAction = "DEALLOCATE"
	PRIV_FETCH      PrivilegeAction = "FETCH"
	PRIV_WHILE      PrivilegeAction = "WHILE"
	PRIV_RETURN     PrivilegeAction = "RETURN"
	PRIV_BREAK      PrivilegeAction = "BREAK"
	PRIV_SET        PrivilegeAction = "SET"
	PRIV_EXIT       PrivilegeAction = "EXIT"
)

// System functions for default values
type SysDate struct{}
type SysTime struct{}
type SysTimestamp struct{}
type GenUUID struct{}

// TableSchema represents a table's schema
type TableSchema struct {
	ColumnDefinitions map[string]*ColumnDefinition
}

// ColumnDefinition represents a column definition with validated Iceberg types
type ColumnDefinition struct {
	Name         string     // Column name
	DataType     string     // Must be valid Iceberg type
	Length       int        // For fixed-length types
	Precision    int        // For decimal types
	Scale        int        // For decimal types
	IsNullable   bool       // Nullable constraint (default true)
	DefaultValue string     // Default value as string
	References   *Reference // Foreign key reference
	Sequence     *string    // For auto-increment/sequence columns
	NotNull      bool       // NOT NULL constraint (alias for !IsNullable)
	Unique       bool       // UNIQUE constraint
	Check        *string    // CHECK constraint
	// Iceberg-specific fields for complex types
	ElementType string              // For list types
	KeyType     string              // For map types
	ValueType   string              // For map types
	Fields      []*ColumnDefinition // For struct types
	// Internal validation state
	validated bool // Track validation state to avoid repeated validation
}

// Validate validates the column definition with Iceberg type checking
func (cd *ColumnDefinition) Validate() error {
	if cd.validated {
		return nil // Already validated
	}

	if cd.Name == "" {
		return errors.New(ErrEmptyColumnName, "column name cannot be empty", nil)
	}

	// Validate Iceberg type using centralized validator
	validator := types.NewIcebergTypeValidator()
	if !validator.IsValidType(cd.DataType) {
		// Check if it's a legacy SQL type and provide specific mapping
		upperType := strings.ToUpper(cd.DataType)
		legacyMappings := map[string]string{
			"VARCHAR":   "string",
			"CHAR":      "string",
			"TEXT":      "string",
			"INT":       "int32",
			"INTEGER":   "int32",
			"BIGINT":    "int64",
			"SMALLINT":  "int32",
			"TINYINT":   "int32",
			"FLOAT":     "float32",
			"DOUBLE":    "float64",
			"REAL":      "float32",
			"NUMERIC":   "decimal",
			"DECIMAL":   "decimal",
			"BOOL":      "boolean",
			"DATETIME":  "timestamp",
			"TIMESTAMP": "timestamp",
			"DATE":      "date",
			"TIME":      "time",
			"BLOB":      "binary",
			"BINARY":    "binary",
			"VARBINARY": "binary",
		}

		if icebergEquivalent, isLegacy := legacyMappings[upperType]; isLegacy {
			return errors.Newf(ErrUnsupportedSQLType, "column '%s': unsupported SQL type '%s'. Use Iceberg type '%s' instead",
				cd.Name, cd.DataType, icebergEquivalent)
		}

		supportedTypes := validator.GetSupportedTypes()
		return errors.Newf(ErrInvalidIcebergType, "column '%s': invalid Iceberg type '%s'. Supported types: %v",
			cd.Name, cd.DataType, supportedTypes)
	}

	// Validate complex types
	if err := validator.ValidateComplexType(cd.DataType); err != nil {
		return errors.Newf(ErrComplexTypeParseError, "column '%s': complex type validation failed: %v", cd.Name, err)
	}

	cd.validated = true
	return nil
}

// Reference represents a foreign key reference
type Reference struct {
	TableName  string
	ColumnName string
}

// IsValidDataType checks if a string is a valid Iceberg data type
func IsValidDataType(dataType string) bool {
	dataType = strings.TrimSpace(dataType)
	validator := types.NewIcebergTypeValidator()
	return validator.IsValidType(dataType)
}

// ValidateIcebergType validates an Iceberg type string and returns detailed error information
func ValidateIcebergType(typeStr string) error {
	return types.ValidateTypeString(typeStr)
}

// ParseIcebergType parses and validates an Iceberg type string
func ParseIcebergType(typeStr string) (types.IcebergType, error) {
	return types.ParseAndValidateType(typeStr)
}

type TokenType int // Token type

const (
	EOF_TOK        = iota // End of input
	KEYWORD_TOK           // Keywords like SELECT, FROM, WHERE, etc.
	IDENT_TOK             // Identifiers like table names, column names, etc.
	COMMENT_TOK           // Comments
	LITERAL_TOK           // Literals like strings, numbers, etc.
	LPAREN_TOK            // (
	RPAREN_TOK            // )
	SEMICOLON_TOK         // ;
	DATATYPE_TOK          // Data types like INT, CHAR, etc.
	COMMA_TOK             // ,
	ASTERISK_TOK          // *
	COMPARISON_TOK        // =, <>, <, >, <=, >=
	PLUS_TOK              // +
	MINUS_TOK             // -
	DIVIDE_TOK            // /
	MODULUS_TOK           // %
	AT_TOK                // @
	DOT_TOK               // .
	COLON_TOK             // :
)

// Parser is a parser for SQL
type Parser struct {
	lexer          *Lexer           // Enhanced lexer with position tracking
	pos            int              // Current token position
	tracker        *PositionTracker // Shared position tracker for enhanced error reporting
	errorCollector *ErrorCollector  // Collects multiple parsing errors for recovery
	recoveryMode   bool             // Whether parser is in error recovery mode
}

// Lexer is a lexer for SQL
type Lexer struct {
	input   []byte           // Input to be tokenized
	pos     int              // Position in the input
	tokens  []Token          // Tokens found
	tracker *PositionTracker // Position tracking for enhanced error reporting
}

// Token is a token found by the lexer
type Token struct {
	tokenT   TokenType     // Type of token
	value    interface{}   // Value of token
	Position TokenPosition // Position information for enhanced error reporting
}

// NewLexer creates a new lexer
func NewLexer(input []byte) *Lexer {
	//rewrites(&input)

	return &Lexer{
		input:   input,
		tracker: NewPositionTracker(input),
	}
}

// isLetter returns true if r is a letter
func isLetter(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_' || r == '*'
}

// isDigit returns true if r is a digit
func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

// checkKeyword returns true if s is a keyword
func checkKeyword(s string) bool {
	for _, k := range keywords {
		if strings.EqualFold(s, k) {
			return true
		}
	}
	return false
}

// tryMatchMultiWordKeyword tries to match multi-word keywords like "IF NOT EXISTS"
func (l *Lexer) tryMatchMultiWordKeyword() string {
	// Get the current position to restore later if no match
	originalPos := l.pos

	// Look for multi-word keywords that start with the current character
	for _, keyword := range keywords {
		if strings.Contains(keyword, " ") {
			// This is a multi-word keyword
			keywordWords := strings.Split(keyword, " ")
			if len(keywordWords) > 1 {
				// Check if the first word matches what we're currently looking at
				firstWord := keywordWords[0]
				if len(firstWord) > 0 && l.pos+len(firstWord) <= len(l.input) {
					// Check if the first word matches
					if strings.EqualFold(string(l.input[l.pos:l.pos+len(firstWord)]), firstWord) {
						// Now check if the rest of the keyword follows
						pos := l.pos + len(firstWord)
						matched := true

						for i := 1; i < len(keywordWords); i++ {
							word := keywordWords[i]
							// Skip whitespace
							for pos < len(l.input) && (l.input[pos] == ' ' || l.input[pos] == '\t' || l.input[pos] == '\n') {
								pos++
							}
							// Check if the word matches
							if pos+len(word) > len(l.input) || !strings.EqualFold(string(l.input[pos:pos+len(word)]), word) {
								matched = false
								break
							}
							pos += len(word)
						}

						if matched {
							// Update the lexer position and return the keyword
							l.pos = pos
							return keyword
						}
					}
				}
			}
		}
	}

	// No multi-word keyword matched, restore position
	l.pos = originalPos
	return ""
}

// createToken creates a token with position information
func (l *Lexer) createToken(tokenType TokenType, value interface{}, startPos, endPos int) Token {
	return Token{
		tokenT: tokenType,
		value:  value,
		Position: TokenPosition{
			Offset: startPos,
			Length: endPos - startPos,
		},
	}
}

// nextToken returns the next token
func (l *Lexer) nextToken() Token {
	insideLiteral := false
	var stringLiteral string // string literal
	quoteChar := byte(0)     // The quote character used to start the string literal

	for {
		if l.pos >= len(l.input) {
			return l.createToken(EOF_TOK, nil, l.pos, l.pos)
		}

		switch l.input[l.pos] {
		case '-':
			if !insideLiteral {

				if l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
					startPos := l.pos
					l.pos += 2
					comment := ""
					for l.pos < len(l.input) && l.input[l.pos] != '\n' {
						comment += string(l.input[l.pos])
						l.pos++
					}

					return l.createToken(COMMENT_TOK, comment, startPos, l.pos)
				}
				startPos := l.pos
				l.pos++
				return l.createToken(MINUS_TOK, "-", startPos, l.pos)

			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case ' ', '\t', '\n': // Skip whitespace
			if !insideLiteral {
				l.pos++
				continue
			}
			stringLiteral += string(l.input[l.pos])
			l.pos++
			continue
		case '\\': // Escape character
			if insideLiteral {
				// If the next character is the same as the quote character, add both the escape character and the quote character to the string literal
				if l.pos+1 < len(l.input) && l.input[l.pos+1] == quoteChar {
					stringLiteral += string(l.input[l.pos]) + string(l.input[l.pos+1])
					l.pos += 2
					continue
				}
			}
			l.pos++
			continue
		case '"', '\'':
			if insideLiteral {
				if l.input[l.pos] == quoteChar {
					// End of string literal
					insideLiteral = false
					stringLiteral += string(l.input[l.pos])
					endPos := l.pos + 1
					l.pos++
					// Calculate start position by working backwards from current position
					startPos := endPos - len(stringLiteral)
					return l.createToken(LITERAL_TOK, stringLiteral, startPos, endPos)
				} else {
					// Quote character inside string literal
					stringLiteral += string(l.input[l.pos])
					l.pos++
					continue
				}
			} else {
				// Start of string literal
				insideLiteral = true
				quoteChar = l.input[l.pos]
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '=':
			if !insideLiteral {
				startPos := l.pos
				l.pos++
				return l.createToken(COMPARISON_TOK, "=", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '+':
			if !insideLiteral {
				startPos := l.pos
				l.pos++
				return l.createToken(PLUS_TOK, "+", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '/':
			if !insideLiteral {
				if l.input[l.pos+1] == '*' {
					startPos := l.pos
					l.pos += 2
					comment := ""
					for l.input[l.pos] != '*' && l.input[l.pos+1] != '/' {
						comment += string(l.input[l.pos])
						l.pos++
					}
					l.pos += 2
					return l.createToken(COMMENT_TOK, strings.TrimSpace(comment), startPos, l.pos)
				}
				startPos := l.pos
				l.pos++
				return l.createToken(DIVIDE_TOK, "/", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '%':
			if !insideLiteral {
				startPos := l.pos
				l.pos++
				return l.createToken(MODULUS_TOK, "%", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '@':
			if !insideLiteral {
				startPos := l.pos
				l.pos++
				return l.createToken(AT_TOK, "@", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '.':
			if !insideLiteral {
				startPos := l.pos
				l.pos++
				return l.createToken(DOT_TOK, ".", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case ':':
			if !insideLiteral {
				startPos := l.pos
				l.pos++
				return l.createToken(COLON_TOK, ":", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '<':
			if !insideLiteral {
				startPos := l.pos
				if l.input[l.pos+1] == '>' {
					l.pos += 2
					return l.createToken(COMPARISON_TOK, "<>", startPos, l.pos)
				} else if l.input[l.pos+1] == '=' {
					l.pos += 2
					return l.createToken(COMPARISON_TOK, "<=", startPos, l.pos)
				}
				l.pos++
				return l.createToken(COMPARISON_TOK, "<", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '>':
			if !insideLiteral {
				startPos := l.pos
				if l.input[l.pos+1] == '=' {
					l.pos += 2
					return l.createToken(COMPARISON_TOK, ">=", startPos, l.pos)
				}
				l.pos++
				return l.createToken(COMPARISON_TOK, ">", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '*':
			if !insideLiteral {
				startPos := l.pos
				l.pos++
				return l.createToken(ASTERISK_TOK, "*", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case ',':
			if !insideLiteral {
				startPos := l.pos
				l.pos++
				return l.createToken(COMMA_TOK, ",", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '(':
			if !insideLiteral {
				startPos := l.pos
				l.pos++
				return l.createToken(LPAREN_TOK, "(", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case ')':
			if !insideLiteral {
				startPos := l.pos
				l.pos++
				return l.createToken(RPAREN_TOK, ")", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '!':
			if insideLiteral {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
			continue
		case '$':
			if insideLiteral {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
			continue
		case ';':
			if !insideLiteral {
				startPos := l.pos
				l.pos++
				return l.createToken(SEMICOLON_TOK, ";", startPos, l.pos)
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}

		default:
			if isLetter(rune(l.input[l.pos])) {
				if !insideLiteral {
					switch l.input[l.pos] {
					case 'T', 't':
						if !insideLiteral {
							// check for TRUE
							if l.input[l.pos+1] == 'R' || l.input[l.pos+1] == 'r' {
								if l.input[l.pos+2] == 'U' || l.input[l.pos+2] == 'u' {
									if l.input[l.pos+3] == 'E' || l.input[l.pos+3] == 'e' {
										startPos := l.pos
										l.pos += 4
										return l.createToken(LITERAL_TOK, true, startPos, l.pos)
									}
								}
							}
						}
					case 'F', 'f':
						if !insideLiteral {
							// check for FALSE
							if l.input[l.pos+1] == 'A' || l.input[l.pos+1] == 'a' {
								if l.input[l.pos+2] == 'L' || l.input[l.pos+2] == 'l' {
									if l.input[l.pos+3] == 'S' || l.input[l.pos+3] == 's' {
										if l.input[l.pos+4] == 'E' || l.input[l.pos+4] == 'e' {
											startPos := l.pos
											l.pos += 5
											return l.createToken(LITERAL_TOK, false, startPos, l.pos)
										}
									}
								}
							}
						}
					}

					startPos := l.pos

					// First, try to match multi-word keywords (like "IF NOT EXISTS")
					multiWordKeyword := l.tryMatchMultiWordKeyword()
					if multiWordKeyword != "" {
						return l.createToken(KEYWORD_TOK, multiWordKeyword, startPos, l.pos)
					}

					// If no multi-word keyword found, process as single word
					for isDigit(rune(l.input[l.pos])) || isLetter(rune(l.input[l.pos])) {
						l.pos++
						if l.pos+1 > len(l.input) {
							break
						}
					}

					word := string(l.input[startPos:l.pos])

					// Check if it's an Iceberg data type first (case-sensitive)
					if IsValidDataType(word) {
						return l.createToken(DATATYPE_TOK, word, startPos, l.pos)
					}

					if checkKeyword(word) {
						return l.createToken(KEYWORD_TOK, strings.ToUpper(word), startPos, l.pos)
					} else {
						parsedUInt, err := strconv.ParseUint(string(l.input[startPos:l.pos]), 10, 32) // convert string to uint
						if err == nil {
							// is parsable uint
							return l.createToken(LITERAL_TOK, parsedUInt, startPos, l.pos)

						} else {
							return l.createToken(IDENT_TOK, string(l.input[startPos:l.pos]), startPos, l.pos)
						}
					}
				} else {
					stringLiteral += string(l.input[l.pos])
					l.pos++
					continue
				}
			} else if isDigit(rune(l.input[l.pos])) {
				if !insideLiteral {
					startPos := l.pos
					n := ""

					if l.pos+1 < len(l.input) {

						for isDigit(rune(l.input[l.pos])) || l.input[l.pos] == '.' {

							n += string(l.input[l.pos])
							l.pos++
						}
						parsedUInt, err := strconv.ParseUint(n, 10, 32) // convert string to uint
						if err == nil {
							// is parsable uint
							return l.createToken(LITERAL_TOK, parsedUInt, startPos, l.pos)

						} else {
							parsedFloat, err := strconv.ParseFloat(n, 64) // convert string to float
							if err == nil {
								// is parsable float
								return l.createToken(LITERAL_TOK, parsedFloat, startPos, l.pos)
							}
						}
					}

				} else {
					stringLiteral += string(l.input[l.pos])
					l.pos++
					continue
				}
			}

			l.pos++
		}
	}
}

// Tokenize tokenizes the input
func (l *Lexer) tokenize() {
	for {
		tok := l.nextToken()
		if tok.tokenT == EOF_TOK {
			break
		}
		l.tokens = append(l.tokens, tok)
	}
}

func Parse(query string) (Node, error) {
	lexer := NewLexer([]byte(query))
	parser := NewParser(lexer)
	return parser.Parse()
}

// NewParser creates a new parser
func NewParser(lexer *Lexer) *Parser {
	return &Parser{
		lexer:          lexer,
		tracker:        lexer.tracker, // Share the position tracker with the lexer
		errorCollector: NewErrorCollector(),
		recoveryMode:   false,
	}
}

// consume consumes the next token
func (p *Parser) consume() {
	p.pos++
}

// Enhanced error creation methods for better error reporting

// newSyntaxError creates a new syntax error with enhanced context
func (p *Parser) newSyntaxError(code errors.Code, message string, expected []string, found *Token) *ParseError {
	var foundStr string
	var position TokenPosition

	if found != nil {
		foundStr = fmt.Sprintf("%v", found.value)
		position = found.Position
	} else {
		foundStr = "end of input"
		// Use position at end of input for EOF errors
		position = TokenPosition{
			Offset: len(p.lexer.input),
			Length: 0,
		}
	}

	return &ParseError{
		Code:     code,
		Message:  message,
		Position: position,
		Token:    found,
		Expected: expected,
		Found:    foundStr,
		Category: SyntaxError,
		tracker:  p.tracker,
	}
}

// newTypeError creates a new type validation error with enhanced context
func (p *Parser) newTypeError(code errors.Code, message string, token *Token, suggestions []string) *ParseError {
	var foundStr string
	var position TokenPosition

	if token != nil {
		foundStr = fmt.Sprintf("%v", token.value)
		position = token.Position
	} else {
		foundStr = "unknown type"
		position = TokenPosition{
			Offset: len(p.lexer.input),
			Length: 0,
		}
	}

	return &ParseError{
		Code:     code,
		Message:  message,
		Position: position,
		Token:    token,
		Expected: suggestions,
		Found:    foundStr,
		Category: TypeError,
		tracker:  p.tracker,
	}
}

// newValidationError creates a new semantic validation error with enhanced context
func (p *Parser) newValidationError(code errors.Code, message string, token *Token, context []string) *ParseError {
	var foundStr string
	var position TokenPosition

	if token != nil {
		foundStr = fmt.Sprintf("%v", token.value)
		position = token.Position
	} else {
		foundStr = "validation context"
		position = TokenPosition{
			Offset: len(p.lexer.input),
			Length: 0,
		}
	}

	return &ParseError{
		Code:     code,
		Message:  message,
		Position: position,
		Token:    token,
		Expected: context,
		Found:    foundStr,
		Category: ValidationError,
		tracker:  p.tracker,
	}
}

// Helper methods for common error patterns

// expectToken creates a syntax error for unexpected tokens
func (p *Parser) expectToken(expected []string, found *Token) *ParseError {
	var message string
	if len(expected) == 1 {
		message = fmt.Sprintf("expected %s", expected[0])
	} else {
		message = fmt.Sprintf("expected one of: %s", strings.Join(expected, ", "))
	}

	return p.newSyntaxError(ErrExpectedKeyword, message, expected, found)
}

// expectKeyword creates a syntax error for missing keywords
func (p *Parser) expectKeyword(keyword string, found *Token) *ParseError {
	message := fmt.Sprintf("expected keyword '%s'", keyword)
	return p.newSyntaxError(ErrExpectedKeyword, message, []string{keyword}, found)
}

// expectIdentifier creates a syntax error for missing identifiers
func (p *Parser) expectIdentifier(found *Token) *ParseError {
	return p.newSyntaxError(ErrExpectedIdentifier, "expected identifier", []string{"identifier"}, found)
}

// expectDataType creates a type error for invalid data types with Iceberg suggestions
func (p *Parser) expectDataType(found *Token) *ParseError {
	// Provide common Iceberg type suggestions
	suggestions := []string{
		"boolean", "int32", "int64", "float32", "float64",
		"decimal", "string", "binary", "date", "time",
		"timestamp", "timestamptz", "uuid",
	}

	message := "expected valid Iceberg data type"
	return p.newTypeError(ErrInvalidIcebergType, message, found, suggestions)
}

// getLegacySQLTypeMapping returns the Iceberg equivalent for legacy SQL types
func (p *Parser) getLegacySQLTypeMapping(legacyType string) string {
	// Map common legacy SQL types to their Iceberg equivalents
	mappings := map[string]string{
		"VARCHAR":   "string",
		"CHAR":      "string",
		"TEXT":      "string",
		"INT":       "int32",
		"INTEGER":   "int32",
		"BIGINT":    "int64",
		"SMALLINT":  "int32",
		"TINYINT":   "int32",
		"FLOAT":     "float32",
		"DOUBLE":    "float64",
		"REAL":      "float32",
		"NUMERIC":   "decimal",
		"DECIMAL":   "decimal",
		"BOOL":      "boolean",
		"DATETIME":  "timestamp",
		"TIMESTAMP": "timestamp",
		"DATE":      "date",
		"TIME":      "time",
		"BLOB":      "binary",
		"BINARY":    "binary",
		"VARBINARY": "binary",
	}

	// Convert to uppercase for case-insensitive matching
	upperType := strings.ToUpper(legacyType)
	return mappings[upperType]
}

// expectLiteral creates a syntax error for missing literals
func (p *Parser) expectLiteral(found *Token) *ParseError {
	return p.newSyntaxError(ErrExpectedLiteral, "expected literal value", []string{"literal"}, found)
}

// expectPunctuation creates a syntax error for missing punctuation
func (p *Parser) expectPunctuation(punctuation string, found *Token) *ParseError {
	var code errors.Code
	switch punctuation {
	case "(":
		code = ErrExpectedLeftParen
	case ")":
		code = ErrExpectedRightParen
	case ",":
		code = ErrExpectedComma
	case ";":
		code = ErrMissingSemicolon
	default:
		code = ErrExpectedKeyword
	}

	message := fmt.Sprintf("expected '%s'", punctuation)
	return p.newSyntaxError(code, message, []string{punctuation}, found)
}

// Error recovery methods for multi-error reporting

// addError adds an error to the collector and determines if parsing should continue
// Returns true if parsing should continue (error recovery), false if it should stop
func (p *Parser) addError(err *ParseError) bool {
	if !p.errorCollector.AddError(err) {
		// Error limit reached, stop parsing
		return false
	}

	p.recoveryMode = true
	p.errorCollector.SetRecovered()
	return true
}

// recoverFromError attempts to recover from a parsing error by finding a safe synchronization point
// Returns true if recovery was successful and parsing can continue
func (p *Parser) recoverFromError(err *ParseError) bool {
	if !p.addError(err) {
		return false
	}

	// Try different recovery strategies based on error type and context
	return p.synchronizeToSafePoint()
}

// synchronizeToSafePoint finds a safe point to resume parsing after an error
// Looks for statement boundaries, keywords, or other synchronization tokens
func (p *Parser) synchronizeToSafePoint() bool {
	// Define synchronization tokens - points where we can safely resume parsing
	syncTokens := map[TokenType]bool{
		SEMICOLON_TOK: true, // End of statement
		EOF_TOK:       true, // End of input
	}

	syncKeywords := map[string]bool{
		"CREATE":   true,
		"DROP":     true,
		"SELECT":   true,
		"INSERT":   true,
		"UPDATE":   true,
		"DELETE":   true,
		"BEGIN":    true,
		"COMMIT":   true,
		"ROLLBACK": true,
		"GRANT":    true,
		"REVOKE":   true,
		"SHOW":     true,
		"ALTER":    true,
		"USE":      true,
	}

	// Skip tokens until we find a synchronization point
	maxSkip := 50 // Prevent infinite loops
	skipped := 0

	for skipped < maxSkip && p.pos < len(p.lexer.tokens) {
		currentToken := p.peek(0)

		// Check for synchronization tokens
		if syncTokens[currentToken.tokenT] {
			if currentToken.tokenT == SEMICOLON_TOK {
				p.consume() // Consume the semicolon
			}
			return true
		}

		// Check for synchronization keywords
		if currentToken.tokenT == KEYWORD_TOK {
			if keyword, ok := currentToken.value.(string); ok {
				if syncKeywords[keyword] {
					return true
				}
			}
		}

		p.consume()
		skipped++
	}

	// If we've reached the end or skipped too many tokens, stop recovery
	return false
}

// tryRecoverFromMissingToken attempts to recover from missing token errors
// Returns true if recovery was successful
func (p *Parser) tryRecoverFromMissingToken(expected string) bool {
	// For certain missing tokens, we can continue parsing
	switch expected {
	case ",":
		// Missing comma in lists - continue parsing the next item
		return true
	case ")":
		// Missing closing parenthesis - try to find the next one
		return p.skipToToken(RPAREN_TOK, 10)
	case "(":
		// Missing opening parenthesis - less recoverable
		return false
	default:
		return false
	}
}

// skipToToken skips tokens until finding the specified token type
// Returns true if the token was found within maxSkip tokens
func (p *Parser) skipToToken(tokenType TokenType, maxSkip int) bool {
	skipped := 0
	for skipped < maxSkip && p.pos < len(p.lexer.tokens) {
		if p.peek(0).tokenT == tokenType {
			return true
		}
		p.consume()
		skipped++
	}
	return false
}

// isRecoverableError determines if an error is recoverable based on its type and context
func (p *Parser) isRecoverableError(err *ParseError) bool {
	// Some errors are more recoverable than others
	switch err.Code {
	case ErrExpectedComma, ErrExpectedRightParen, ErrExpectedLeftParen:
		return true
	case ErrExpectedKeyword:
		// Keyword errors might be recoverable depending on context
		return len(err.Expected) == 1
	case ErrExpectedIdentifier, ErrExpectedLiteral:
		return true
	case ErrMissingSemicolon:
		// Missing semicolon is usually recoverable
		return true
	default:
		return false
	}
}

// handleParsingError processes a parsing error with recovery logic
// Returns the error to return (nil if recovered) and whether to continue parsing
func (p *Parser) handleParsingError(err *ParseError) (error, bool) {
	if !p.isRecoverableError(err) {
		// Non-recoverable error, add to collector and stop
		p.addError(err)
		return p.errorCollector.CreateMultiError(), false
	}

	// Try to recover from the error
	if p.recoverFromError(err) {
		// Recovery successful, continue parsing
		return nil, true
	}

	// Recovery failed, return collected errors
	return p.errorCollector.CreateMultiError(), false
}

// finalizeErrors returns the appropriate error(s) at the end of parsing
func (p *Parser) finalizeErrors() error {
	if !p.errorCollector.HasErrors() {
		return nil
	}

	return p.errorCollector.CreateMultiError()
}

// peekToken returns a pointer to the token at the given offset
func (p *Parser) peekToken(offset int) *Token {
	if p.pos+offset >= len(p.lexer.tokens) {
		eofToken := Token{tokenT: EOF_TOK, Position: TokenPosition{Offset: len(p.lexer.input), Length: 0}}
		return &eofToken
	}
	return &p.lexer.tokens[p.pos+offset]
}

// peek returns the next token
func (p *Parser) peek(i int) Token {
	if p.pos+i >= len(p.lexer.tokens) {
		return Token{tokenT: EOF_TOK, Position: TokenPosition{Offset: len(p.lexer.input), Length: 0}}
	}
	return p.lexer.tokens[p.pos+i]
}

// switch switches one token with another
func (p *Parser) switchToken(i, j int) {
	p.lexer.tokens[p.pos+i], p.lexer.tokens[p.pos+j] = p.lexer.tokens[p.pos+j], p.lexer.tokens[p.pos+i]

}

// rewind goes back one token
func (p *Parser) rewind(i int) {
	p.pos -= i
}

// peekBack returns the previous token
func (p *Parser) peekBack(i int) Token {
	if p.pos-i < len(p.lexer.tokens) || p.pos-i > len(p.lexer.tokens) {
		return Token{tokenT: EOF_TOK, Position: TokenPosition{Offset: len(p.lexer.input), Length: 0}}
	}
	return p.lexer.tokens[p.pos-i]
}

// stripComments removes comments from the token list
func (l *Lexer) stripComments() {
	var newTokens []Token
	for _, tok := range l.tokens {
		if tok.tokenT != COMMENT_TOK {
			newTokens = append(newTokens, tok)
		}
	}
	l.tokens = newTokens

}

// Parse parses the input
func (p *Parser) Parse() (Node, error) {
	p.lexer.tokenize()      // Tokenize the input
	p.lexer.stripComments() // Strip comments

	// Check if statement is empty
	if len(p.lexer.tokens) == 0 {
		return nil, errors.New(ErrEmptyStatement, "empty statement", nil)
	}

	if len(p.lexer.tokens) < 1 {
		return nil, errors.New(ErrInvalidStatement, "invalid statement", nil)
	}

	// Check if statement ends with a semicolon
	if p.lexer.tokens[len(p.lexer.tokens)-1].tokenT != SEMICOLON_TOK {
		return nil, errors.New(ErrMissingSemicolon, "expected ';'", nil)
	}

	// Check if statement starts with a keyword
	if p.peek(0).tokenT == KEYWORD_TOK {
		switch p.peek(0).value {
		case "CREATE":
			return p.parseCreateStmt()
		case "DROP":
			return p.parseDropStmt()
		case "USE":
			return p.parseUseStmt()
		case "INSERT":
			return p.parseInsertStmt()
		case "SELECT":
			return p.parseSelectStmt()
		case "UPDATE":
			return p.parseUpdateStmt()
		case "DELETE":
			return p.parseDeleteStmt()
		case "BEGIN":
			return p.parseBeginStmt()
		case "COMMIT":
			return p.parseCommitStmt()
		case "ROLLBACK":
			return p.parseRollbackStmt()
		case "GRANT":
			return p.parseGrantStmt()
		case "REVOKE":
			return p.parseRevokeStmt()
		case "SHOW":
			return p.parseShowStmt()
		case "ALTER":
			return p.parseAlterStmt()
		case "DECLARE":
			return p.parseDeclareStmt()
		case "OPEN":
			return p.parseOpenStmt()
		case "CLOSE":
			return p.parseCloseStmt()
		case "DEALLOCATE":
			return p.parseDeallocateStmt()
		case "FETCH":
			return p.parseFetchStmt()
		case "WHILE":
			return p.parseWhileStmt()
		case "PRINT":
			return p.parsePrintStmt()
		case "EXEC":
			return p.parseExecStmt()
		case "EXPLAIN":
			return p.parseExplainStmt()

		}
	}

	return nil, errors.New(ErrExpectedKeyword, "expected keyword", nil)

}

// ParseWithRecovery parses the input with error recovery and multi-error reporting
// This is a separate method that can be used when you specifically want error recovery
func (p *Parser) ParseWithRecovery() (Node, error) {
	p.lexer.tokenize()      // Tokenize the input
	p.lexer.stripComments() // Strip comments

	// Check if statement is empty
	if len(p.lexer.tokens) == 0 {
		emptyErr := p.newSyntaxError(ErrEmptyStatement, "empty statement", []string{"SQL statement"}, nil)
		p.addError(emptyErr)
		return nil, p.finalizeErrors()
	}

	if len(p.lexer.tokens) < 1 {
		invalidErr := p.newSyntaxError(ErrInvalidStatement, "invalid statement", []string{"valid SQL"}, nil)
		p.addError(invalidErr)
		return nil, p.finalizeErrors()
	}

	// Check if statement ends with a semicolon
	if p.lexer.tokens[len(p.lexer.tokens)-1].tokenT != SEMICOLON_TOK {
		lastToken := &p.lexer.tokens[len(p.lexer.tokens)-1]
		semicolonErr := p.expectPunctuation(";", lastToken)

		// Missing semicolon is recoverable - try to continue parsing
		if err, shouldContinue := p.handleParsingError(semicolonErr); !shouldContinue {
			return nil, err
		}
	}

	// Parse the main statement with error recovery
	node, err := p.parseMainStatementWithRecovery()

	// If we have parsing errors, return them along with any successfully parsed node
	if p.errorCollector.HasErrors() {
		if err != nil {
			// Add the final error to the collection
			if parseErr, ok := IsParseError(err); ok {
				p.addError(parseErr)
			}
		}
		return node, p.finalizeErrors()
	}

	return node, err
}

// parseMainStatementWithRecovery parses the main SQL statement with error recovery
func (p *Parser) parseMainStatementWithRecovery() (Node, error) {
	// Check if statement starts with a keyword
	if p.peek(0).tokenT != KEYWORD_TOK {
		expectedErr := p.expectKeyword("SQL keyword", p.peekToken(0))
		if err, shouldContinue := p.handleParsingError(expectedErr); !shouldContinue {
			return nil, err
		}

		// Try to recover by finding the next keyword
		if !p.synchronizeToSafePoint() {
			return nil, p.finalizeErrors()
		}
	}

	// Parse based on the keyword
	switch p.peek(0).value {
	case "CREATE":
		return p.parseCreateStmtWithRecovery()
	case "DROP":
		return p.parseDropStmtWithRecovery()
	case "USE":
		return p.parseUseStmtWithRecovery()
	case "INSERT":
		return p.parseInsertStmtWithRecovery()
	case "SELECT":
		return p.parseSelectStmtWithRecovery()
	case "UPDATE":
		return p.parseUpdateStmtWithRecovery()
	case "DELETE":
		return p.parseDeleteStmtWithRecovery()
	case "BEGIN":
		return p.parseBeginStmt()
	case "COMMIT":
		return p.parseCommitStmt()
	case "ROLLBACK":
		return p.parseRollbackStmt()
	case "GRANT":
		return p.parseGrantStmtWithRecovery()
	case "REVOKE":
		return p.parseRevokeStmtWithRecovery()
	case "SHOW":
		return p.parseShowStmtWithRecovery()
	case "ALTER":
		return p.parseAlterStmtWithRecovery()
	case "DECLARE":
		return p.parseDeclareStmtWithRecovery()
	case "OPEN":
		return p.parseOpenStmt()
	case "CLOSE":
		return p.parseCloseStmt()
	case "DEALLOCATE":
		return p.parseDeallocateStmt()
	case "FETCH":
		return p.parseFetchStmt()
	case "WHILE":
		return p.parseWhileStmtWithRecovery()
	case "PRINT":
		return p.parsePrintStmt()
	case "EXEC":
		return p.parseExecStmtWithRecovery()
	case "EXPLAIN":
		return p.parseExplainStmtWithRecovery()
	default:
		unknownErr := p.newSyntaxError(ErrExpectedKeyword,
			fmt.Sprintf("unknown keyword '%v'", p.peek(0).value),
			[]string{"CREATE", "DROP", "SELECT", "INSERT", "UPDATE", "DELETE"},
			p.peekToken(0))
		return nil, unknownErr
	}
}

// Wrapper methods for parsing with error recovery
// These methods wrap the original parsing methods with error recovery logic

// parseCreateStmtWithRecovery wraps parseCreateStmt with error recovery
func (p *Parser) parseCreateStmtWithRecovery() (Node, error) {
	node, err := p.parseCreateStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			// Error was recovered, continue with partial result
			return node, nil
		}
		// Non-ParseError, convert and handle
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid CREATE statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseSelectStmtWithRecovery wraps parseSelectStmt with error recovery
func (p *Parser) parseSelectStmtWithRecovery() (Node, error) {
	node, err := p.parseSelectStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid SELECT statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseInsertStmtWithRecovery wraps parseInsertStmt with error recovery
func (p *Parser) parseInsertStmtWithRecovery() (Node, error) {
	node, err := p.parseInsertStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid INSERT statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseUpdateStmtWithRecovery wraps parseUpdateStmt with error recovery
func (p *Parser) parseUpdateStmtWithRecovery() (Node, error) {
	node, err := p.parseUpdateStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid UPDATE statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseDeleteStmtWithRecovery wraps parseDeleteStmt with error recovery
func (p *Parser) parseDeleteStmtWithRecovery() (Node, error) {
	node, err := p.parseDeleteStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid DELETE statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// Additional recovery wrapper methods for other statement types
// (Similar pattern for DROP, USE, GRANT, REVOKE, SHOW, ALTER, etc.)

// parseDropStmtWithRecovery wraps parseDropStmt with error recovery
func (p *Parser) parseDropStmtWithRecovery() (Node, error) {
	node, err := p.parseDropStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid DROP statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseUseStmtWithRecovery wraps parseUseStmt with error recovery
func (p *Parser) parseUseStmtWithRecovery() (Node, error) {
	node, err := p.parseUseStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid USE statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseGrantStmtWithRecovery wraps parseGrantStmt with error recovery
func (p *Parser) parseGrantStmtWithRecovery() (Node, error) {
	node, err := p.parseGrantStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid GRANT statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseRevokeStmtWithRecovery wraps parseRevokeStmt with error recovery
func (p *Parser) parseRevokeStmtWithRecovery() (Node, error) {
	node, err := p.parseRevokeStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid REVOKE statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseShowStmtWithRecovery wraps parseShowStmt with error recovery
func (p *Parser) parseShowStmtWithRecovery() (Node, error) {
	node, err := p.parseShowStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid SHOW statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseAlterStmtWithRecovery wraps parseAlterStmt with error recovery
func (p *Parser) parseAlterStmtWithRecovery() (Node, error) {
	node, err := p.parseAlterStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid ALTER statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseDeclareStmtWithRecovery wraps parseDeclareStmt with error recovery
func (p *Parser) parseDeclareStmtWithRecovery() (Node, error) {
	node, err := p.parseDeclareStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid DECLARE statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseWhileStmtWithRecovery wraps parseWhileStmt with error recovery
func (p *Parser) parseWhileStmtWithRecovery() (Node, error) {
	node, err := p.parseWhileStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid WHILE statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseExecStmtWithRecovery wraps parseExecStmt with error recovery
func (p *Parser) parseExecStmtWithRecovery() (Node, error) {
	node, err := p.parseExecStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid EXEC statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseExplainStmtWithRecovery wraps parseExplainStmt with error recovery
func (p *Parser) parseExplainStmtWithRecovery() (Node, error) {
	node, err := p.parseExplainStmt()
	if err != nil {
		if parseErr, ok := IsParseError(err); ok {
			if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
				return node, recoveryErr
			}
			return node, nil
		}
		parseErr := p.newSyntaxError(ErrInvalidStatement, err.Error(), []string{"valid EXPLAIN statement"}, p.peekToken(0))
		if recoveryErr, shouldContinue := p.handleParsingError(parseErr); !shouldContinue {
			return node, recoveryErr
		}
	}
	return node, err
}

// parseExplainStmt parses an EXPLAIN statement
func (p *Parser) parseExplainStmt() (Node, error) {
	p.consume() // Consume EXPLAIN

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New(ErrExpectedKeyword, "expected keyword", nil)
	}

	switch p.peek(0).value {
	case "SELECT":
		p.consume() // Consume SELECT

		selectStmt, err := p.parseSelectStmt()
		if err != nil {
			return nil, err
		}

		return &ExplainStmt{
			Stmt: selectStmt,
		}, nil

	case "UPDATE":
		p.consume() // Consume UPDATE

		updateStmt, err := p.parseUpdateStmt()
		if err != nil {
			return nil, err
		}

		return &ExplainStmt{
			Stmt: updateStmt,
		}, nil
	case "DELETE":
		p.consume() // Consume DELETE

		deleteStmt, err := p.parseDeleteStmt()
		if err != nil {
			return nil, err
		}

		return &ExplainStmt{
			Stmt: deleteStmt,
		}, nil
	}

	return nil, errors.New(ErrExpectedSelectInsert, "expected SELECT, DELETE, UPDATE", nil)

}

// parsePrintStmt parses a PRINT statement
func (p *Parser) parsePrintStmt() (Node, error) {
	p.consume() // Consume PRINT

	// PRINT (literal | @variable)
	// i.e. PRINT 'Hello, World!'
	// i.e. PRINT @variable

	// You can print a literal or a @variable
	if p.peek(0).tokenT == AT_TOK {
		variableName := ""
		if p.peek(1).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
		}

		variableName = p.peek(0).value.(string) + p.peek(1).value.(string)
		p.consume()
		p.consume()

		return &PrintStmt{
			Expr: &Identifier{Value: variableName},
		}, nil
	} else {
		// check for literal
		if p.peek(0).tokenT != LITERAL_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
		}

		return &PrintStmt{
			Expr: &Literal{Value: p.peek(0).value},
		}, nil
	}

}

// parseWhileStmt parses a WHILE statement
func (p *Parser) parseWhileStmt() (Node, error) {
	// Look for
	// WHILE @@FETCH_STATUS = 0
	// BEGIN

	p.consume() // Consume WHILE

	if p.peek(0).tokenT != AT_TOK {
		return nil, errors.New(ErrExpectedAtSymbol, "expected @", nil)
	}

	p.consume() // Consume @

	if p.peek(0).tokenT != AT_TOK {
		return nil, errors.New(ErrExpectedAtSymbol, "expected @", nil)
	}

	p.consume() // Consume @

	// Should be FETCH_STATUS
	if p.peek(0).tokenT != IDENT_TOK || p.peek(0).value != "FETCH_STATUS" {
		return nil, errors.New(ErrExpectedFetchStatus, "expected FETCH_STATUS", nil)

	}

	p.consume() // Consume FETCH_STATUS

	// Next tok should be =
	if p.peek(0).tokenT != COMPARISON_TOK || p.peek(0).value != "=" {
		return nil, errors.New(ErrExpectedEquals, "expected =", nil)
	}

	p.consume() // Consume =

	// Next tok should be 0
	if p.peek(0).tokenT != LITERAL_TOK || p.peek(0).value != uint64(0) {
		return nil, errors.New(ErrExpectedZero, "expected 0", nil) // While @@FETCH_STATUS = 0 means while there are rows to fetch
		// There is no benefit in using -1, or -2, or any other number
	}

	fetchStatus := p.peek(0).value.(uint64)

	p.consume() // Consume 0

	// Next tok should be BEGIN

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "BEGIN" {
		return nil, errors.New(ErrExpectedBegin, "expected BEGIN", nil)
	}

	p.consume() // Consume BEGIN

	// Parse statements inside the while loop
	stmts, err := p.parseCursorStmts()
	if err != nil {
		return nil, err
	}

	// Look for END
	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "END" {
		return nil, errors.New(ErrExpectedEnd, "expected END", nil)
	}

	p.consume() // Consume END

	return &WhileStmt{
		FetchStatus: &Literal{Value: fetchStatus},
		Stmts: &BeginEndBlock{
			Stmts: stmts,
		},
	}, nil

}

// parseCursorStmts parses statements inside a cursor
func (p *Parser) parseCursorStmts() ([]interface{}, error) {
	stmts := make([]interface{}, 0)

	for {
		if p.peek(0).tokenT == KEYWORD_TOK && p.peek(0).value == "END" {
			break
		}

		stmt, err := p.Parse()
		if err != nil {
			return nil, err
		}

		stmts = append(stmts, stmt)

		if p.peek(0).tokenT == SEMICOLON_TOK {
			p.consume()
			continue
		}
	}

	return stmts, nil

}

// parseFetchStmt parses a FETCH statement
func (p *Parser) parseFetchStmt() (Node, error) {
	p.consume() // Consume FETCH

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "NEXT" {
		return nil, errors.New(ErrExpectedNext, "expected NEXT", nil)
	}

	p.consume() // Consume NEXT

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "FROM" {
		return nil, errors.New(ErrExpectedFrom, "expected FROM", nil)
	}

	p.consume() // Consume FROM

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	cursorName := p.peek(0).value.(string)
	p.consume() // Consume cursor name

	// check for INTO
	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "INTO" {
		return nil, errors.New(ErrExpectedInto, "expected INTO", nil)
	}

	p.consume() // Consume INTO

	var into []*Identifier

	for {
		// The cursor variable name must start with a @
		if p.peek(0).tokenT != IDENT_TOK && !strings.HasPrefix(p.peek(0).value.(string), "@") {
			return nil, errors.New(ErrExpectedCursorVar, "expected cursor variable name", nil)
		}

		varName := p.peek(0).value.(string)

		if p.peek(1).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
		}

		varName += p.peek(1).value.(string)

		p.consume() // Consume cursor variable name
		p.consume()

		if p.peek(0).tokenT != COMMA_TOK {
			into = append(into, &Identifier{Value: varName})
			break
		} else {
			into = append(into, &Identifier{Value: varName})
			p.consume()
		}

	}

	return &FetchStmt{
		CursorName: &Identifier{Value: cursorName},
		Into:       into,
	}, nil

}

// parseDeallocateStmt parses a DEALLOCATE statement
func (p *Parser) parseDeallocateStmt() (Node, error) {
	p.consume() // Consume DEALLOCATE

	if p.peek(0).tokenT != IDENT_TOK && p.peek(0).value != "@" {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	// if the ident starts with a @
	if strings.HasPrefix(p.peek(0).value.(string), "@") {
		// check next token
		if p.peek(1).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
		}

		variableName := p.peek(0).value.(string) + p.peek(1).value.(string)
		p.consume()
		p.consume()

		return &DeallocateStmt{
			CursorVariableName: &Identifier{Value: variableName},
		}, nil

	} else {

		cursorName := p.peek(0).value.(string)
		p.consume() // Consume cursor name

		return &DeallocateStmt{
			CursorName: &Identifier{Value: cursorName},
		}, nil
	}

}

// parseCloseStmt parses an CLOSE statement
func (p *Parser) parseCloseStmt() (Node, error) {
	p.consume() // Consume OPEN

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	cursorName := p.peek(0).value.(string)
	p.consume() // Consume cursor name

	return &CloseStmt{
		CursorName: &Identifier{Value: cursorName},
	}, nil

}

// parseOpenStmt parses an OPEN statement
func (p *Parser) parseOpenStmt() (Node, error) {
	p.consume() // Consume OPEN

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	cursorName := p.peek(0).value.(string)
	p.consume() // Consume cursor name

	return &OpenStmt{
		CursorName: &Identifier{Value: cursorName},
	}, nil

}

// parseDeclareStmt parses a DECLARE statement
func (p *Parser) parseDeclareStmt() (Node, error) {
	p.consume() // Consume DECLARE

	if p.peek(0).tokenT != IDENT_TOK && p.peek(0).value != "@" {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	// if the ident starts with a @
	if strings.HasPrefix(p.peek(0).value.(string), "@") {
		if p.peek(1).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
		}

		// we know it's a cursor variable not a cursor
		cursorVariableName := p.peek(0).value.(string) + p.peek(1).value.(string)

		// consume the cursor variable name
		p.consume()

		p.consume()

		// Check for datatype
		if p.peek(0).tokenT != DATATYPE_TOK {
			return nil, errors.New(ErrExpectedDataType, "expected datatype", nil)
		}

		// consume the datatype
		cursorVariableDataType := p.peek(0).value.(string)

		// consume the datatype
		p.consume()

		return &DeclareStmt{
			CursorVariableName: &Identifier{Value: cursorVariableName},
			CursorVariableDataType: &Identifier{
				Value: cursorVariableDataType,
			},
		}, nil

	} else {
		// we know it's a cursor
		cursorName := p.peek(0).value.(string)

		// consume the cursor name
		p.consume()

		// look for CURSOR FOR
		if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "CURSOR" {
			return nil, errors.New(ErrExpectedCursor, "expected CURSOR", nil)
		}

		p.consume() // Consume CURSOR

		if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "FOR" {
			return nil, errors.New(ErrExpectedFor, "expected FOR", nil)
		}

		p.consume() // Consume FOR

		// parse select statement
		selectStmt, err := p.parseSelectStmt()
		if err != nil {
			return nil, err
		}

		return &DeclareStmt{
			CursorName: &Identifier{Value: cursorName},
			CursorStmt: selectStmt.(*SelectStmt),
		}, nil

	}

}

// parseAlterStmt parses an ALTER statement
func (p *Parser) parseAlterStmt() (Node, error) {
	p.consume() // Consume ALTER

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New(ErrExpectedKeyword, "expected keyword", nil)
	}

	switch p.peek(0).value {
	case "USER":
		return p.parseAlterUserStmt()
	case "TABLE":
		return p.parseAlterTableStmt()
	}

	return nil, errors.New(ErrExpectedUserOrTable, "expected USER or TABLE", nil)

}

// parseAlterTableStmt
func (p *Parser) parseAlterTableStmt() (Node, error) {
	p.consume() // Consume TABLE

	// Parse table identifier (can be qualified or unqualified)
	tableIdent, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}

	// ALTER COLUMN [identifier] [column_definition]
	// DROP COLUMN [identifier]

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New(ErrExpectedKeyword, "expected keyword", nil)
	}

	switch p.peek(0).value {
	case "DROP":
		p.consume() // Consume DROP

		if p.peek(0).tokenT != KEYWORD_TOK {
			return nil, errors.New(ErrExpectedKeyword, "expected keyword", nil)
		}

		p.consume() // Consume COLUMN

		if p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
		}

		columnName := p.peek(0).value.(string)

		p.consume()

		return &AlterTableStmt{
			TableName:  tableIdent,
			ColumnName: &Identifier{Value: columnName},
			Action:     AlterTableActionDropColumn,
		}, nil

	case "SETTINGS":
		p.consume() // Consume SETTINGS

		// Parse settings as key=value pairs
		settings := make(map[string]interface{})

		// First token after SETTINGS must be an identifier (key)
		if p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedIdentifier, "expected identifier after SETTINGS", nil)
		}

		for {
			// Check for end of statement
			if p.peek(0).tokenT == SEMICOLON_TOK || p.peek(0).tokenT == EOF_TOK {
				break
			}

			// Parse key=value pair
			if p.peek(0).tokenT != IDENT_TOK {
				break
			}

			key := p.peek(0).value.(string)
			p.consume() // Consume key

			if p.peek(0).tokenT != COMPARISON_TOK || p.peek(0).value != "=" {
				return nil, errors.New(ErrExpectedEquals, "expected = after setting key", nil)
			}
			p.consume() // Consume =

			// Parse value (can be literal or identifier)
			var value interface{}
			if p.peek(0).tokenT == LITERAL_TOK {
				rawValue := p.peek(0).value
				// Convert uint64 to int for consistency
				if uint64Val, ok := rawValue.(uint64); ok {
					value = int(uint64Val)
				} else {
					value = rawValue
				}
			} else if p.peek(0).tokenT == IDENT_TOK {
				valueStr := p.peek(0).value.(string)
				// Try to convert to appropriate type
				if parsedValue, err := p.parseSettingValue(valueStr); err == nil {
					value = parsedValue
				} else {
					value = valueStr
				}
			} else {
				return nil, errors.New(ErrExpectedLiteral, "expected value after =", nil)
			}
			p.consume() // Consume value

			settings[key] = value

			// Check for comma separator
			if p.peek(0).tokenT == COMMA_TOK {
				p.consume() // Consume comma
			} else {
				break
			}
		}

		return &AlterTableStmt{
			TableName: tableIdent,
			Action:    AlterTableActionSettings,
			Settings:  settings,
		}, nil

	case "ALTER":
		p.consume() // Consume ALTER

		if p.peek(0).tokenT != KEYWORD_TOK {
			return nil, errors.New(ErrExpectedKeyword, "expected keyword", nil)
		}

		p.consume() // Consume COLUMN

		dummyCreateTblStatement := &CreateTableStmt{
			TableSchema: &TableSchema{
				ColumnDefinitions: make(map[string]*ColumnDefinition),
			},
		}

		if p.peek(0).tokenT != IDENT_TOK {

			err := p.parseTableConstraints(dummyCreateTblStatement, "")
			if err != nil {
				return nil, err
			}
			break
		}

		columnName := p.peek(0).value.(string)

		p.consume() // Consume column name

		if p.peek(0).tokenT != DATATYPE_TOK {

			return nil, errors.New(ErrExpectedDataType, "expected data type", nil)
		}

		dataType := p.peek(0).value.(string)

		dummyCreateTblStatement.TableSchema.ColumnDefinitions[columnName] = &ColumnDefinition{
			DataType: dataType,
		}

		p.consume() // Consume data type

		// check for DATATYPE(LEN) or DATATYPE(PRECISION, SCALE)
		if p.peek(0).tokenT == LPAREN_TOK {
			switch dataType {

			case "CHAR", "CHARACTER", "BINARY", "VARCHAR":
				p.consume() // Consume (

				if p.peek(0).tokenT != LITERAL_TOK {
					return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
				}

				length := p.peek(0).value.(uint64)

				p.consume() // Consume literal

				if p.peek(0).tokenT != RPAREN_TOK {
					return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
				}

				p.consume() // Consume )

				dummyCreateTblStatement.TableSchema.ColumnDefinitions[columnName].Length = int(length)
			case "DEC", "DECIMAL", "NUMERIC", "REAL", "FLOAT", "DOUBLE":

				p.consume() // Consume (

				if p.peek(0).tokenT != LITERAL_TOK {
					return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
				}

				precision := p.peek(0).value.(uint64)

				p.consume() // Consume literal

				if p.peek(0).tokenT != COMMA_TOK {
					return nil, errors.New(ErrExpectedComma, "expected ,", nil)
				}

				p.consume() // Consume ,

				if p.peek(0).tokenT != LITERAL_TOK {
					return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
				}

				scale := p.peek(0).value.(uint64)

				p.consume() // Consume literal

				if p.peek(0).tokenT != RPAREN_TOK {
					return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
				}

				p.consume() // Consume )

				dummyCreateTblStatement.TableSchema.ColumnDefinitions[columnName].Precision = int(precision)
				dummyCreateTblStatement.TableSchema.ColumnDefinitions[columnName].Scale = int(scale)

			}
		}

		err := p.parseTableConstraints(dummyCreateTblStatement, columnName)
		if err != nil {
			return nil, err

		}

		return &AlterTableStmt{
			TableName:        tableIdent,
			ColumnName:       &Identifier{Value: columnName},
			ColumnDefinition: dummyCreateTblStatement.TableSchema.ColumnDefinitions[columnName],
			Action:           AlterTableActionAlterColumn,
		}, nil

	}

	return nil, errors.New(ErrExpectedAddDropSet, "expected ADD, DROP, SET, RENAME, or MODIFY", nil)

}

// parseAlterUserStmt parses an ALTER USER statement
func (p *Parser) parseAlterUserStmt() (Node, error) {
	alterUserStmt := &AlterUserStmt{}
	p.consume() // Consume USER

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	alterUserStmt.Username = &Identifier{Value: p.peek(0).value.(string)}
	p.consume() // Consume username

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New(ErrExpectedKeyword, "expected keyword", nil)
	}

	switch p.peek(0).value {
	case "SET":
		p.consume() // Consume SET
		switch p.peek(0).value {
		case "PASSWORD":
			alterUserStmt.SetType = ALTER_USER_SET_PASSWORD
		case "USERNAME":
			alterUserStmt.SetType = ALTER_USER_SET_USERNAME
		default:
			return nil, errors.New(ErrExpectedPasswordUser, "expected PASSWORD or USERNAME", nil)

		}
	default:
		return nil, errors.New(ErrExpectedSet, "expected SET", nil)
	}

	p.consume() // Consume PASSWORD or USERNAME

	if p.peek(0).tokenT != LITERAL_TOK {

		return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
	}

	alterUserStmt.Value = &Literal{Value: strings.TrimSuffix(strings.TrimPrefix(p.peek(0).value.(string), "'"), "'")}

	return alterUserStmt, nil
}

// parseShowStmt parses a SHOW statement
func (p *Parser) parseShowStmt() (Node, error) {
	p.consume() // Consume SHOW

	switch strings.ToUpper(p.peek(0).value.(string)) {
	case "DATABASES":
		return &ShowStmt{ShowType: SHOW_DATABASES}, nil
	case "TABLES":
		p.consume() // Consume TABLES

		// Check if there's a FROM clause
		if p.peek(0).tokenT == KEYWORD_TOK && p.peek(0).value == "FROM" {
			p.consume() // Consume FROM

			if p.peek(0).tokenT != IDENT_TOK {
				return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
			}

			databaseName := p.peek(0).value.(string)
			p.consume() // Consume database name

			return &ShowStmt{ShowType: SHOW_TABLES, From: &Identifier{Value: databaseName}}, nil
		}

		return &ShowStmt{ShowType: SHOW_TABLES}, nil
	case "USERS":
		return &ShowStmt{ShowType: SHOW_USERS}, nil
	case "INDEXES":
		p.consume() // Consume INDEXES

		if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "FROM" {
			return nil, errors.New(ErrExpectedFrom, "expected FROM", nil)
		}

		p.consume() // Consume FROM

		if p.peek(0).tokenT != IDENT_TOK {

			return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
		}

		tableName := p.peek(0).value.(string)

		return &ShowStmt{ShowType: SHOW_INDEXES, From: &Identifier{
			Value: tableName,
		}}, nil
	case "COLUMNS":
		p.consume() // Consume COLUMNS

		if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "FROM" {
			return nil, errors.New(ErrExpectedFrom, "expected FROM", nil)
		}

		p.consume() // Consume FROM

		// Parse table identifier (can be qualified database.table or just table)
		tableIdent, err := p.parseTableIdentifier()
		if err != nil {
			return nil, err
		}

		return &ShowStmt{ShowType: SHOW_COLUMNS, TableName: tableIdent}, nil
	case "CREATE":
		p.consume() // Consume CREATE

		if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "TABLE" {
			return nil, errors.New(ErrExpectedKeyword, "expected TABLE", nil)
		}

		p.consume() // Consume TABLE

		// Parse table identifier (can be qualified database.table or just table)
		tableIdent, err := p.parseTableIdentifier()
		if err != nil {
			return nil, err
		}

		return &ShowStmt{ShowType: SHOW_CREATE_TABLE, TableName: tableIdent}, nil
	case "GRANTS":

		p.consume() // Consume GRANTS

		if p.peek(0).tokenT == KEYWORD_TOK && p.peek(0).value == "FOR" {
			p.consume() // Consume ON

			if p.peek(0).tokenT != IDENT_TOK {
				return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
			}

			tableName := p.peek(0).value.(string)
			p.consume() // Consume username

			return &ShowStmt{ShowType: SHOW_GRANTS, For: &Identifier{Value: tableName}}, nil
		}

		return &ShowStmt{ShowType: SHOW_GRANTS}, nil
	}

	return nil, errors.New(ErrExpectedDatabases, "expected DATABASES, TABLES, USERS, COLUMNS, or CREATE TABLE", nil)

}

// parseRevokeStmt parses a REVOKE statement
func (p *Parser) parseRevokeStmt() (Node, error) {
	p.consume() // Consume REVOKE

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New(ErrExpectedKeyword, "expected keyword", nil)
	}

	switch p.peek(0).value {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "ALL", "DROP", "CREATE", "CONNECT", "ALTER":
		return p.parsePrivilegeStmt(true)
	}

	return nil, errors.New(ErrExpectedSelectInsert, "expected SELECT, INSERT, UPDATE, DELETE", nil)

}

// parseGrantStmt parses a GRANT statement
func (p *Parser) parseGrantStmt() (Node, error) {

	p.consume() // Consume GRANT

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New(ErrExpectedKeyword, "expected keyword", nil)
	}

	switch p.peek(0).value {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "ALL", "DROP", "CREATE", "CONNECT", "ALTER":
		return p.parsePrivilegeStmt(false)
	}

	return nil, errors.New(ErrExpectedSelectInsert, "expected SELECT, INSERT, UPDATE, DELETE", nil)

}

// parsePrivilegeStmt parses a privilege statement
func (p *Parser) parsePrivilegeStmt(revoke bool) (Node, error) {
	//  GRANT SELECT, INSERT, UPDATE, DELETE ON database.table TO user;

	grantStmt := &GrantStmt{}
	revokeStmt := &RevokeStmt{}

	privilegeDefinition := &PrivilegeDefinition{
		Actions: make([]PrivilegeAction, 0),
	}

	all := false

	for {
		switch p.peek(0).value {
		case "ALL":
			privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_ALL)
			all = true
		case "SELECT":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_SELECT)
			}
		case "BEGIN":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_BEGIN)
			}
		case "COMMIT":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_COMMIT)
			}
		case "ROLLBACK":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_ROLLBACK)
			}
		case "INSERT":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_INSERT)
			}
		case "UPDATE":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_UPDATE)
			}
		case "DELETE":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_DELETE)
			}
		case "DROP":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_DROP)
			}
		case "CREATE":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_CREATE)
			}
		case "CONNECT":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_CONNECT)
			}
		case "ALTER":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_ALTER)
			}
		case "REVOKE":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_REVOKE)
			}
		case "GRANT":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_GRANT)
			}
		case "SHOW":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_SHOW)
			}
		case "EXEC":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_EXEC)
			}
		case "EXPLAIN":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_EXPLAIN)
			}
		case "PRINT":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_PRINT)
			}
		case "DECLARE":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_DECLARE)
			}
		case "OPEN":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_OPEN)
			}
		case "CLOSE":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_CLOSE)
			}
		case "DEALLOCATE":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_DEALLOCATE)
			}
		case "FETCH":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_FETCH)
			}
		case "WHILE":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_WHILE)
			}
		case "RETURN":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_RETURN)
			}
		case "BREAK":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_BREAK)
			}
		case "SET":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_SET)
			}
		case "EXIT":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, PRIV_EXIT)
			}
		default:
			return nil, errors.New(ErrExpectedPrivilege, "expected privilege", nil)
		}

		p.consume()

		if p.peek(0).tokenT == COMMA_TOK {
			p.consume()
			continue
		} else {
			break
		}

	}

	if p.peek(0).value != "TO" {

		if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "ON" {
			return nil, errors.New(ErrExpectedOn, "expected ON", nil)
		}

		p.consume() // Consume ON

		if p.peek(0).tokenT != IDENT_TOK {
			if p.peek(0).value != "*" {
				return nil, errors.New(ErrExpectedStarOrTable, "expected identifier or * ", nil)
			}
		}

		db := &Identifier{}
		table := &Identifier{}

		// if asterisk is found, set database to *
		if p.peek(0).value == "*" {
			db = &Identifier{Value: "*"}
			p.consume()

			// check if next value is * or identifier
			if p.peek(0).value == ".*" {
				table = &Identifier{Value: "*"}
			}
			p.consume() // Consume table name
			privilegeDefinition.Object = &Identifier{Value: db.Value + "." + table.Value}

		} else {
			// Check if this is a database name followed by .*
			if p.peek(0).tokenT == IDENT_TOK {
				dbName := p.peek(0).value.(string)
				p.consume() // Consume database name

				// Check if next token is . followed by *
				if p.peek(0).tokenT == DOT_TOK {
					p.consume() // Consume .

					if p.peek(0).tokenT == ASTERISK_TOK {
						// This is database.* format
						db = &Identifier{Value: dbName}
						table = &Identifier{Value: "*"}
						privilegeDefinition.Object = &Identifier{Value: db.Value + "." + table.Value}
						p.consume() // Consume *
					} else if p.peek(0).tokenT == IDENT_TOK {
						// This is database.table format
						tableName := p.peek(0).value.(string)
						db = &Identifier{Value: dbName}
						table = &Identifier{Value: tableName}
						privilegeDefinition.Object = &Identifier{Value: db.Value + "." + table.Value}
						p.consume() // Consume table name
					} else {
						return nil, errors.New(ErrExpectedStarOrTableAfterDot, "expected * or table name after .", nil)
					}
				} else {
					return nil, errors.New(ErrExpectedDotAfterDB, "expected . after database name", nil)
				}
			} else {
				return nil, errors.New(ErrExpectedDatabaseOrStar, "expected database name or *", nil)
			}
		}
	}

	p.consume() // Consume TO

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	user := p.peek(0).value.(string)

	if revoke {
		privilegeDefinition.Revokee = &Identifier{Value: user}
		revokeStmt.PrivilegeDefinition = privilegeDefinition

		return revokeStmt, nil

	}
	privilegeDefinition.Grantee = &Identifier{Value: user}
	grantStmt.PrivilegeDefinition = privilegeDefinition

	return grantStmt, nil

}

// parseBeginStmt parses a BEGIN statement
func (p *Parser) parseBeginStmt() (Node, error) {
	p.consume() // Consume BEGIN
	return &BeginStmt{}, nil
}

// parseCommitStmt parses a COMMIT statement
func (p *Parser) parseCommitStmt() (Node, error) {
	p.consume() // Consume COMMIT
	return &CommitStmt{}, nil
}

// parseRollbackStmt parses a ROLLBACK statement
func (p *Parser) parseRollbackStmt() (Node, error) {
	p.consume() // Consume ROLLBACK
	return &RollbackStmt{}, nil

}

// parseDeleteStmt parses a DELETE statement
func (p *Parser) parseDeleteStmt() (Node, error) {
	p.consume() // Consume DELETE

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "FROM" {
		return nil, p.expectKeyword("FROM", p.peekToken(0))
	}

	p.consume() // Consume FROM

	// Parse table identifier (can be qualified or unqualified)
	tableIdent, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}

	deleteStmt := &DeleteStmt{
		TableName: tableIdent,
	}

	if p.peek(0).tokenT == KEYWORD_TOK && p.peek(0).value == "WHERE" {
		whereClause, err := p.parseWhereClause()
		if err != nil {
			return nil, err
		}

		deleteStmt.WhereClause = whereClause
	}

	return deleteStmt, nil

}

// parseUpdateStmt parses an UPDATE statement
func (p *Parser) parseUpdateStmt() (Node, error) {
	p.consume() // Consume UPDATE

	// Parse table identifier (can be qualified or unqualified)
	tableIdent, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "SET" {
		return nil, p.expectKeyword("SET", p.peekToken(0))
	}

	p.consume() // Consume SET

	updateStmt := &UpdateStmt{
		TableName: tableIdent,
		SetClause: make([]*SetClause, 0),
	}

	for p.peek(0).value != "WHERE" {

		if p.peek(0).tokenT != IDENT_TOK {
			return nil, p.expectIdentifier(p.peekToken(0))
		}

		columnName := p.peek(0).value.(string)
		p.consume() // Consume column name

		if p.peek(0).tokenT != COMPARISON_TOK || p.peek(0).value != "=" {
			return nil, p.expectPunctuation("=", p.peekToken(0))
		}

		p.consume() // Consume =

		var literal interface{}

		if p.peek(0).tokenT != LITERAL_TOK {
			if p.peek(0).value == "SYS_DATE" {
				literal = &SysDate{}
			} else if p.peek(0).value == "SYS_TIME" {
				literal = &SysTime{}
			} else if p.peek(0).value == "GENERATE_UUID" {
				literal = &GenUUID{}
			} else if p.peek(1).tokenT == PLUS_TOK || p.peek(1).tokenT == MINUS_TOK || p.peek(1).tokenT == ASTERISK_TOK || p.peek(1).tokenT == DIVIDE_TOK {
				// binary expression
				binaryExpr, err := p.parseBinaryExpr(0)
				if err != nil {
					return nil, err
				}

				literal = binaryExpr

				p.rewind(1)
			} else if p.peek(0).tokenT == IDENT_TOK {
				literal = &Identifier{Value: p.peek(0).value.(string)}
			}
		} else if p.peek(0).tokenT == LITERAL_TOK {
			literal = p.peek(0).value
		}

		setClause := &SetClause{
			Column: &Identifier{Value: columnName},
			Value:  &Literal{Value: literal},
		}

		updateStmt.SetClause = append(updateStmt.SetClause, setClause)

		p.consume()

		if p.peek(0).tokenT == SEMICOLON_TOK {
			break
		} else if p.peek(0).tokenT != COMMA_TOK {
			break
		}

	}

	// Parse where
	if p.peek(0).tokenT == KEYWORD_TOK || p.peek(0).value == "WHERE" {
		whereClause, err := p.parseWhereClause()
		if err != nil {
			return nil, err
		}

		updateStmt.WhereClause = whereClause
	}

	return updateStmt, nil

}

// parseDropStmt parses a DROP statement
func (p *Parser) parseDropStmt() (Node, error) {
	p.consume() // Consume DROP

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New(ErrExpectedKeyword, "expected keyword", nil)
	}

	switch p.peek(0).value {
	case "DATABASE":
		return p.parseDropDatabaseStmt()
	case "TABLE":
		return p.parseDropTableStmt()
	case "INDEX":
		return p.parseDropIndexStmt()
	case "USER":
		return p.parseDropUserStmt()
	case "PROCEDURE":
		return p.parseDropProcedureStmt()
	}

	return nil, errors.New(ErrExpectedDatabaseOrTable, "expected DATABASE or TABLE", nil)

}

// parseDropProcedureStmt parses a DROP PROCEDURE statement
func (p *Parser) parseDropProcedureStmt() (Node, error) {
	p.consume() // Consume PROCEDURE

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	procedureName := p.peek(0).value.(string)
	p.consume() // Consume procedure name

	return &DropProcedureStmt{
		ProcedureName: &Identifier{Value: procedureName},
	}, nil

}

// parseDropUserStmt parses a DROP USER statement
func (p *Parser) parseDropUserStmt() (Node, error) {
	p.consume() // Consume USER

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	user := p.peek(0).value.(string)
	p.consume() // Consume user

	return &DropUserStmt{
		Username: &Identifier{Value: user},
	}, nil

}

// parseTableIdentifier parses a table identifier that can be qualified (database.table) or unqualified (table)
func (p *Parser) parseTableIdentifier() (*TableIdentifier, error) {
	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	databaseName := p.peek(0).value.(string)
	p.consume() // Consume first identifier

	// Check if next token is a dot (qualified name)
	if p.peek(0).tokenT == DOT_TOK {
		p.consume() // Consume dot

		if p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedIdentifierAfterDot, "expected identifier after dot", nil)
		}

		tableName := p.peek(0).value.(string)
		p.consume() // Consume second identifier

		// Validate the identifiers
		if databaseName == "" {
			return nil, errors.New(ErrDatabaseNameEmpty, "database name cannot be empty", nil)
		}
		if tableName == "" {
			return nil, errors.New(ErrTableNameEmpty, "table name cannot be empty", nil)
		}

		return &TableIdentifier{
			Database: &Identifier{Value: databaseName},
			Table:    &Identifier{Value: tableName},
		}, nil
	} else {
		// Simple table name (unqualified)
		if databaseName == "" {
			return nil, errors.New(ErrTableNameEmpty, "table name cannot be empty", nil)
		}

		return &TableIdentifier{
			Database: nil, // No database specified
			Table:    &Identifier{Value: databaseName},
		}, nil
	}
}

// parseDropTableStmt parses a DROP TABLE statement
func (p *Parser) parseDropTableStmt() (Node, error) {
	p.consume() // Consume TABLE

	// Check for IF EXISTS clause
	ifExists := false
	if p.peek(0).tokenT == KEYWORD_TOK && p.peek(0).value == "IF" {
		p.consume() // Consume IF

		if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "EXISTS" {
			return nil, errors.New(ErrExpectedExistsAfterIf, "expected EXISTS after IF", nil)
		}
		p.consume() // Consume EXISTS
		ifExists = true
	}

	// Parse table identifier (can be qualified or unqualified)
	tableIdent, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}

	return &DropTableStmt{
		TableName: tableIdent,
		IfExists:  ifExists,
	}, nil
}

// parseDropIndexStmt parses a DROP INDEX statement
func (p *Parser) parseDropIndexStmt() (Node, error) {
	p.consume() // Consume INDEX

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	indexName := p.peek(0).value.(string)
	p.consume() // Consume identifier

	if p.peek(0).value != "ON" {
		return nil, errors.New(ErrExpectedOn, "expected ON", nil)
	}

	p.consume() // Consume ON

	// Parse table identifier (can be qualified or unqualified)
	tableIdent, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}

	return &DropIndexStmt{
		TableName: tableIdent,
		IndexName: &Identifier{Value: indexName},
	}, nil

}

// parseDropDatabaseStmt parses a DROP DATABASE statement
func (p *Parser) parseDropDatabaseStmt() (Node, error) {
	p.consume() // Consume DATABASE

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	name := p.peek(0).value.(string)
	p.consume() // Consume identifier

	return &DropDatabaseStmt{
		Name: &Identifier{Value: name},
	}, nil
}

// parseInsertStmt parses an INSERT statement
func (p *Parser) parseInsertStmt() (Node, error) {
	// INSERT INTO schema_name.table_name (column_name1, column_name2, ...) VALUES (value1, value2, ...), (value1, value2, ...), ...

	insertStmt := &InsertStmt{}

	// Eat INSERT
	p.consume()

	if p.peek(0).value != "INTO" {
		return nil, p.expectKeyword("INTO", p.peekToken(0))
	}

	// Eat INTO
	p.consume()

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, p.expectIdentifier(p.peekToken(0))
	}

	// Parse table identifier (can be qualified or unqualified)
	tableIdent, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}
	insertStmt.TableName = tableIdent

	insertStmt.ColumnNames = make([]*Identifier, 0)
	insertStmt.Values = make([][]interface{}, 0)

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
	}

	p.consume() // Consume (
	for {
		if p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
		}

		columnName := p.peek(0).value.(string)
		insertStmt.ColumnNames = append(insertStmt.ColumnNames, &Identifier{Value: columnName})

		p.consume() // Consume column name

		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT != COMMA_TOK {
			return nil, errors.New(ErrExpectedComma, "expected ,", nil)
		}

		p.consume() // Consume ,

	}

	if p.peek(0).tokenT != RPAREN_TOK {
		return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
	}

	p.consume() // Consume )

	// Look for VALUES

	if p.peek(0).value != "VALUES" {
		return nil, p.expectKeyword("VALUES", p.peekToken(0))
	}

	p.consume() // Consume VALUES

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
	}

	for {
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
		}

		p.consume() // Consume (

		values := make([]interface{}, 0)

		for {
			if p.peek(0).tokenT == RPAREN_TOK {
				break
			}

			if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).value != "NULL" && p.peek(0).value != "SYS_DATE" && p.peek(0).value != "SYS_TIME" && p.peek(0).value != "SYS_TIMESTAMP" && p.peek(0).value != "GENERATE_UUID" {

				return nil, p.newSyntaxError(ErrExpectedLiteralOrNull, "expected literal, NULL, or system function",
					[]string{"literal", "NULL", "SYS_DATE", "SYS_TIME", "SYS_TIMESTAMP", "GENERATE_UUID"}, p.peekToken(0))

			}

			if p.peek(0).value == "NULL" {
				values = append(values, &Literal{Value: nil})
			} else if p.peek(0).value == "SYS_DATE" {
				values = append(values, &SysDate{})
			} else {
				values = append(values, &Literal{Value: p.peek(0).value})
			}

			p.consume() // Consume literal

			if p.peek(0).tokenT == RPAREN_TOK {
				break
			}

			if p.peek(0).tokenT != COMMA_TOK {
				return nil, errors.New(ErrExpectedComma, "expected ,", nil)
			}

			p.consume() // Consume ,
		}

		insertStmt.Values = append(insertStmt.Values, values)

		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
		}

		p.consume() // Consume )

		if p.peek(0).tokenT == SEMICOLON_TOK {
			break
		}

		if p.peek(0).tokenT != COMMA_TOK {
			return nil, errors.New(ErrExpectedComma, "expected ,", nil)
		}

		p.consume() // Consume ,
	}

	return insertStmt, nil
}

// parseCreateStmt parses a CREATE statement
func (p *Parser) parseCreateStmt() (Node, error) {
	p.consume() // Consume CREATE

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, p.expectToken([]string{"DATABASE", "TABLE", "INDEX", "USER", "PROCEDURE"}, p.peekToken(0))
	}

	switch strings.ToUpper(p.peek(0).value.(string)) {
	case "DATABASE":
		return p.parseCreateDatabaseStmt()
	case "INDEX", "UNIQUE":
		if p.peek(1).value == "INDEX" {
			// eat unique
			p.consume()

			ast, err := p.parseCreateIndexStmt()
			if err != nil {
				return nil, err
			}

			ast.(*CreateIndexStmt).Unique = true
			return ast, nil
		}
		return p.parseCreateIndexStmt()
	case "TABLE":
		return p.parseCreateTableStmt()
	case "USER":
		return p.parseCreateUserStmt()
	case "PROCEDURE":
		return p.parseCreateProcedureStmt()
	}

	return nil, p.expectToken([]string{"DATABASE", "TABLE", "INDEX", "USER", "PROCEDURE"}, p.peekToken(0))

}

// parseExecStmt parses an EXEC statement
func (p *Parser) parseExecStmt() (Node, error) {
	p.consume() // Consume EXEC

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	procedureName := p.peek(0).value.(string)
	p.consume() // Consume procedure name

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
	}

	p.consume() // Consume (

	var args []interface{}

	for {
		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).value != "NULL" {
			return nil, errors.New(ErrExpectedLiteralOrNull, "expected literal or NULL", nil)
		}

		if p.peek(0).value == "NULL" {
			args = append(args, &Literal{Value: nil})
		} else {
			args = append(args, &Literal{Value: p.peek(0).value})
		}

		p.consume() // Consume literal

		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT != COMMA_TOK {
			return nil, errors.New(ErrExpectedComma, "expected ,", nil)
		}

		p.consume() // Consume ,
	}

	p.consume() // Consume )

	return &ExecStmt{
		ProcedureName: &Identifier{Value: procedureName},
		Args:          args,
	}, nil

}

// parseCreateProcedureStmt parses a CREATE PROCEDURE statement
func (p *Parser) parseCreateProcedureStmt() (Node, error) {
	p.consume() // Consume PROCEDURE

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	name := p.peek(0).value.(string)

	p.consume() // Consume name

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
	}

	p.consume() // Consume (

	var params []*Parameter

	for {
		// @var1 INT, @var2 VARCHAR(255)
		// if == )
		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		// Check for @
		if p.peek(0).value != "@" {

			return nil, errors.New(ErrExpectedAtSymbol, "expected @", nil)
		}

		p.consume() // Consume @

		if p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
		}

		paramName := fmt.Sprintf("@%s", p.peek(0).value.(string))

		p.consume() // Consume name

		if p.peek(0).tokenT != DATATYPE_TOK {
			return nil, errors.New(ErrExpectedDataType, "expected datatype", nil)
		}

		dataType := p.peek(0).value.(string)

		p.consume() // Consume data type

		// Check for DATATYPE(LEN) or DATATYPE(PRECISION, SCALE)
		if p.peek(0).tokenT == LPAREN_TOK {
			switch dataType {
			case "CHAR", "CHARACTER", "BINARY", "VARCHAR":
				p.consume() // Consume (

				if p.peek(0).tokenT != LITERAL_TOK {
					return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
				}

				length := p.peek(0).value.(uint64)

				p.consume() // Consume literal

				if p.peek(0).tokenT != RPAREN_TOK {
					return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
				}

				p.consume() // Consume )

				params = append(params, &Parameter{
					Name:     &Identifier{Value: paramName},
					DataType: &Identifier{Value: dataType},
					Length:   &Literal{Value: length},
				})
			case "DEC", "DECIMAL", "NUMERIC", "REAL", "FLOAT", "DOUBLE":

				p.consume() // Consume (

				if p.peek(0).tokenT != LITERAL_TOK {
					return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
				}

				precision := p.peek(0).value.(uint64)

				p.consume() // Consume literal

				if p.peek(0).tokenT != COMMA_TOK {
					return nil, errors.New(ErrExpectedComma, "expected ,", nil)
				}

				p.consume() // Consume ,

				if p.peek(0).tokenT != LITERAL_TOK {
					return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
				}

				scale := p.peek(0).value.(uint64)

				p.consume() // Consume literal

				if p.peek(0).tokenT != RPAREN_TOK {
					return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
				}

				p.consume() // Consume )

				params = append(params, &Parameter{
					Name:      &Identifier{Value: paramName},
					DataType:  &Identifier{Value: dataType},
					Precision: &Literal{Value: precision},
					Scale:     &Literal{Value: scale},
				})
			}
		} else {
			params = append(params, &Parameter{
				Name:     &Identifier{Value: paramName},
				DataType: &Identifier{Value: dataType},
			})
		}

		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT != COMMA_TOK {
			return nil, errors.New(ErrExpectedComma, "expected ,", nil)
		}

		p.consume() // Consume ,

	}

	if p.peek(0).tokenT != RPAREN_TOK {
		return nil, errors.New(ErrExpectedRightParen, "expected )", nil)

	}

	p.consume() // Consume )

	var block []interface{}

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "BEGIN" {
		return nil, errors.New(ErrExpectedBegin, "expected BEGIN", nil)
	}

	p.consume() // Consume BEGIN

	// Parse statements
	for p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "END" {
		stmt, err := p.Parse()
		if err != nil {
			return nil, err
		}

		// Add statement to create procedure statement

		block = append(block, stmt)

		p.consume() // Consume ;
	}

	p.consume() // Consume END

	return &CreateProcedureStmt{
		Procedure: &Procedure{
			Name:       &Identifier{Value: name},
			Parameters: params,
			Body: &BeginEndBlock{
				Stmts: block,
			},
		},
	}, nil

}

// parseCreateUserStmt
func (p *Parser) parseCreateUserStmt() (Node, error) {
	createUserStmt := &CreateUserStmt{}

	// Eat USER
	p.consume()

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	username := p.peek(0).value.(string)
	createUserStmt.Username = &Identifier{Value: username}

	p.consume() // Consume username

	// Eat IDENTIFIED
	if p.peek(0).value != "IDENTIFIED" {
		return nil, errors.New(ErrExpectedKeyword, "expected IDENTIFIED", nil)
	}

	p.consume() // Consume IDENTIFIED

	// Eat BY
	if p.peek(0).value != "BY" {
		return nil, errors.New(ErrExpectedKeyword, "expected BY", nil)
	}

	p.consume() // Consume BY

	if p.peek(0).tokenT != LITERAL_TOK {
		return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
	}

	password := p.peek(0).value.(string)
	createUserStmt.Password = &Literal{Value: strings.TrimSuffix(strings.TrimPrefix(password, "'"), "'")}

	p.consume() // Consume password

	return createUserStmt, nil

}

// parseCreateTableStmt parses a CREATE TABLE statement
func (p *Parser) parseCreateTableStmt() (Node, error) {
	// CREATE TABLE [IF NOT EXISTS] schema_name.table_name (column_name1 data_type constraints, column_name2 data_type constraints, ...)
	createTableStmt := &CreateTableStmt{}

	// Eat TABLE
	p.consume()

	// Check for IF NOT EXISTS clause BEFORE the table identifier
	ifNotExists := false
	if p.peek(0).tokenT == KEYWORD_TOK && p.peek(0).value == "IF NOT EXISTS" {
		p.consume() // Consume IF NOT EXISTS
		ifNotExists = true
	}

	createTableStmt.IfNotExists = ifNotExists

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, p.expectIdentifier(p.peekToken(0))
	}

	// Parse table identifier (can be qualified or unqualified)
	tableIdent, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}
	createTableStmt.TableName = tableIdent

	createTableStmt.TableSchema = &TableSchema{
		ColumnDefinitions: make(map[string]*ColumnDefinition),
	}

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, p.expectPunctuation("(", p.peekToken(0))
	}

	p.consume() // Consume (

	// Parse columns until we hit the closing parenthesis
	for p.peek(0).tokenT != RPAREN_TOK && p.peek(0).tokenT != SEMICOLON_TOK {
		if p.peek(0).tokenT != IDENT_TOK {
			break // Exit column parsing when we hit non-identifier
		}

		columnName := p.peek(0).value.(string)
		p.consume() // Consume column name

		// Check for duplicate column names during parsing
		if _, exists := createTableStmt.TableSchema.ColumnDefinitions[columnName]; exists {
			return nil, p.newValidationError(ErrDuplicateColumnName,
				fmt.Sprintf("duplicate column name '%s'", columnName),
				&p.lexer.tokens[p.pos-1], // Previous token was the column name
				[]string{"unique column name"})
		}

		// Parse Iceberg data type with support for complex types (list, map, struct)
		var dataType string
		var err error

		if p.peek(0).tokenT == DATATYPE_TOK {
			dataType = p.peek(0).value.(string)
			p.consume() // Consume data type

			// Check if this is a complex type that needs further parsing
			if dataType == types.IcebergList || dataType == types.IcebergMap || dataType == types.IcebergStruct {
				// Parse complex type syntax
				complexType, parseErr := p.parseComplexIcebergType(dataType)
				if parseErr != nil {
					return nil, parseErr
				}
				dataType = complexType
			}
		} else if p.peek(0).tokenT == IDENT_TOK {
			// Handle identifiers that might be Iceberg types
			dataType = p.peek(0).value.(string)
			p.consume() // Consume the identifier

			// Check if this is a complex type that needs further parsing
			if dataType == types.IcebergList || dataType == types.IcebergMap || dataType == types.IcebergStruct {
				// Parse complex type syntax
				complexType, parseErr := p.parseComplexIcebergType(dataType)
				if parseErr != nil {
					return nil, parseErr
				}
				dataType = complexType
			}
		} else if p.peek(0).tokenT == KEYWORD_TOK {
			// Handle legacy SQL types that come through as keywords - reject with descriptive error
			legacyType := p.peek(0).value.(string)
			currentToken := p.peekToken(0)

			// Map legacy SQL types to Iceberg equivalents
			icebergEquivalent := p.getLegacySQLTypeMapping(legacyType)
			if icebergEquivalent != "" {
				message := fmt.Sprintf("unsupported SQL type '%s'. Use Iceberg type '%s' instead", legacyType, icebergEquivalent)
				return nil, p.newTypeError(ErrUnsupportedSQLType, message, currentToken, []string{icebergEquivalent})
			}

			// Generic error for unknown legacy types
			message := fmt.Sprintf("legacy SQL type '%s' is not supported. Only Iceberg types are allowed", legacyType)
			return nil, p.newTypeError(ErrUnsupportedSQLType, message, currentToken, []string{"valid Iceberg type"})
		} else {
			return nil, p.expectDataType(p.peekToken(0))
		}

		// Don't validate here yet - we might need to parse decimal parameters first

		// Create column definition (always create it here to ensure consistency)
		createTableStmt.TableSchema.ColumnDefinitions[columnName] = &ColumnDefinition{
			Name:       columnName,
			DataType:   dataType,
			IsNullable: true, // Default to nullable
		}

		// Handle decimal(precision,scale) format for Iceberg decimal types
		if p.peek(0).tokenT == LPAREN_TOK && dataType == types.IcebergDecimal {
			p.consume() // Consume (

			if p.peek(0).tokenT != LITERAL_TOK {
				return nil, p.expectLiteral(p.peekToken(0))
			}

			precision := p.peek(0).value.(uint64)
			p.consume() // Consume precision literal

			if p.peek(0).tokenT != COMMA_TOK {
				return nil, p.expectPunctuation(",", p.peekToken(0))
			}

			p.consume() // Consume ,

			if p.peek(0).tokenT != LITERAL_TOK {
				return nil, p.expectLiteral(p.peekToken(0))
			}

			scale := p.peek(0).value.(uint64)
			p.consume() // Consume scale literal

			if p.peek(0).tokenT != RPAREN_TOK {
				return nil, p.expectPunctuation(")", p.peekToken(0))
			}

			p.consume() // Consume )

			// Update the data type to include precision and scale
			dataType = fmt.Sprintf("decimal(%d,%d)", precision, scale)
			createTableStmt.TableSchema.ColumnDefinitions[columnName].DataType = dataType
			createTableStmt.TableSchema.ColumnDefinitions[columnName].Precision = int(precision)
			createTableStmt.TableSchema.ColumnDefinitions[columnName].Scale = int(scale)

			// Validate the decimal type
			if err := p.validateIcebergType(dataType); err != nil {
				return nil, err
			}
		} else if dataType == types.IcebergDecimal {
			// If decimal is used without precision/scale, it's invalid in Iceberg
			currentToken := &p.lexer.tokens[p.pos-1]
			message := "decimal type requires precision and scale: decimal(precision,scale)"
			return nil, p.newTypeError(ErrComplexTypeParseError, message, currentToken, []string{"decimal(10,2)"})
		}

		// Validate Iceberg type after handling decimal parameters
		if err := p.validateIcebergType(dataType); err != nil {
			return nil, err
		}

		// Parse column constraints
		err = p.parseTableConstraints(createTableStmt, columnName)
		if err != nil {
			return nil, err
		}

		// Check for comma separator between columns
		if p.peek(0).tokenT == COMMA_TOK {
			p.consume() // Consume comma
		}
	}

	// Consume the closing parenthesis
	if p.peek(0).tokenT == RPAREN_TOK {
		p.consume() // Consume )
	} else {
		return nil, p.expectPunctuation(")", p.peekToken(0))
	}

	// Parse additional clauses (STORAGE, PARTITION BY, ORDER BY, SETTINGS, ENGINE)
	// These are parsed after the column definitions
	for {
		// Check if we're at the end of the statement
		if p.peek(0).tokenT == SEMICOLON_TOK || p.peek(0).tokenT == EOF_TOK {
			break
		}

		// Try to parse a clause
		clauseParsed, err := p.parseCreateTableClause(createTableStmt)
		if err != nil {
			return nil, err
		}

		// If no clause was parsed, break to avoid infinite loop
		if !clauseParsed {
			break
		}
	}

	// Validate that we have either a storage engine or legacy engine
	if createTableStmt.StorageEngine == nil {
		return nil, errors.New(ErrTableNameRequired, "STORAGE clause is required for CREATE TABLE statements", nil)
	}

	// Validate storage engine if specified
	if createTableStmt.StorageEngine != nil {
		validEngines := map[string]bool{
			"filesystem": true,
			"memory":     true,
			"s3":         true,
		}
		if !validEngines[strings.ToLower(createTableStmt.StorageEngine.Value)] {
			return nil, errors.New(ErrExpectedIdentifier, fmt.Sprintf("invalid storage engine: %s", createTableStmt.StorageEngine.Value), nil)
		}
	}

	// Validate the complete CREATE TABLE statement with Iceberg types
	if err := createTableStmt.Validate(); err != nil {
		return nil, err
	}

	return createTableStmt, nil
}

// validateIcebergType validates an Iceberg type string during parsing with descriptive error messages
func (p *Parser) validateIcebergType(typeStr string) error {
	validator := types.NewIcebergTypeValidator()
	if !validator.IsValidType(typeStr) {
		// Get the current token for error context
		currentToken := &p.lexer.tokens[p.pos-1] // Previous token was the type

		// Check if it's a legacy SQL type and provide specific mapping
		icebergEquivalent := p.getLegacySQLTypeMapping(typeStr)
		if icebergEquivalent != "" {
			message := fmt.Sprintf("unsupported SQL type '%s'. Use Iceberg type '%s' instead", typeStr, icebergEquivalent)
			return p.newTypeError(ErrUnsupportedSQLType, message, currentToken, []string{icebergEquivalent})
		}

		// Provide comprehensive Iceberg type suggestions including complex types
		suggestions := []string{
			"boolean", "int32", "int64", "float32", "float64",
			"decimal(precision,scale)", "string", "binary", "date", "time",
			"timestamp", "timestamptz", "uuid",
			"list<elementType>", "map<keyType,valueType>", "struct<field:type,...>",
		}

		message := fmt.Sprintf("invalid Iceberg type '%s'. Only Iceberg types are supported", typeStr)
		return p.newTypeError(ErrInvalidIcebergType, message, currentToken, suggestions)
	}

	// Validate complex types for proper structure
	if err := validator.ValidateComplexType(typeStr); err != nil {
		currentToken := &p.lexer.tokens[p.pos-1]
		message := fmt.Sprintf("complex type validation failed for '%s': %v", typeStr, err)
		return p.newTypeError(ErrComplexTypeParseError, message, currentToken, []string{"valid complex type syntax"})
	}

	return nil
}

// parseComplexIcebergType parses complex Iceberg types like list<string>, map<string,int32>, struct<name:string,age:int32>
func (p *Parser) parseComplexIcebergType(baseType string) (string, error) {
	var typeStr strings.Builder
	typeStr.WriteString(baseType) // Start with the base type (list, map, struct)

	// Expect opening angle bracket
	if p.peek(0).tokenT != COMPARISON_TOK || p.peek(0).value != "<" {
		return "", p.newTypeError(ErrComplexTypeParseError,
			fmt.Sprintf("expected '<' after %s type", baseType),
			p.peekToken(0), []string{"<"})
	}

	typeStr.WriteString("<")
	p.consume() // Consume <

	depth := 1

	// Parse the complete type string including nested brackets
	for depth > 0 {
		if p.peek(0).tokenT == EOF_TOK {
			return "", p.newTypeError(ErrComplexTypeParseError,
				"unexpected end of input while parsing complex type",
				p.peekToken(0), []string{">"})
		}

		token := p.peek(0)
		switch token.tokenT {
		case IDENT_TOK, DATATYPE_TOK:
			typeStr.WriteString(token.value.(string))
			p.consume()
		case COMPARISON_TOK:
			if token.value == "<" {
				typeStr.WriteString("<")
				depth++
				p.consume()
			} else if token.value == ">" {
				typeStr.WriteString(">")
				depth--
				p.consume()
			} else {
				return "", p.newTypeError(ErrComplexTypeParseError,
					fmt.Sprintf("unexpected comparison operator '%s' in complex type", token.value),
					p.peekToken(0), []string{"<", ">"})
			}
		case COMMA_TOK:
			typeStr.WriteString(",")
			p.consume()
		case COLON_TOK:
			// For struct field separators like field:type (can be nested in other types)
			typeStr.WriteString(":")
			p.consume()
		case DOT_TOK:
			// Dots are not expected in complex types
			return "", p.newTypeError(ErrComplexTypeParseError,
				fmt.Sprintf("unexpected '.' in %s type", baseType),
				p.peekToken(0), []string{"valid type syntax"})
		case LITERAL_TOK:
			// Handle numeric literals in decimal types like decimal(10,2)
			typeStr.WriteString(fmt.Sprintf("%v", token.value))
			p.consume()
		case LPAREN_TOK:
			// Handle parentheses for decimal(precision,scale) syntax
			typeStr.WriteString("(")
			p.consume()
		case RPAREN_TOK:
			// Handle closing parentheses for decimal(precision,scale) syntax
			typeStr.WriteString(")")
			p.consume()
		default:
			return "", p.newTypeError(ErrComplexTypeParseError,
				fmt.Sprintf("unexpected token '%v' in complex type", token.value),
				p.peekToken(0), []string{"valid type syntax"})
		}
	}

	// Validate the complete complex type
	finalType := typeStr.String()
	validator := types.NewIcebergTypeValidator()
	if !validator.IsValidType(finalType) {
		return "", p.newTypeError(ErrComplexTypeParseError,
			fmt.Sprintf("invalid complex type syntax: %s", finalType),
			p.peekToken(0), []string{"valid complex type"})
	}

	return finalType, nil
}

func (p *Parser) parseTableConstraints(createTableStmt *CreateTableStmt, columnName string) error {
	// Check for constraints
	if p.peek(0).tokenT == KEYWORD_TOK {
		for p.peek(0).tokenT == KEYWORD_TOK {
			switch p.peek(0).value {
			case "CHECK":
				p.consume() // Consume CHECK

				if p.peek(0).tokenT != LPAREN_TOK {
					return errors.New(ErrExpectedLeftParen, "expected (", nil)
				}

				p.consume() // Consume (

				searchCond, err := p.parseSearchCondition()
				if err != nil {
					return err
				}

				// Convert searchCond to string for Check constraint
				if checkStr, ok := searchCond.(string); ok {
					createTableStmt.TableSchema.ColumnDefinitions[columnName].Check = &checkStr
				}

				if p.peek(0).tokenT != RPAREN_TOK {
					return errors.New(ErrExpectedRightParen, "expected )", nil)
				}

				p.consume() // Consume )

			case "DEFAULT":
				p.consume() // Consume DEFAULT

				if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != KEYWORD_TOK {
					return errors.New(ErrExpectedLiteral, "expected literal or keyword", nil)
				}

				defaultValue := p.peek(0).value

				// Store default value as string for Iceberg compatibility
				if defaultValue == "SYS_DATE" {
					createTableStmt.TableSchema.ColumnDefinitions[columnName].DefaultValue = "SYS_DATE"
				} else if defaultValue == "SYS_TIME" {
					createTableStmt.TableSchema.ColumnDefinitions[columnName].DefaultValue = "SYS_TIME"
				} else if defaultValue == "SYS_TIMESTAMP" {
					createTableStmt.TableSchema.ColumnDefinitions[columnName].DefaultValue = "SYS_TIMESTAMP"
				} else if defaultValue == "GENERATE_UUID" {
					createTableStmt.TableSchema.ColumnDefinitions[columnName].DefaultValue = "GENERATE_UUID"
				} else {
					// Remove quotes from string literals
					defaultStr := fmt.Sprintf("%v", defaultValue)
					if len(defaultStr) >= 2 && defaultStr[0] == '\'' && defaultStr[len(defaultStr)-1] == '\'' {
						defaultStr = defaultStr[1 : len(defaultStr)-1] // Remove surrounding quotes
					}
					createTableStmt.TableSchema.ColumnDefinitions[columnName].DefaultValue = defaultStr
				}

				p.consume() // Consume literal or keyword

			case "PRIMARY":
				p.consume() // Consume PRIMARY
				if p.peek(0).value != "KEY" {
					return errors.New(ErrExpectedKeyword, "expected KEY", nil)
				}

				p.consume() // Consume KEY
				// We set not null to true
				// We set unique to true
				// We set sequence to true
				createTableStmt.TableSchema.ColumnDefinitions[columnName].NotNull = true
				createTableStmt.TableSchema.ColumnDefinitions[columnName].Unique = true
				sequenceStr := "AUTO_INCREMENT"
				createTableStmt.TableSchema.ColumnDefinitions[columnName].Sequence = &sequenceStr

			case "FOREIGN":
				p.consume() // Consume FOREIGN
				// FOREIGN KEY (1) REFERENCES Departments(2)
				// 1, 2 should be the same column identifier
				if p.peek(0).value != "KEY" {
					return errors.New(ErrExpectedKeyword, "expected KEY", nil)
				}

				p.consume() // Consume KEY

				if p.peek(0).tokenT != LPAREN_TOK {
					return errors.New(ErrExpectedLeftParen, "expected (", nil)
				}

				p.consume() // Consume (

				refColumn := ""

				if p.peek(0).tokenT != IDENT_TOK {
					return errors.New(ErrExpectedIdentifier, "expected identifier", nil)
				}

				refColumn = p.peek(0).value.(string)

				p.consume() // Consume column name

				if p.peek(0).tokenT != RPAREN_TOK {
					return errors.New(ErrExpectedRightParen, "expected )", nil)
				}

				p.consume() // Consume )

				// Check for REFERENCES keyword
				if p.peek(0).value != "REFERENCES" {
					return errors.New(ErrExpectedKeyword, "expected REFERENCES", nil)
				}

				p.consume() // Consume REFERENCES

				if p.peek(0).tokenT != IDENT_TOK {
					return errors.New(ErrExpectedIdentifier, "expected identifier", nil)
				}

				refTable := p.peek(0).value.(string)

				p.consume() // Consume table name

				createTableStmt.TableSchema.ColumnDefinitions[refColumn].References = &Reference{
					TableName:  refTable,
					ColumnName: refColumn,
				}

				if p.peek(0).tokenT != LPAREN_TOK {
					return errors.New(ErrExpectedLeftParen, "expected (", nil)
				}

				p.consume() // Consume (

				if p.peek(0).tokenT != IDENT_TOK {
					return errors.New(ErrExpectedIdentifier, "expected identifier", nil)
				}

				// Check if the column name is the same as the reference column name
				if p.peek(0).value != refColumn {
					return errors.New(ErrExpectedIdentifier, "expected column name to be the same as the reference column name", nil)
				}

			case "NOT":
				p.consume() // Consume NOT

				if p.peek(0).value != "NULL" {
					return errors.New(ErrExpectedKeyword, "expected NULL", nil)
				}

				p.consume() // Consume NULL

				createTableStmt.TableSchema.ColumnDefinitions[columnName].NotNull = true
				createTableStmt.TableSchema.ColumnDefinitions[columnName].IsNullable = false
			case "UNIQUE":
				createTableStmt.TableSchema.ColumnDefinitions[columnName].Unique = true

				p.consume() // Consume UNIQUE
			case "SEQUENCE":
				sequenceStr := "SEQUENCE"
				createTableStmt.TableSchema.ColumnDefinitions[columnName].Sequence = &sequenceStr

				p.consume() // Consume SEQUENCE
			case "ENCRYPT":
				createTableStmt.Encrypt = true

				p.consume() // Consume ENCRYPT

				// look for (
				if p.peek(0).tokenT != LPAREN_TOK {
					return errors.New(ErrExpectedLeftParen, "expected (", nil)
				}

				p.consume()

				key, err := p.parseLiteral()
				if err != nil {
					return err
				}

				createTableStmt.EncryptKey = key.(*Literal)

				// look for )
				if p.peek(0).tokenT != RPAREN_TOK {
					return errors.New(ErrExpectedRightParen, "expected )", nil)
				}

				p.consume() // Consume )
			case "COMPRESS":
				createTableStmt.Compress = true
				p.consume() // Consume COMPRESS

			default:
				return errors.New(ErrExpectedKeyword, "expected NOT NULL, UNIQUE, SEQUENCE, PRIMARY KEY, FOREIGN KEY, CHECK, DEFAULT, COMPRESS, ENCRYPT", nil)
			}

		}
	}

	return nil

}

// parseCreateIndexStmt parses a CREATE INDEX statement
func (p *Parser) parseCreateIndexStmt() (Node, error) {
	createIndexStmt := &CreateIndexStmt{}
	// CREATE INDEX index_name ON schema_name.table_name (column_name1, column_name2, ...)
	// creating unique index
	// CREATE UNIQUE INDEX index_name ON schema_name.table_name (column_name1, column_name2, ...)

	// Eat INDEX
	p.consume()

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	indexName := p.peek(0).value.(string)
	p.consume() // Consume index name

	if p.peek(0).value != "ON" {
		return nil, errors.New(ErrExpectedOn, "expected ON", nil)
	}

	p.consume() // Consume ON

	// Parse table identifier (can be qualified or unqualified)
	tableIdent, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)

	}

	p.consume() // Consume (

	createIndexStmt.TableName = tableIdent
	createIndexStmt.IndexName = &Identifier{Value: indexName}
	createIndexStmt.ColumnNames = make([]*Identifier, 0)

	for {
		if p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
		}

		columnName := p.peek(0).value.(string)
		createIndexStmt.ColumnNames = append(createIndexStmt.ColumnNames, &Identifier{Value: columnName})

		p.consume() // Consume column name

		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT != COMMA_TOK {
			return nil, errors.New(ErrExpectedComma, "expected ,", nil)
		}

		p.consume() // Consume ,

	}

	if p.peek(0).tokenT != RPAREN_TOK {
		return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
	}

	p.consume() // Consume )

	return createIndexStmt, nil
}

// parseCreateDatabaseStmt parses a CREATE DATABASE statement
func (p *Parser) parseCreateDatabaseStmt() (Node, error) {
	p.consume() // Consume DATABASE

	// Check for IF NOT EXISTS clause BEFORE the identifier
	ifNotExists := false
	if p.peek(0).tokenT == KEYWORD_TOK && p.peek(0).value == "IF NOT EXISTS" {
		p.consume() // Consume IF NOT EXISTS
		ifNotExists = true
	}

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	name := p.peek(0).value.(string)
	p.consume() // Consume identifier

	return &CreateDatabaseStmt{
		Name:        &Identifier{Value: name},
		IfNotExists: ifNotExists,
	}, nil
}

// parseUseStmt parses a USE statement
func (p *Parser) parseUseStmt() (Node, error) {
	p.consume() // Consume USE

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	name := p.peek(0).value.(string)
	p.consume() // Consume identifier

	return &UseStmt{
		DatabaseName: &Identifier{Value: name},
	}, nil

}

// parseSelectStmt parses a SELECT statement
func (p *Parser) parseSelectStmt() (Node, error) {

	selectStmt := &SelectStmt{}

	// Eat SELECT
	p.consume()

	// Check for DISTINCT
	if p.peek(0).value == "DISTINCT" {
		selectStmt.Distinct = true
		p.consume()
	}

	// Parse select list
	err := p.parseSelectList(selectStmt)
	if err != nil {
		return nil, err
	}

	// Check for FROM
	if p.peek(0).value == "FROM" {
		tableExpr, err := p.parseTableExpression()
		if err != nil {
			return nil, err

		}

		selectStmt.TableExpression = tableExpr

	}

	// Check for WHERE
	if p.peek(0).value == "WHERE" {

		whereClause, err := p.parseWhereClause()
		if err != nil {
			return nil, err
		}

		selectStmt.TableExpression.WhereClause = whereClause

	}

	// Look for GROUP BY
	if p.peek(0).value == "GROUP" {
		if p.peek(1).value != "BY" {
			return nil, p.expectKeyword("BY", p.peekToken(1))

		} else {
			p.consume()
			p.consume()

			groupByClause, err := p.parseGroupByClause()
			if err != nil {
				return nil, err
			}

			selectStmt.TableExpression.GroupByClause = groupByClause

		}
	}

	if p.peek(0).value == "HAVING" {
		havingClause, err := p.parseHavingClause()
		if err != nil {
			return nil, err
		}

		selectStmt.TableExpression.HavingClause = havingClause

	}

	// Look for ORDER BY
	if p.peek(0).value == "ORDER" {
		if p.peek(1).value != "BY" {
			return nil, p.expectKeyword("BY", p.peekToken(1))

		} else {

			orderByClause, err := p.parseOrderByClause()
			if err != nil {
				return nil, err
			}

			selectStmt.TableExpression.OrderByClause = orderByClause

		}

	}

	if p.peek(0).value == "LIMIT" {
		limitClause, err := p.parseLimitClause()
		if err != nil {
			return nil, err
		}

		selectStmt.TableExpression.LimitClause = limitClause
	}

	// Look for union
	if p.peek(0).value == "UNION" {
		p.consume()
		if p.peek(0).value == "ALL" {
			selectStmt.UnionAll = true
			p.consume()
		}

		unionStmt, err := p.parseSelectStmt()
		if err != nil {
			return nil, err
		}

		selectStmt.Union = unionStmt.(*SelectStmt)

	}

	return selectStmt, nil

}

// parseLimitClause parses a LIMIT clause
func (p *Parser) parseLimitClause() (*LimitClause, error) {
	limitClause := &LimitClause{}

	// Eat LIMIT
	p.consume()

	if p.peek(0).tokenT != LITERAL_TOK {
		return nil, p.expectLiteral(p.peekToken(0))
	}

	count := p.peek(0).value.(uint64)

	p.consume()

	// check for offset
	if p.peek(0).value == "OFFSET" {
		p.consume()

		if p.peek(0).tokenT != LITERAL_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
		}

		offset := p.peek(0).value.(uint64)
		limitClause.Offset = &Literal{Value: offset}

		p.consume()
	}

	limitClause.Count = &Literal{Value: count}

	return limitClause, nil

}

// parseOrderByClause parses an ORDER BY clause
func (p *Parser) parseOrderByClause() (*OrderByClause, error) {
	orderByClause := &OrderByClause{}

	// Parse order by list
	err := p.parseOrderByList(orderByClause)
	if err != nil {
		return nil, err
	}

	return orderByClause, nil
}

// parseOrderByList parses an order by list
func (p *Parser) parseOrderByList(orderByClause *OrderByClause) error {
	p.consume() // Consume ORDER

	if p.peek(0).value != "BY" {
		return errors.New(ErrExpectedKeyword, "expected BY", nil)
	}

	p.consume() // Consume BY

	for p.peek(0).tokenT != EOF_TOK || p.peek(0).value != "ROWS" || p.peek(0).value == "RANGE" || p.peek(0).tokenT != RPAREN_TOK {

		// Parse order by expression
		expr, err := p.parseValueExpression()
		if err != nil {
			return err
		}

		orderByClause.OrderByExpressions = append(orderByClause.OrderByExpressions, expr)

		// Look for ,
		if p.peek(0).value == "," {
			p.consume() // Consume ,

			continue
		}

		break

	}

	if p.peek(0).value == "ASC" {
		orderByClause.Order = ASC
		p.consume()

	} else if p.peek(0).value == "DESC" {
		orderByClause.Order = DESC
		p.consume()
	} else {
		orderByClause.Order = ASC

	}

	return nil

}

// parseGroupByClause
func (p *Parser) parseGroupByClause() (*GroupByClause, error) {

	groupByClause := &GroupByClause{}

	// Parse group by list
	err := p.parseGroupByList(groupByClause)
	if err != nil {
		return nil, err
	}

	return groupByClause, nil
}

// parseGroupByList parses a group by list
func (p *Parser) parseGroupByList(groupByClause *GroupByClause) error {
	// Parse group by expression
	expr, err := p.parseValueExpression()
	if err != nil {
		return err
	}

	groupByClause.GroupByExpressions = append(groupByClause.GroupByExpressions, expr)

	// Look for ,
	for p.peek(0).value == "," {
		p.consume() // Consume ,

		expr, err := p.parseValueExpression()
		if err != nil {
			return err
		}

		groupByClause.GroupByExpressions = append(groupByClause.GroupByExpressions, expr)
	}

	return nil
}

// parseWhereClause parses a WHERE clause
func (p *Parser) parseWhereClause() (*WhereClause, error) {
	whereClause := &WhereClause{}

	// Eat WHERE
	p.consume()

	// Parse search condition
	searchCondition, err := p.parseSearchCondition()
	if err != nil {
		return nil, err
	}

	whereClause.SearchCondition = searchCondition

	return whereClause, nil

}

// parseSearchCondition parses a search condition
func (p *Parser) parseSearchCondition() (interface{}, error) {
	// A search condition can be a binary expression, comparison expression, or a logical expression

	var expr interface{}
	var err error
	var not *NotExpr

	if p.peek(0).tokenT == IDENT_TOK {
		if p.peek(1).value == "NOT" {
			// put ident in the not position

			p.switchToken(0, 1)

			p.consume()

			not = &NotExpr{}
		}

	}

	if p.peek(0).value == "EXISTS" {
		// Parse subquery

		p.consume()
		p.consume()

		subquery, err := p.parseSubquery()
		if err != nil {
			return nil, err
		}

		return &ExistsPredicate{
			Expr: subquery,
		}, nil
	}

	if p.peek(1).tokenT == COMPARISON_TOK || p.peek(1).tokenT == ASTERISK_TOK || p.peek(1).tokenT == PLUS_TOK || p.peek(1).tokenT == MINUS_TOK || p.peek(1).tokenT == DIVIDE_TOK || p.peek(1).tokenT == MODULUS_TOK || p.peek(1).tokenT == AT_TOK {
		// Parse comparison expression
		expr, err = p.parseComparisonExpr(nil)
		if err != nil {
			return nil, err
		}

	} else if p.peek(1).tokenT == KEYWORD_TOK {
		// Check for case
		if p.peek(0).value == "CASE" {
			expr, err = p.parseCaseExpr()
			if err != nil {
				return nil, err
			}

			// Check next token
			// Case can be evaluated as a left side of a predicate
			if p.peek(0).tokenT == COMPARISON_TOK || p.peek(0).tokenT == ASTERISK_TOK || p.peek(0).tokenT == PLUS_TOK || p.peek(0).tokenT == MINUS_TOK || p.peek(0).tokenT == DIVIDE_TOK || p.peek(0).tokenT == MODULUS_TOK || p.peek(0).tokenT == AT_TOK {
				// Parse comparison expression
				expr, err = p.parseComparisonExpr(&ValueExpression{
					Value: expr,
				})
				if err != nil {
					return nil, err
				}
			} else if p.peek(0).tokenT == KEYWORD_TOK {
				switch p.peek(0).value {
				case "BETWEEN":
					// Parse between expression
					expr, err = p.parseBetweenExpr(&ValueExpression{
						Value: expr,
					})
					if err != nil {
						return nil, err
					}

					if not != nil {
						not.Expr = expr
						expr = not
					}
				case "IN":
					// Parse in expression
					expr, err = p.parseInExpr(&ValueExpression{
						Value: expr,
					})
					if err != nil {
						return nil, err
					}

					if not != nil {
						not.Expr = expr
						expr = not
					}
				case "LIKE":
					// Parse like expression
					expr, err = p.parseLikeExpr(&ValueExpression{
						Value: expr,
					})
					if err != nil {
						return nil, err
					}

					if not != nil {
						not.Expr = expr
						expr = not
					}
				case "IS":
					// Parse is expression
					expr, err = p.parseIsExpr(&ValueExpression{
						Value: expr,
					})
					if err != nil {
						return nil, err
					}
				default:
					return nil, errors.New(ErrExpectedKeyword, "expected predicate or logical expression", nil)

				}
			}

		}

		switch p.peek(1).value {
		case "BETWEEN":

			// Parse between expression
			expr, err = p.parseBetweenExpr(nil)
			if err != nil {
				return nil, err
			}

			if not != nil {
				not.Expr = expr
				expr = not
			}

		case "IN":
			// Parse in expression
			expr, err = p.parseInExpr(nil)
			if err != nil {
				return nil, err
			}

			if not != nil {
				not.Expr = expr
				expr = not
			}
		case "LIKE":
			// Parse like expression
			expr, err = p.parseLikeExpr(nil)
			if err != nil {
				return nil, err
			}

			if not != nil {
				not.Expr = expr
				expr = not
			}
		case "IS":
			// Parse is expression
			expr, err = p.parseIsExpr(nil)
			if err != nil {
				return nil, err
			}

		}
	} else if p.peek(0).tokenT == KEYWORD_TOK {

		currentPos := p.pos

		if p.peek(0).value == "AVG" || p.peek(0).value == "COUNT" || p.peek(0).value == "MAX" || p.peek(0).value == "MIN" || p.peek(0).value == "SUM" {
			expr, err = p.parseAggregateFunc()
			if err != nil {
				return nil, err
			}
		} else if p.peek(0).value == "LENGTH" || p.peek(0).value == "LOWER" || p.peek(0).value == "UPPER" || p.peek(0).value == "TRIM" || p.peek(0).value == "SUBSTRING" || p.peek(0).value == "POSITION" || p.peek(0).value == "CONCAT" || p.peek(0).value == "COALESCE" ||
			p.peek(0).value == "CAST" || p.peek(0).value == "REVERSE" || p.peek(0).value == "ROUND" || p.peek(0).value == "REPLACE" || p.peek(0).value == "TRIM" || p.peek(0).value == "COALESCE" {
			expr, err = p.parseSystemFunc()
			if err != nil {
				return nil, err
			}
		}

		if p.peek(0).tokenT == COMPARISON_TOK || p.peek(0).tokenT == ASTERISK_TOK || p.peek(0).tokenT == PLUS_TOK || p.peek(0).tokenT == MINUS_TOK || p.peek(0).tokenT == DIVIDE_TOK || p.peek(0).tokenT == MODULUS_TOK || p.peek(0).tokenT == AT_TOK {
			// Parse comparison expression
			p.pos = currentPos

			expr, err = p.parseComparisonExpr(nil)
			if err != nil {
				return nil, err
			}

		}

		switch p.peek(0).value {
		case "BETWEEN":

			// Parse between expression
			expr, err = p.parseBetweenExpr(nil)
			if err != nil {
				return nil, err
			}

			if not != nil {
				not.Expr = expr
				expr = not
			}

		case "IN":
			// Parse in expression
			expr, err = p.parseInExpr(nil)
			if err != nil {
				return nil, err
			}

			if not != nil {
				not.Expr = expr
				expr = not
			}
		case "LIKE":
			// Parse like expression
			expr, err = p.parseLikeExpr(nil)
			if err != nil {
				return nil, err
			}

			if not != nil {
				not.Expr = expr
				expr = not
			}
		case "IS":
			// Parse is expression
			expr, err = p.parseIsExpr(nil)
			if err != nil {
				return nil, err
			}

		}
	} else {
		return nil, errors.New(ErrExpectedKeyword, "expected predicate or logical expression", nil)
	}

	if p.peek(0).tokenT == KEYWORD_TOK {
		if p.peek(0).value == "AND" || p.peek(0).value == "OR" {
			// Parse logical expression
			expr, err = p.parseLogicalExpr(expr)
			if err != nil {
				return nil, err
			}

		}
	}

	return expr, nil

}

// parseLikeExpr parses a LIKE expression
func (p *Parser) parseLikeExpr(left *ValueExpression) (*LikePredicate, error) {
	// Parse left side of like expression

	if left == nil {
		var err error
		left, err = p.parseValueExpression()
		if err != nil {
			return nil, err
		}
	}

	// Eat LIKE
	p.consume()

	// Parse pattern
	pattern, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	return &LikePredicate{
		Left:    left,
		Pattern: pattern,
	}, nil

}

// parseIsExpr parses an IS expression
func (p *Parser) parseIsExpr(left *ValueExpression) (*IsPredicate, error) {
	// Parse left side of is expression
	if left == nil {
		var err error
		left, err = p.parseValueExpression()
		if err != nil {
			return nil, err
		}
	}

	// Eat IS
	p.consume()

	// NULL or NOT NULL

	if p.peek(0).value == "NULL" {
		p.consume()
		return &IsPredicate{
			Left: left,
			Null: true,
		}, nil
	} else if p.peek(0).value == "NOT" {
		// Eat NOT
		p.consume()

		if p.peek(0).value != "NULL" {
			return nil, errors.New(ErrExpectedKeyword, "expected NULL", nil)
		}

		p.consume()

		return &IsPredicate{
			Left: left,
			Null: false,
		}, nil

	}

	return nil, errors.New(ErrExpectedKeyword, "expected NULL or NOT NULL", nil)

}

// parseInExpr parses an IN expression
func (p *Parser) parseInExpr(left *ValueExpression) (*InPredicate, error) {
	// Parse left side of in expression

	if left == nil {
		var err error
		left, err = p.parseValueExpression()
		if err != nil {
			return nil, err
		}
	}

	// Eat IN
	p.consume()

	// Eat (
	p.consume()

	inPredicate := &InPredicate{
		Left: left,
	}

	if p.peek(0).value == "SELECT" {
		// Parse subquery
		subquery, err := p.parseSubquery()
		if err != nil {
			return nil, err
		}

		inPredicate.Values = append(inPredicate.Values, subquery)

		// Eat )
		p.consume()

		return inPredicate, nil

	}

	for p.peek(0).tokenT != EOF_TOK {
		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT == COMMA_TOK {
			p.consume()
			continue
		}

		// Parse right side of in expression
		right, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		inPredicate.Values = append(inPredicate.Values, right)
	}

	// Eat )
	p.consume()

	return inPredicate, nil

}

// parseBetweenExpr parses a between expression
func (p *Parser) parseBetweenExpr(left *ValueExpression) (*BetweenPredicate, error) {
	// check for not if there remove

	if left == nil {
		var err error
		// Parse left side of between expression
		left, err = p.parseValueExpression()
		if err != nil {
			return nil, err
		}
	}

	// Eat BETWEEN
	p.consume()

	// Parse lower bound
	lower, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	// Eat AND
	p.consume()

	// Parse upper bound
	upper, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	return &BetweenPredicate{
		Left:  left,
		Lower: lower,
		Upper: upper,
	}, nil

}

// parseComparisonExpr parses a comparison expression
func (p *Parser) parseComparisonExpr(left *ValueExpression) (*ComparisonPredicate, error) {
	// Parse left side of comparison
	if left == nil {
		var err error
		left, err = p.parseValueExpression()
		if err != nil {
			return nil, err
		}
	}

	// Parse comparison operator
	op := p.peek(0).value.(string)

	p.consume()

	// Parse right side of comparison
	right, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	return &ComparisonPredicate{
		Left:  left,
		Op:    getComparisonOperator(op),
		Right: right,
	}, nil
}

// parseLogicalExpr parses a logical expression
func (p *Parser) parseLogicalExpr(left interface{}) (*LogicalCondition, error) {

	// Parse logical operator
	op := p.peek(0).value.(string)
	p.consume()

	// Parse right side of logical expression
	right, err := p.parseSearchCondition()
	if err != nil {
		return nil, err
	}

	return &LogicalCondition{
		Left:  left,
		Op:    getLogicalOperator(op),
		Right: right,
	}, nil
}

// parseTableExpression parses a table expression
func (p *Parser) parseTableExpression() (*TableExpression, error) {
	tableExpr := &TableExpression{}

	// Eat FROM
	p.consume()

	// Parse from clause
	fromClause, err := p.parseFromClause()
	if err != nil {
		return nil, err
	}

	tableExpr.FromClause = fromClause

	return tableExpr, nil
}

// parseFromClause parses a FROM clause
func (p *Parser) parseFromClause() (*FromClause, error) {
	fromClause := &FromClause{
		Tables: make([]*Table, 0),
	}

	for p.peek(0).tokenT != SEMICOLON_TOK || p.peek(0).value != "WHERE" || p.peek(0).value != "INNER" || p.peek(0).value != "LEFT" || p.peek(0).value != "RIGHT" || p.peek(0).value != "FULL" || p.peek(0).value != "GROUP" || p.peek(0).value != "HAVING" || p.peek(0).value != "ORDER" || p.peek(0).value != "LIMIT" || p.peek(0).value != "UNION" || p.peek(0).value != "JOIN" {
		if p.peek(0).tokenT == COMMA_TOK {
			p.consume()

			continue
		}

		if p.peek(0).tokenT == SEMICOLON_TOK || p.peek(0).value == "WHERE" || p.peek(0).tokenT == LPAREN_TOK || p.peek(0).tokenT == RPAREN_TOK || p.peek(0).value == "GROUP" || p.peek(0).value == "HAVING" || p.peek(0).value == "ORDER" || p.peek(0).value == "LIMIT" || p.peek(0).value == "INNER" || p.peek(0).value == "LEFT" || p.peek(0).value == "RIGHT" || p.peek(0).value == "FULL" || p.peek(0).value == "GROUP" || p.peek(0).value == "HAVING" || p.peek(0).value == "ORDER" || p.peek(0).value == "LIMIT" || p.peek(0).value == "UNION" || p.peek(0).value == "JOIN" || p.peek(0).value == "JOIN" {
			break
		}

		// Parse table
		table, err := p.parseTable()
		if err != nil {
			return nil, err
		}

		fromClause.Tables = append(fromClause.Tables, table)
	}

	return fromClause, nil
}

// parseTable parses a table
func (p *Parser) parseTable() (*Table, error) {
	table := &Table{}

	// Parse first identifier (could be database or table name)
	firstIdent, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}

	// Check if next token is a dot (indicating database.table format)
	if p.peek(0).tokenT == DOT_TOK {
		// This is database.table format
		table.Database = firstIdent
		p.consume() // Consume the dot

		// Parse table name
		tableName, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		table.Name = tableName
	} else {
		// This is just table name (no database specified)
		table.Name = firstIdent
		// Database will be set to "default" later in processing
	}

	// can have tablename aliasname i.e users u
	// OR tablename aliasname i.e users as u
	if p.peek(0).tokenT == KEYWORD_TOK {
		if p.peek(0).value == "AS" {
			p.consume()
		}
	}

	if p.peek(0).tokenT == IDENT_TOK {
		aliasName, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		table.Alias = aliasName
	}

	return table, nil
}

// parseSelectList parses a select list
func (p *Parser) parseSelectList(selectStmt *SelectStmt) error {
	selectList := &SelectList{
		Expressions: make([]*ValueExpression, 0),
	}

	for p.peek(0).value != "FROM" || p.peek(0).tokenT != SEMICOLON_TOK {
		if p.peek(0).tokenT == COMMA_TOK {
			p.consume()
			continue
		} else if p.peek(0).tokenT == KEYWORD_TOK {
			if p.peek(0).value == "FROM" {
				break
			}
		}

		// can be binary expression, column spec, or aggregate function
		if p.peek(0).tokenT == ASTERISK_TOK {
			// if we encounter an asterisk, we add all columns and no more columns nor expressions can be added
			selectList.Expressions = append(selectList.Expressions, &ValueExpression{
				Value: &Wildcard{},
			})

			p.consume()

			selectStmt.SelectList = selectList

			return nil

		}

		if p.peek(0).tokenT == SEMICOLON_TOK {
			break
		}

		// Parse value expression
		valueExpr, err := p.parseValueExpression()
		if err != nil {
			return err
		}

		selectList.Expressions = append(selectList.Expressions, valueExpr)
	}

	selectStmt.SelectList = selectList

	return nil

}

// parseValueExpression parses a value expression
func (p *Parser) parseValueExpression() (*ValueExpression, error) {
	// A value expression can be a binary expression, column spec, or aggregate function

	if p.peek(1).tokenT == ASTERISK_TOK || p.peek(1).tokenT == PLUS_TOK || p.peek(1).tokenT == MINUS_TOK || p.peek(1).tokenT == DIVIDE_TOK {
		// Parse binary expression
		expr, err := p.parseBinaryExpr(0)
		if err != nil {
			return nil, err
		}

		var alias *Identifier

		// Check for alias
		if p.peek(0).value == "AS" {
			p.consume()

			alias, err = p.parseIdentifier()
			if err != nil {
				return nil, err
			}
		}
		if alias != nil {
			return &ValueExpression{
				Value: expr,
				Alias: alias,
			}, nil
		}

		return &ValueExpression{
			Value: expr,
		}, nil
	}

	if p.peek(0).tokenT == LPAREN_TOK {
		// Subquery
		p.consume()
		subquery, err := p.parseSubquery()
		if err != nil {
			return nil, err
		}

		// Eat )

		var alias *Identifier

		// Check for alias
		if p.peek(0).value == "AS" {
			p.consume()

			alias, err = p.parseIdentifier()
			if err != nil {
				return nil, err
			}
		}
		if alias != nil {
			return &ValueExpression{
				Value: subquery,
				Alias: alias,
			}, nil
		}

		return &ValueExpression{
			Value: subquery,
		}, nil
	}

	switch p.peek(0).tokenT {

	case LITERAL_TOK:
		lit, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}

		var alias *Identifier

		// Check for alias
		if p.peek(0).value == "AS" {
			p.consume()

			alias, err = p.parseIdentifier()
			if err != nil {
				return nil, err
			}
		}
		if alias != nil {
			return &ValueExpression{
				Value: lit,
				Alias: alias,
			}, nil
		}

		return &ValueExpression{
			Value: lit,
		}, nil

	case KEYWORD_TOK:
		switch p.peek(0).value {
		case "COUNT", "MAX", "MIN", "SUM", "AVG",
			"ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
			"LEAD", "LAG", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
			"PERCENT_RANK", "CUME_DIST", "PERCENTILE_CONT", "PERCENTILE_DISC":
			expr, err := p.parseBinaryExpr(0)
			if err != nil {
				return nil, err
			}

			var alias *Identifier

			// Check for alias
			if p.peek(0).value == "AS" {
				p.consume()

				alias, err = p.parseIdentifier()
				if err != nil {
					return nil, err
				}
			}
			if alias != nil {
				return &ValueExpression{
					Value: expr,
					Alias: alias,
				}, nil
			}

			return &ValueExpression{
				Value: expr,
			}, nil
		case "CASE":
			caseExpr, err := p.parseCaseExpr()
			if err != nil {
				return nil, err
			}

			var alias *Identifier

			// Check for alias
			if p.peek(0).value == "AS" {
				p.consume()

				alias, err = p.parseIdentifier()
				if err != nil {
					return nil, err
				}
			}

			if alias != nil {
				return &ValueExpression{
					Value: caseExpr,
					Alias: alias,
				}, nil
			} else {
				return &ValueExpression{
					Value: caseExpr,
				}, nil
			}

		case "UPPER", "LOWER", "CAST",
			"COALESCE", "REVERSE", "ROUND", "POSITION", "LENGTH", "REPLACE", "CONCAT",
			"SUBSTRING", "TRIM", "SYS_DATE", "SYS_TIME", "SYS_TIMESTAMP":
			// Parse system function
			sysFunc, err := p.parseSystemFunc()
			if err != nil {
				return nil, err
			}

			var alias *Identifier

			// Check for alias
			if p.peek(0).value == "AS" {
				p.consume()

				alias, err = p.parseIdentifier()
				if err != nil {
					return nil, err
				}
			}

			if alias != nil {
				return &ValueExpression{
					Value: sysFunc,
					Alias: alias,
				}, nil
			} else {
				return &ValueExpression{
					Value: sysFunc,
				}, nil
			}
		default:
			return nil, errors.New(ErrExpectedKeyword, "expected keyword", nil)
		}

	case AT_TOK:
		if p.peek(1).tokenT == IDENT_TOK {
			p.lexer.tokens[p.pos+1] = Token{
				tokenT: IDENT_TOK,
				value:  "@" + p.peek(1).value.(string),
			}
		}

		p.consume()
		variableName := p.peek(0).value.(string)
		p.consume()
		return &ValueExpression{
			Value: &Variable{
				VariableName: &Identifier{
					Value: variableName,
				},
			},
		}, nil
	case IDENT_TOK:

		// Parse column spec
		colSpec, err := p.parseColumnSpecification()
		if err != nil {
			return nil, err
		}

		var alias *Identifier

		// Check for alias
		if p.peek(0).value == "AS" {
			p.consume()

			// Check if next tok type is a keyword, if so skip alias
			if p.peek(0).tokenT == DATATYPE_TOK {
				p.rewind(1)

				alias = nil

			} else {

				alias, err = p.parseIdentifier()
				if err != nil {
					return nil, err
				}
			}
		}
		if alias != nil {
			return &ValueExpression{
				Value: colSpec,
				Alias: alias,
			}, nil
		}

		return &ValueExpression{
			Value: colSpec,
		}, nil
	default:

		return nil, errors.New(ErrExpectedKeyword, "expected column spec or aggregate function or subquery", nil)
	}

}

// parseCaseExpr parses a CASE expression
func (p *Parser) parseCaseExpr() (*CaseExpr, error) {
	caseExpr := &CaseExpr{}

	p.consume() // Consume CASE

	// Parse when clauses
	for p.peek(0).value != "ELSE" {
		whenClause, err := p.parseWhenClause()
		if err != nil {
			return nil, err
		}

		caseExpr.WhenClauses = append(caseExpr.WhenClauses, whenClause)
	}

	// Eat ELSE
	p.consume()

	// Parse else clause
	elseClause, err := p.parseElseClause()
	if err != nil {
		return nil, err
	}

	caseExpr.ElseClause = elseClause

	// Eat END
	p.consume()

	return caseExpr, nil

}

// parseWhenClause parses a WHEN clause
func (p *Parser) parseWhenClause() (*WhenClause, error) {
	whenClause := &WhenClause{}

	p.consume() // Consume WHEN

	// Parse search condition
	searchCondition, err := p.parseSearchCondition()
	if err != nil {
		return nil, err
	}

	whenClause.Condition = searchCondition

	// Eat THEN
	p.consume()

	// Parse result
	result, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	whenClause.Result = result

	return whenClause, nil

}

// parseElseClause
func (p *Parser) parseElseClause() (*ElseClause, error) {
	elseClause := &ElseClause{}

	// Parse result
	result, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	elseClause.Result = result

	return elseClause, nil
}

// parseSystemFunction parses system function like UPPER, LOWER, CAST, COALESCE, etc
func (p *Parser) parseSystemFunc() (interface{}, error) {
	switch p.peek(0).value {
	case "SUBSTRING":
		substrFunc := &SubstrFunc{}

		p.consume() // Consume SUBSTRING

		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
		}

		p.consume() // Consume (

		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal or identifier", nil)
		}

		// Parse value expression
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		substrFunc.Arg = expr

		// Look for ,
		if p.peek(0).value != "," {
			return nil, errors.New(ErrExpectedComma, "expected ,", nil)
		}

		p.consume() // Consume ,

		if p.peek(0).tokenT != LITERAL_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
		}

		// Parse literal
		startPos, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}

		// Look for ,
		if p.peek(0).value != "," {
			return nil, errors.New(ErrExpectedComma, "expected ,", nil)
		}

		p.consume() // Consume ,

		if p.peek(0).tokenT != LITERAL_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
		}

		leng, err := p.parseLiteral()
		if err != nil {
			return nil, err

		}

		if p.peek(0).tokenT == RPAREN_TOK {
			p.consume()
		}

		substrFunc.StartPos = startPos.(*Literal)
		substrFunc.Length = leng.(*Literal)

		return substrFunc, nil
	case "CONCAT":
		concatFunc := &ConcatFunc{}

		p.consume() // Consume POSITION

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
		}

		// Consume LPAREN
		p.consume()
		for p.peek(0).tokenT != RPAREN_TOK || p.peek(0).tokenT != EOF_TOK {

			if p.peek(0).tokenT == RPAREN_TOK {
				break
			}

			// Parse value expression
			expr, err := p.parseValueExpression()
			if err != nil {
				return nil, err
			}

			concatFunc.Args = append(concatFunc.Args, expr)

			// Look for ,
			if p.peek(0).value == "," {
				p.consume() // Consume ,
			}

		}

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
		}

		p.consume() // Consume RPAREN

		return concatFunc, nil

	case "POSITION":
		positionFunc := &PositionFunc{}

		p.consume() // Consume POSITION

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal or identifier", nil)
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err

		}

		// look for IN
		if p.peek(0).value != "IN" {
			return nil, errors.New(ErrExpectedKeyword, "expected IN", nil)

		}

		p.consume() // Consume IN

		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal or identifier", nil)
		}

		inExpr, err := p.parseValueExpression()
		if err != nil {
			return nil, err

		}

		positionFunc.Arg = expr
		positionFunc.In = inExpr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
		}

		// Consume RPAREN
		p.consume()

		return positionFunc, nil
	case "LENGTH":
		lengthFunc := &LengthFunc{}

		p.consume() // Consume UPPER

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal or identifier", nil)
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		lengthFunc.Arg = expr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
		}

		// Consume RPAREN
		p.consume()

		return lengthFunc, nil
	case "TRIM":
		trimFunc := &TrimFunc{}

		p.consume() // Consume UPPER

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal or identifier", nil)
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		trimFunc.Arg = expr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
		}

		// Consume RPAREN
		p.consume()

		return trimFunc, nil
	case "ROUND":
		roundFunc := &RoundFunc{}

		p.consume() // Consume UPPER

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal or identifier", nil)
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		roundFunc.Arg = expr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
		}

		// Consume RPAREN
		p.consume()

		return roundFunc, nil
	case "REVERSE":
		reverseFunc := &ReverseFunc{}

		p.consume() // Consume UPPER

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal or identifier", nil)
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		reverseFunc.Arg = expr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
		}

		// Consume RPAREN
		p.consume()

		return reverseFunc, nil
	case "UPPER":
		upperFunc := &UpperFunc{}

		p.consume() // Consume UPPER

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal or identifier", nil)
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		upperFunc.Arg = expr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
		}

		// Consume RPAREN
		p.consume()

		return upperFunc, nil
	case "LOWER":
		lowerFunc := &LowerFunc{}

		p.consume() // Consume LOWER

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New(ErrExpectedLiteral, "expected literal or identifier", nil)
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		lowerFunc.Arg = expr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
		}

		// Consume RPAREN
		p.consume()

		return lowerFunc, nil
	case "CAST":

		castFunc := &CastFunc{}

		p.consume() // Consume CAST

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
		}

		// Consume LPAREN

		p.consume() // Consume LPAREN

		// Parse value expression
		valueExpr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		castFunc.Expr = valueExpr

		// Look for AS
		if p.peek(0).value != "AS" {
			return nil, errors.New(ErrExpectedKeyword, "expected AS", nil)

		}

		p.consume() // Consume AS

		if p.peek(0).tokenT != DATATYPE_TOK {
			return nil, errors.New(ErrExpectedKeyword, "expected keyword", nil)
		}

		castFunc.DataType = &Identifier{
			Value: p.peek(0).value.(string),
		}

		p.consume() // Consume datatype

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
		}

		p.consume() // Consume RPAREN

		return castFunc, nil
	case "COALESCE":
		coalesceFunc := &CoalesceFunc{}

		p.consume() // Consume COALESCE

		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
		}

		p.consume() // Consume LPAREN

		for p.peek(0).tokenT != RPAREN_TOK {
			// Parse value expression
			valueExpr, err := p.parseValueExpression()
			if err != nil {
				return nil, err
			}

			coalesceFunc.Args = append(coalesceFunc.Args, valueExpr)

			if p.peek(0).tokenT == COMMA_TOK {
				p.consume()
			}
		}

		p.consume() // Consume RPAREN

		// The last value expression is the default value
		coalesceFunc.Value = coalesceFunc.Args[len(coalesceFunc.Args)-1]

		// remove the last value expression from the args
		coalesceFunc.Args = coalesceFunc.Args[:len(coalesceFunc.Args)-1]

		return coalesceFunc, nil
	case "SYS_DATE":
		return &SysDate{}, nil
	case "SYS_TIME":
		return &SysTime{}, nil
	case "SYS_TIMESTAMP":
		return &SysTimestamp{}, nil
	case "GENERATE_UUID":
		return &GenUUID{}, nil
	default:
		return nil, errors.New(ErrExpectedKeyword, "expected system function", nil)

	}
}

// parseColumnSpecification parses a column specification
func (p *Parser) parseColumnSpecification() (*ColumnSpecification, error) {

	// A column specification is in the form of table_name.column_name or column_name depending on FROM

	// Parse column name
	columnName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}

	if len(strings.Split(columnName.Value, ".")) == 2 {
		tableName := &Identifier{
			Value: strings.Split(columnName.Value, ".")[0],
		}
		columnName = &Identifier{
			Value: strings.Split(columnName.Value, ".")[1],
		}
		return &ColumnSpecification{
			TableName:  tableName,
			ColumnName: columnName,
		}, nil
	}

	return &ColumnSpecification{
		ColumnName: columnName,
	}, nil
}

// parseIdentifier parses an identifier
func (p *Parser) parseIdentifier() (*Identifier, error) {
	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New(ErrExpectedIdentifier, "expected identifier", nil)
	}

	ident := &Identifier{
		Value: p.peek(0).value.(string),
	}

	p.consume()

	return ident, nil

}

// parseAggregateFunc parses an aggregate function
func (p *Parser) parseAggregateFunc() (*AggregateFunc, error) {
	// Eat aggregate function
	aggFunc := &AggregateFunc{FuncName: p.peek(0).value.(string)}

	p.consume()

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, errors.New(ErrExpectedLeftParen, "expected (", nil)
	}

	p.consume() // Consume (

	for p.peek(0).tokenT != RPAREN_TOK && (p.peek(0).tokenT != SEMICOLON_TOK || p.peek(0).tokenT != COMMA_TOK || p.peek(0).value != "FROM") {
		// Catch nested aggregate functions, binary expressions, column specs, and literals
		if p.peek(0).tokenT == KEYWORD_TOK {
			switch p.peek(0).value {

			case "AVG", "COUNT", "MAX", "MIN", "SUM":
				// Parse aggregate function
				innerAggFunc, err := p.parseAggregateFunc()
				if err != nil {
					return nil, err
				}

				aggFunc.Args = append(aggFunc.Args, innerAggFunc)
			default:
				return nil, errors.New(ErrExpectedKeyword, "expected aggregate function", nil)
			}
		} else if p.peek(0).tokenT == LPAREN_TOK {
			// Parse binary expression
			expr, err := p.parseBinaryExpr(0)
			if err != nil {
				return nil, err
			}

			aggFunc.Args = append(aggFunc.Args, expr)

		} else if p.peek(0).tokenT == IDENT_TOK {
			if p.peek(1).tokenT == ASTERISK_TOK || p.peek(1).tokenT == PLUS_TOK || p.peek(1).tokenT == MINUS_TOK || p.peek(1).tokenT == DIVIDE_TOK {
				// Parse binary expression
				expr, err := p.parseBinaryExpr(0)
				if err != nil {
					return nil, err
				}

				aggFunc.Args = append(aggFunc.Args, expr)
			} else {
				// Parse column spec
				columnSpec, err := p.parseColumnSpecification()
				if err != nil {
					return nil, err
				}

				aggFunc.Args = append(aggFunc.Args, columnSpec)
			}
		} else if p.peek(0).tokenT == ASTERISK_TOK {
			aggFunc.Args = append(aggFunc.Args, &Wildcard{})

			p.consume()

		} else {
			return nil, errors.New(ErrExpectedKeyword, "expected aggregate function, binary expression, or column spec", nil)
		}

	}

	if p.peek(0).tokenT != RPAREN_TOK {
		return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
	}

	p.consume()

	return aggFunc, nil
}

// getPrecendence returns the precedence of an arithmetic operator
func (p *Parser) getPrecedence(tokenT TokenType) int {
	switch tokenT {
	case ASTERISK_TOK, DIVIDE_TOK:
		return 2
	case PLUS_TOK, MINUS_TOK:
		return 1
	default:
		return 0
	}
}

// parseBinaryExpr parses a binary expression
func (p *Parser) parseBinaryExpr(precedence int) (interface{}, error) {
	left, err := p.parsePrimaryExpr()
	if err != nil {
		return nil, err
	}

	for {
		nextPrecedence := p.getPrecedence(p.peek(0).tokenT)

		if nextPrecedence <= precedence {
			return left, nil
		}

		op := p.peek(0).value

		p.consume()

		right, err := p.parseBinaryExpr(nextPrecedence)
		if err != nil {
			return nil, err
		}

		left = &BinaryExpression{Left: left, Op: getBinaryExpressionOperator(op.(string)), Right: right}
	}
}

// parsePrimaryExpr parses a primary expression
func (p *Parser) parsePrimaryExpr() (interface{}, error) {
	if p.peek(0).tokenT == LPAREN_TOK {
		p.consume()

		expr, err := p.parseBinaryExpr(0)
		if err != nil {
			return nil, err
		}

		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New(ErrExpectedRightParen, "expected )", nil)
		}

		p.consume()

		return expr, nil
	}

	return p.parseUnaryExpr()
}

// parseUnaryExpr parses a unary expression
func (p *Parser) parseUnaryExpr() (interface{}, error) {
	if p.peek(0).tokenT == PLUS_TOK || p.peek(0).tokenT == MINUS_TOK || p.peek(0).tokenT == ASTERISK_TOK || p.peek(0).tokenT == DIVIDE_TOK {
		op := p.peek(0).value.(string)

		p.consume()

		expr, err := p.parsePrimaryExpr()
		if err != nil {
			return nil, err
		}

		return &UnaryExpr{Op: op, Expr: expr}, nil
	}

	switch p.peek(0).tokenT {
	case LITERAL_TOK:
		return p.parseLiteral()
	case IDENT_TOK:
		return p.parseColumnSpecification()
	case KEYWORD_TOK:
		switch p.peek(0).value {
		case "AVG", "COUNT", "MAX", "MIN", "SUM":
			return p.parseAggregateFunc()
		default:
			return nil, errors.New(ErrExpectedKeyword, "expected aggregate function", nil)
		}
	default:
		return nil, errors.New(ErrExpectedLiteral, "expected literal or column spec", nil)
	}
}

// parseLiteral parses a literal
func (p *Parser) parseLiteral() (interface{}, error) {
	if p.peek(0).tokenT != LITERAL_TOK {
		return nil, errors.New(ErrExpectedLiteral, "expected literal", nil)
	}

	lit := p.peek(0).value

	p.consume()

	return &Literal{Value: lit}, nil
}

// parseSubquery parses a subquery
func (p *Parser) parseSubquery() (*ValueExpression, error) {
	// Parse select statement
	selectStmt, err := p.parseSelectStmt()
	if err != nil {
		return nil, err
	}

	return &ValueExpression{
		Value: selectStmt,
	}, nil

}

// parseHavingClause parses a HAVING clause
func (p *Parser) parseHavingClause() (*HavingClause, error) {
	havingClause := &HavingClause{
		SearchCondition: make([]interface{}, 0),
	}

	var err error

	// Eat HAVING
	p.consume()

	// Parse search condition
	havingClause.SearchCondition, err = p.parseSearchCondition()
	if err != nil {
		return nil, err
	}

	return havingClause, nil
}

// parseCreateTableClause parses additional clauses after the column definitions
// Returns true if a clause was parsed, false if no clause was found, error if parsing failed
func (p *Parser) parseCreateTableClause(createTableStmt *CreateTableStmt) (bool, error) {
	if p.peek(0).tokenT != KEYWORD_TOK {
		return false, nil // Not a keyword, end of clauses
	}

	clause := strings.ToUpper(p.peek(0).value.(string))

	switch clause {
	case "STORAGE":
		return true, p.parseStorageClause(createTableStmt)
	case "PARTITION":
		return true, p.parsePartitionByClause(createTableStmt)
	case "ORDER":
		return true, p.parseCreateTableOrderByClause(createTableStmt)
	case "SETTINGS":
		return true, p.parseSettingsClause(createTableStmt)
	default:
		return false, nil // Unknown clause, end of parsing
	}
}

// parseStorageClause parses the STORAGE clause
func (p *Parser) parseStorageClause(createTableStmt *CreateTableStmt) error {
	p.consume() // Consume STORAGE

	if p.peek(0).tokenT != IDENT_TOK {
		return errors.New(ErrExpectedIdentifier, "expected storage engine name after STORAGE", nil)
	}

	storageEngine := p.peek(0).value.(string)
	createTableStmt.StorageEngine = &Identifier{Value: storageEngine}
	p.consume() // Consume storage engine name

	return nil
}

// parsePartitionByClause parses the PARTITION BY clause
func (p *Parser) parsePartitionByClause(createTableStmt *CreateTableStmt) error {
	p.consume() // Consume PARTITION

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "BY" {
		return errors.New(ErrExpectedKeyword, "expected BY after PARTITION", nil)
	}

	p.consume() // Consume BY

	if p.peek(0).tokenT != LPAREN_TOK {
		return errors.New(ErrExpectedLeftParen, "expected ( after PARTITION BY", nil)
	}

	p.consume() // Consume (

	// Parse partition columns
	var partitionColumns []*Identifier

	for {
		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT != IDENT_TOK {
			return errors.New(ErrExpectedIdentifier, "expected column name in PARTITION BY", nil)
		}

		columnName := p.peek(0).value.(string)
		partitionColumns = append(partitionColumns, &Identifier{Value: columnName})
		p.consume() // Consume column name

		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT != COMMA_TOK {
			return errors.New(ErrExpectedComma, "expected , between partition columns", nil)
		}

		p.consume() // Consume comma
	}

	p.consume() // Consume )

	createTableStmt.PartitionBy = partitionColumns
	return nil
}

// parseCreateTableOrderByClause parses the ORDER BY clause for CREATE TABLE
func (p *Parser) parseCreateTableOrderByClause(createTableStmt *CreateTableStmt) error {
	p.consume() // Consume ORDER

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "BY" {
		return errors.New(ErrExpectedKeyword, "expected BY after ORDER", nil)
	}

	p.consume() // Consume BY

	if p.peek(0).tokenT != LPAREN_TOK {
		return errors.New(ErrExpectedLeftParen, "expected ( after ORDER BY", nil)
	}

	p.consume() // Consume (

	// Parse order columns
	var orderColumns []*Identifier

	for {
		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT != IDENT_TOK {
			return errors.New(ErrExpectedIdentifier, "expected column name in ORDER BY", nil)
		}

		columnName := p.peek(0).value.(string)
		orderColumns = append(orderColumns, &Identifier{Value: columnName})
		p.consume() // Consume column name

		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT != COMMA_TOK {
			return errors.New(ErrExpectedComma, "expected , between order columns", nil)
		}

		p.consume() // Consume comma
	}

	p.consume() // Consume )

	createTableStmt.OrderBy = orderColumns
	return nil
}

// parseSettingsClause parses the SETTINGS clause
func (p *Parser) parseSettingsClause(createTableStmt *CreateTableStmt) error {
	p.consume() // Consume SETTINGS

	// Initialize Settings map if not already done
	if createTableStmt.Settings == nil {
		createTableStmt.Settings = make(map[string]interface{})
	}

	// Parse settings as key=value pairs directly using tokens
	// Format: key1=value1,key2=value2

	for {
		// Check for end of statement
		if p.peek(0).tokenT == SEMICOLON_TOK || p.peek(0).tokenT == EOF_TOK {
			break
		}

		// Check if this is the start of another clause
		if p.peek(0).tokenT == KEYWORD_TOK {
			nextKeyword := strings.ToUpper(p.peek(0).value.(string))
			if nextKeyword == "STORAGE" || nextKeyword == "PARTITION" || nextKeyword == "ORDER" {
				break
			}
		}

		// Parse key=value pair
		if p.peek(0).tokenT != IDENT_TOK {
			break
		}

		key := p.peek(0).value.(string)
		p.consume() // Consume key

		if p.peek(0).tokenT != COMPARISON_TOK || p.peek(0).value != "=" {
			return errors.New(ErrExpectedEquals, "expected = after setting key", nil)
		}
		p.consume() // Consume =

		// Parse value (can be literal or identifier)
		var value interface{}
		if p.peek(0).tokenT == LITERAL_TOK {
			rawValue := p.peek(0).value
			// Convert uint64 to int for consistency
			if uint64Val, ok := rawValue.(uint64); ok {
				value = int(uint64Val)
			} else {
				value = rawValue
			}
		} else if p.peek(0).tokenT == IDENT_TOK {
			valueStr := p.peek(0).value.(string)
			// Try to convert to appropriate type
			if parsedValue, err := p.parseSettingValue(valueStr); err == nil {
				value = parsedValue
			} else {
				value = valueStr
			}
		} else {
			return errors.New(ErrExpectedLiteral, "expected value after =", nil)
		}
		p.consume() // Consume value

		createTableStmt.Settings[key] = value

		// Check for comma separator
		if p.peek(0).tokenT == COMMA_TOK {
			p.consume() // Consume comma
		} else {
			break
		}
	}

	return nil
}

// parseSettingValue converts a string value to appropriate type
func (p *Parser) parseSettingValue(valueStr string) (any, error) {
	// Try boolean
	if valueStr == "true" {
		return true, nil
	}
	if valueStr == "false" {
		return false, nil
	}

	// Try integer
	if intVal, err := strconv.Atoi(valueStr); err == nil {
		return intVal, nil
	}

	// Try float
	if floatVal, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return floatVal, nil
	}

	// Default to string
	return valueStr, nil
}
