package parser

import "github.com/gear6io/ranger/pkg/errors"

// Parser-specific error codes
var (
	// Syntax errors
	ErrEmptyStatement        = errors.MustNewCode("parser.syntax.empty_statement")
	ErrInvalidStatement      = errors.MustNewCode("parser.syntax.invalid_statement")
	ErrMissingSemicolon      = errors.MustNewCode("parser.syntax.missing_semicolon")
	ErrExpectedKeyword       = errors.MustNewCode("parser.syntax.expected_keyword")
	ErrExpectedIdentifier    = errors.MustNewCode("parser.syntax.expected_identifier")
	ErrExpectedLiteral       = errors.MustNewCode("parser.syntax.expected_literal")
	ErrExpectedAtSymbol      = errors.MustNewCode("parser.syntax.expected_at_symbol")
	ErrExpectedFetchStatus   = errors.MustNewCode("parser.syntax.expected_fetch_status")
	ErrExpectedEquals        = errors.MustNewCode("parser.syntax.expected_equals")
	ErrExpectedZero          = errors.MustNewCode("parser.syntax.expected_zero")
	ErrExpectedBegin         = errors.MustNewCode("parser.syntax.expected_begin")
	ErrExpectedEnd           = errors.MustNewCode("parser.syntax.expected_end")
	ErrExpectedNext          = errors.MustNewCode("parser.syntax.expected_next")
	ErrExpectedFrom          = errors.MustNewCode("parser.syntax.expected_from")
	ErrExpectedInto          = errors.MustNewCode("parser.syntax.expected_into")
	ErrExpectedCursorVar     = errors.MustNewCode("parser.syntax.expected_cursor_variable")
	ErrExpectedDataType      = errors.MustNewCode("parser.syntax.expected_data_type")
	ErrExpectedCursor        = errors.MustNewCode("parser.syntax.expected_cursor")
	ErrExpectedFor           = errors.MustNewCode("parser.syntax.expected_for")
	ErrExpectedUserOrTable   = errors.MustNewCode("parser.syntax.expected_user_or_table")
	ErrExpectedLeftParen     = errors.MustNewCode("parser.syntax.expected_left_paren")
	ErrExpectedRightParen    = errors.MustNewCode("parser.syntax.expected_right_paren")
	ErrExpectedComma         = errors.MustNewCode("parser.syntax.expected_comma")
	ErrExpectedAddDropSet    = errors.MustNewCode("parser.syntax.expected_add_drop_set")
	ErrExpectedPasswordUser  = errors.MustNewCode("parser.syntax.expected_password_username")
	ErrExpectedSet           = errors.MustNewCode("parser.syntax.expected_set")
	ErrExpectedDatabases     = errors.MustNewCode("parser.syntax.expected_databases_tables_users")
	ErrExpectedSelectInsert  = errors.MustNewCode("parser.syntax.expected_select_insert_update_delete")
	ErrExpectedPrivilege     = errors.MustNewCode("parser.syntax.expected_privilege")
	ErrExpectedOn            = errors.MustNewCode("parser.syntax.expected_on")
	ErrExpectedStarOrTable   = errors.MustNewCode("parser.syntax.expected_star_or_table")
	ErrExpectedDotAfterDB    = errors.MustNewCode("parser.syntax.expected_dot_after_database")
	ErrExpectedDatabaseOrStar = errors.MustNewCode("parser.syntax.expected_database_or_star")
	ErrExpectedStarOrTableAfterDot = errors.MustNewCode("parser.syntax.expected_star_or_table_after_dot")
	ErrExpectedDatabaseOrTable = errors.MustNewCode("parser.syntax.expected_database_or_table")
	ErrExpectedIdentifierAfterDot = errors.MustNewCode("parser.syntax.expected_identifier_after_dot")
	ErrExpectedExistsAfterIf = errors.MustNewCode("parser.syntax.expected_exists_after_if")
	ErrExpectedValues        = errors.MustNewCode("parser.syntax.expected_values")
	ErrExpectedLiteralOrNull = errors.MustNewCode("parser.syntax.expected_literal_or_null")

	// Validation errors
	ErrTableNameRequired     = errors.MustNewCode("parser.validation.table_name_required")
	ErrTableNameEmpty        = errors.MustNewCode("parser.validation.table_name_empty")
	ErrDatabaseNameEmpty     = errors.MustNewCode("parser.validation.database_name_empty")
	ErrDatabaseNameEmptyIfSpecified = errors.MustNewCode("parser.validation.database_name_empty_if_specified")

	// Parsing errors
	ErrUnexpectedDataType    = errors.MustNewCode("parser.parse.unexpected_data_type")
)
