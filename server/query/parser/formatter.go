package parser

import (
	"fmt"
	"strings"
)

// FormatQuery formats a parsed SQL AST into a properly formatted SQL string
// with uppercase keywords and consistent spacing/indentation
func FormatQuery(node Node) string {
	if node == nil {
		return ""
	}

	switch stmt := node.(type) {
	case *SelectStmt:
		return formatSelectStmt(stmt)
	case *CreateTableStmt:
		return formatCreateTableStmt(stmt)
	case *InsertStmt:
		return formatInsertStmt(stmt)
	case *UpdateStmt:
		return formatUpdateStmt(stmt)
	case *DeleteStmt:
		return formatDeleteStmt(stmt)
	case *CreateDatabaseStmt:
		return formatCreateDatabaseStmt(stmt)
	case *DropTableStmt:
		return formatDropTableStmt(stmt)
	case *ShowStmt:
		return formatShowStmt(stmt)
	case *UseStmt:
		return formatUseStmt(stmt)
	case *ExplainStmt:
		return formatExplainStmt(stmt)
	default:
		return fmt.Sprintf("-- Unsupported statement type: %T", node)
	}
}

// formatSelectStmt formats a SELECT statement
func formatSelectStmt(stmt *SelectStmt) string {
	var parts []string

	// SELECT clause
	selectClause := "SELECT"
	if stmt.Distinct {
		selectClause += " DISTINCT"
	}
	parts = append(parts, selectClause)

	// Select list
	if stmt.SelectList != nil {
		parts = append(parts, formatSelectList(stmt.SelectList))
	}

	// FROM clause
	if stmt.TableExpression != nil {
		parts = append(parts, "FROM")
		parts = append(parts, formatTableExpression(stmt.TableExpression))
	}

	// WHERE clause
	if stmt.TableExpression != nil && stmt.TableExpression.WhereClause != nil {
		parts = append(parts, "WHERE")
		parts = append(parts, formatWhereClause(stmt.TableExpression.WhereClause))
	}

	// GROUP BY clause
	if stmt.TableExpression != nil && stmt.TableExpression.GroupByClause != nil {
		parts = append(parts, "GROUP BY")
		parts = append(parts, formatGroupByClause(stmt.TableExpression.GroupByClause))
	}

	// HAVING clause
	if stmt.TableExpression != nil && stmt.TableExpression.HavingClause != nil {
		parts = append(parts, "HAVING")
		parts = append(parts, formatHavingClause(stmt.TableExpression.HavingClause))
	}

	// ORDER BY clause
	if stmt.TableExpression != nil && stmt.TableExpression.OrderByClause != nil {
		parts = append(parts, "ORDER BY")
		parts = append(parts, formatOrderByClause(stmt.TableExpression.OrderByClause))
	}

	// LIMIT clause
	if stmt.TableExpression != nil && stmt.TableExpression.LimitClause != nil {
		parts = append(parts, "LIMIT")
		parts = append(parts, formatLimitClause(stmt.TableExpression.LimitClause))
	}

	// UNION clause
	if stmt.Union != nil {
		parts = append(parts, "UNION")
		if stmt.UnionAll {
			parts = append(parts, "ALL")
		}
		parts = append(parts, formatSelectStmt(stmt.Union))
	}

	return strings.Join(parts, " ") + ";"
}

// formatSelectList formats the SELECT list
func formatSelectList(list *SelectList) string {
	if list == nil {
		return "*"
	}

	var items []string
	for _, item := range list.Expressions {
		items = append(items, formatSelectItem(item))
	}

	return strings.Join(items, ", ")
}

// formatSelectItem formats a single SELECT item
func formatSelectItem(item *ValueExpression) string {
	if item == nil {
		return ""
	}

	var parts []string

	// Expression
	if item.Value != nil {
		parts = append(parts, formatExpression(item.Value))
	}

	// Alias
	if item.Alias != nil {
		parts = append(parts, "AS")
		parts = append(parts, formatIdentifier(item.Alias))
	}

	return strings.Join(parts, " ")
}

// formatTableExpression formats a table expression
func formatTableExpression(expr *TableExpression) string {
	if expr == nil {
		return ""
	}

	var parts []string

	// FROM clause
	if expr.FromClause != nil {
		parts = append(parts, formatFromClause(expr.FromClause))
	}

	return strings.Join(parts, " ")
}

// formatFromClause formats a FROM clause
func formatFromClause(from *FromClause) string {
	if from == nil || len(from.Tables) == 0 {
		return ""
	}

	var parts []string
	for i, table := range from.Tables {
		if i > 0 {
			parts = append(parts, ",")
		}
		parts = append(parts, formatTable(table))
	}

	return strings.Join(parts, " ")
}

// formatTable formats a table reference
func formatTable(table *Table) string {
	if table == nil {
		return ""
	}

	var parts []string

	// Database name (optional)
	if table.Database != nil {
		parts = append(parts, formatIdentifier(table.Database))
		parts = append(parts, ".")
	}

	// Table name
	if table.Name != nil {
		parts = append(parts, formatIdentifier(table.Name))
	}

	// Alias
	if table.Alias != nil {
		parts = append(parts, "AS")
		parts = append(parts, formatIdentifier(table.Alias))
	}

	return strings.Join(parts, " ")
}

// formatWhereClause formats a WHERE clause
func formatWhereClause(where *WhereClause) string {
	if where == nil || where.SearchCondition == nil {
		return ""
	}

	return formatSearchCondition(where.SearchCondition)
}

// formatGroupByClause formats a GROUP BY clause
func formatGroupByClause(groupBy *GroupByClause) string {
	if groupBy == nil {
		return ""
	}

	var parts []string
	for _, expr := range groupBy.GroupByExpressions {
		parts = append(parts, formatExpression(expr))
	}

	return strings.Join(parts, ", ")
}

// formatHavingClause formats a HAVING clause
func formatHavingClause(having *HavingClause) string {
	if having == nil || having.SearchCondition == nil {
		return ""
	}

	return formatSearchCondition(having.SearchCondition)
}

// formatOrderByClause formats an ORDER BY clause
func formatOrderByClause(orderBy *OrderByClause) string {
	if orderBy == nil {
		return ""
	}

	var parts []string
	for _, item := range orderBy.OrderByExpressions {
		parts = append(parts, formatOrderByItem(item))
	}

	return strings.Join(parts, ", ")
}

// formatOrderByItem formats an ORDER BY item
func formatOrderByItem(item *ValueExpression) string {
	if item == nil {
		return ""
	}

	var parts []string

	// Expression
	if item.Value != nil {
		parts = append(parts, formatExpression(item.Value))
	}

	// Order (using the clause's order)
	// Note: The current AST structure doesn't support per-item ordering
	// This is a limitation of the current parser

	return strings.Join(parts, " ")
}

// formatLimitClause formats a LIMIT clause
func formatLimitClause(limit *LimitClause) string {
	if limit == nil {
		return ""
	}

	var parts []string

	if limit.Count != nil {
		parts = append(parts, formatExpression(limit.Count))
	}

	if limit.Offset != nil {
		parts = append(parts, "OFFSET")
		parts = append(parts, formatExpression(limit.Offset))
	}

	return strings.Join(parts, " ")
}

// formatExpression formats an expression
func formatExpression(expr interface{}) string {
	if expr == nil {
		return ""
	}

	switch e := expr.(type) {
	case *Identifier:
		return formatIdentifier(e)
	case *Literal:
		return formatLiteral(e)
	case *BinaryExpression:
		return formatBinaryExpression(e)
	case *ValueExpression:
		return formatValueExpression(e)
	case *AggregateFunc:
		return formatAggregateFunc(e)
	case *UnaryExpr:
		return formatUnaryExpr(e)
	case *NotExpr:
		return formatNotExpr(e)
	case *BetweenPredicate:
		return formatBetweenPredicate(e)
	case *InPredicate:
		return formatInPredicate(e)
	case *LikePredicate:
		return formatLikePredicate(e)
	case *IsPredicate:
		return formatIsPredicate(e)
	case *ComparisonPredicate:
		return formatComparisonPredicate(e)
	case *LogicalCondition:
		return formatLogicalCondition(e)
	default:
		return fmt.Sprintf("%v", expr)
	}
}

// formatIdentifier formats an identifier
func formatIdentifier(id *Identifier) string {
	if id == nil {
		return ""
	}
	return id.Value
}

// formatLiteral formats a literal
func formatLiteral(lit *Literal) string {
	if lit == nil {
		return ""
	}

	switch v := lit.Value.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// formatBinaryExpression formats a binary expression
func formatBinaryExpression(expr *BinaryExpression) string {
	if expr == nil {
		return ""
	}

	left := formatExpression(expr.Left)
	right := formatExpression(expr.Right)

	// Get operator string from the enum
	var operator string
	switch expr.Op {
	case OP_PLUS:
		operator = "+"
	case OP_MINUS:
		operator = "-"
	case OP_MULT:
		operator = "*"
	case OP_DIV:
		operator = "/"
	default:
		operator = "?"
	}

	return fmt.Sprintf("%s %s %s", left, operator, right)
}

// formatValueExpression formats a value expression
func formatValueExpression(expr *ValueExpression) string {
	if expr == nil {
		return ""
	}

	var parts []string

	// Value
	if expr.Value != nil {
		parts = append(parts, formatExpression(expr.Value))
	}

	// Alias
	if expr.Alias != nil {
		parts = append(parts, "AS")
		parts = append(parts, formatIdentifier(expr.Alias))
	}

	return strings.Join(parts, " ")
}

// formatAggregateFunc formats an aggregate function
func formatAggregateFunc(fn *AggregateFunc) string {
	if fn == nil {
		return ""
	}

	var parts []string

	// Function name (uppercase)
	parts = append(parts, strings.ToUpper(fn.FuncName))

	// Arguments
	if len(fn.Args) > 0 {
		args := make([]string, len(fn.Args))
		for i, arg := range fn.Args {
			args[i] = formatExpression(arg)
		}
		parts = append(parts, "("+strings.Join(args, ", ")+")")
	} else {
		parts = append(parts, "()")
	}

	return strings.Join(parts, "")
}

// formatUnaryExpr formats a unary expression
func formatUnaryExpr(expr *UnaryExpr) string {
	if expr == nil {
		return ""
	}

	op := strings.ToUpper(expr.Op)
	operand := formatExpression(expr.Expr)

	return fmt.Sprintf("%s %s", op, operand)
}

// formatNotExpr formats a NOT expression
func formatNotExpr(expr *NotExpr) string {
	if expr == nil {
		return ""
	}

	operand := formatExpression(expr.Expr)
	return fmt.Sprintf("NOT %s", operand)
}

// formatBetweenPredicate formats a BETWEEN predicate
func formatBetweenPredicate(expr *BetweenPredicate) string {
	if expr == nil {
		return ""
	}

	left := formatExpression(expr.Left)
	lower := formatExpression(expr.Lower)
	upper := formatExpression(expr.Upper)

	return fmt.Sprintf("%s BETWEEN %s AND %s", left, lower, upper)
}

// formatInPredicate formats an IN predicate
func formatInPredicate(expr *InPredicate) string {
	if expr == nil {
		return ""
	}

	left := formatExpression(expr.Left)
	parts := []string{left, "IN", "("}

	values := make([]string, len(expr.Values))
	for i, val := range expr.Values {
		values[i] = formatExpression(val)
	}
	parts = append(parts, strings.Join(values, ", "))
	parts = append(parts, ")")

	return strings.Join(parts, " ")
}

// formatLikePredicate formats a LIKE predicate
func formatLikePredicate(expr *LikePredicate) string {
	if expr == nil {
		return ""
	}

	left := formatExpression(expr.Left)
	pattern := formatExpression(expr.Pattern)

	return fmt.Sprintf("%s LIKE %s", left, pattern)
}

// formatIsPredicate formats an IS predicate
func formatIsPredicate(expr *IsPredicate) string {
	if expr == nil {
		return ""
	}

	left := formatExpression(expr.Left)
	if expr.Null {
		return fmt.Sprintf("%s IS NULL", left)
	}
	return fmt.Sprintf("%s IS NOT NULL", left)
}

// formatComparisonPredicate formats a comparison predicate
func formatComparisonPredicate(expr *ComparisonPredicate) string {
	if expr == nil {
		return ""
	}

	left := formatExpression(expr.Left)
	right := formatExpression(expr.Right)

	// Get operator string from the enum
	var operator string
	switch expr.Op {
	case OP_EQ:
		operator = "="
	case OP_NEQ:
		operator = "<>"
	case OP_LT:
		operator = "<"
	case OP_LTE:
		operator = "<="
	case OP_GT:
		operator = ">"
	case OP_GTE:
		operator = ">="
	default:
		operator = "?"
	}

	return fmt.Sprintf("%s %s %s", left, operator, right)
}

// formatLogicalCondition formats a logical condition
func formatLogicalCondition(expr *LogicalCondition) string {
	if expr == nil {
		return ""
	}

	left := formatExpression(expr.Left)
	right := formatExpression(expr.Right)

	// Get operator string from the enum
	var operator string
	switch expr.Op {
	case OP_AND:
		operator = "AND"
	case OP_OR:
		operator = "OR"
	case OP_NOT:
		operator = "NOT"
	default:
		operator = "?"
	}

	return fmt.Sprintf("%s %s %s", left, operator, right)
}

// formatSearchCondition formats a search condition
func formatSearchCondition(condition interface{}) string {
	if condition == nil {
		return ""
	}

	switch c := condition.(type) {
	case *BinaryExpression:
		return formatBinaryExpression(c)
	case *ComparisonPredicate:
		return formatComparisonPredicate(c)
	case *InPredicate:
		return formatInPredicate(c)
	case *LikePredicate:
		return formatLikePredicate(c)
	case *BetweenPredicate:
		return formatBetweenPredicate(c)
	case *IsPredicate:
		return formatIsPredicate(c)
	case *LogicalCondition:
		return formatLogicalCondition(c)
	default:
		return fmt.Sprintf("%v", condition)
	}
}

// formatCreateTableStmt formats a CREATE TABLE statement
func formatCreateTableStmt(stmt *CreateTableStmt) string {
	if stmt == nil {
		return ""
	}

	var parts []string
	parts = append(parts, "CREATE TABLE")

	if stmt.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	parts = append(parts, formatTableIdentifier(stmt.TableName))

	if stmt.TableSchema != nil {
		parts = append(parts, "(")
		parts = append(parts, formatTableSchema(stmt.TableSchema))
		parts = append(parts, ")")
	}

	if stmt.Compress {
		parts = append(parts, "COMPRESS")
	}

	if stmt.Encrypt {
		parts = append(parts, "ENCRYPT")
		if stmt.EncryptKey != nil {
			parts = append(parts, "KEY")
			parts = append(parts, formatLiteral(stmt.EncryptKey))
		}
	}

	return strings.Join(parts, " ") + ";"
}

// formatTableSchema formats a table schema
func formatTableSchema(schema *TableSchema) string {
	if schema == nil {
		return ""
	}

	var parts []string

	// Columns - iterate over the map
	columnNames := make([]string, 0, len(schema.ColumnDefinitions))
	for colName := range schema.ColumnDefinitions {
		columnNames = append(columnNames, colName)
	}

	for i, colName := range columnNames {
		if i > 0 {
			parts = append(parts, ",")
		}
		colDef := schema.ColumnDefinitions[colName]
		parts = append(parts, formatColumnDefinition(colName, colDef))
	}

	return strings.Join(parts, " ")
}

// formatColumnDefinition formats a column definition
func formatColumnDefinition(colName string, col *ColumnDefinition) string {
	if col == nil {
		return ""
	}

	var parts []string

	// Column name
	parts = append(parts, colName)

	// Data type
	parts = append(parts, strings.ToUpper(col.DataType))
	if col.Length > 0 {
		parts = append(parts, fmt.Sprintf("(%d)", col.Length))
	}
	if col.Precision > 0 {
		parts = append(parts, fmt.Sprintf("(%d,%d)", col.Precision, col.Scale))
	}

	// Constraints
	if col.NotNull {
		parts = append(parts, "NOT NULL")
	}

	if col.DefaultValue != "" {
		parts = append(parts, "DEFAULT")
		parts = append(parts, col.DefaultValue)
	}

	return strings.Join(parts, " ")
}

// Note: Foreign key support is not implemented in the current AST structure
// This function is a placeholder for future implementation
func formatForeignKey(fk interface{}) string {
	return "-- Foreign key support not implemented"
}

// formatInsertStmt formats an INSERT statement
func formatInsertStmt(stmt *InsertStmt) string {
	if stmt == nil {
		return ""
	}

	var parts []string
	parts = append(parts, "INSERT INTO")
	parts = append(parts, formatTableIdentifier(stmt.TableName))

	// Column names
	if len(stmt.ColumnNames) > 0 {
		parts = append(parts, "(")
		colNames := make([]string, len(stmt.ColumnNames))
		for i, col := range stmt.ColumnNames {
			colNames[i] = formatIdentifier(col)
		}
		parts = append(parts, strings.Join(colNames, ", "))
		parts = append(parts, ")")
	}

	// VALUES
	parts = append(parts, "VALUES")
	valueLists := make([]string, len(stmt.Values))
	for i, valueList := range stmt.Values {
		values := make([]string, len(valueList))
		for j, val := range valueList {
			values[j] = formatExpression(val)
		}
		valueLists[i] = "(" + strings.Join(values, ", ") + ")"
	}
	parts = append(parts, strings.Join(valueLists, ", "))

	return strings.Join(parts, " ") + ";"
}

// formatUpdateStmt formats an UPDATE statement
func formatUpdateStmt(stmt *UpdateStmt) string {
	if stmt == nil {
		return ""
	}

	var parts []string
	parts = append(parts, "UPDATE")
	parts = append(parts, formatTableIdentifier(stmt.TableName))

	// SET clause
	if len(stmt.SetClause) > 0 {
		parts = append(parts, "SET")
		setItems := make([]string, len(stmt.SetClause))
		for i, item := range stmt.SetClause {
			setItems[i] = fmt.Sprintf("%s = %s",
				formatIdentifier(item.Column),
				formatExpression(item.Value))
		}
		parts = append(parts, strings.Join(setItems, ", "))
	}

	// WHERE clause
	if stmt.WhereClause != nil {
		parts = append(parts, "WHERE")
		parts = append(parts, formatWhereClause(stmt.WhereClause))
	}

	return strings.Join(parts, " ") + ";"
}

// formatDeleteStmt formats a DELETE statement
func formatDeleteStmt(stmt *DeleteStmt) string {
	if stmt == nil {
		return ""
	}

	var parts []string
	parts = append(parts, "DELETE FROM")
	parts = append(parts, formatTableIdentifier(stmt.TableName))

	// WHERE clause
	if stmt.WhereClause != nil {
		parts = append(parts, "WHERE")
		parts = append(parts, formatWhereClause(stmt.WhereClause))
	}

	return strings.Join(parts, " ") + ";"
}

// formatCreateDatabaseStmt formats a CREATE DATABASE statement
func formatCreateDatabaseStmt(stmt *CreateDatabaseStmt) string {
	if stmt == nil {
		return ""
	}

	var parts []string
	parts = append(parts, "CREATE DATABASE")

	if stmt.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	parts = append(parts, formatIdentifier(stmt.Name))

	return strings.Join(parts, " ") + ";"
}

// formatTableIdentifier formats a TableIdentifier as a string
func formatTableIdentifier(ti *TableIdentifier) string {
	if ti == nil {
		return ""
	}
	if ti.IsQualified() {
		return formatIdentifier(ti.Database) + "." + formatIdentifier(ti.Table)
	}
	return formatIdentifier(ti.Table)
}

// formatDropTableStmt formats a DROP TABLE statement
func formatDropTableStmt(stmt *DropTableStmt) string {
	if stmt == nil {
		return ""
	}

	var parts []string
	parts = append(parts, "DROP TABLE")

	if stmt.IfExists {
		parts = append(parts, "IF EXISTS")
	}

	parts = append(parts, formatTableIdentifier(stmt.TableName))

	return strings.Join(parts, " ") + ";"
}

// formatShowStmt formats a SHOW statement
func formatShowStmt(stmt *ShowStmt) string {
	if stmt == nil {
		return ""
	}

	var parts []string
	parts = append(parts, "SHOW")

	switch stmt.ShowType {
	case SHOW_DATABASES:
		parts = append(parts, "DATABASES")
	case SHOW_TABLES:
		parts = append(parts, "TABLES")
	case SHOW_USERS:
		parts = append(parts, "USERS")
	case SHOW_INDEXES:
		parts = append(parts, "INDEXES")
	case SHOW_GRANTS:
		parts = append(parts, "GRANTS")
	}

	// Add FROM clause if present
	if stmt.From != nil {
		parts = append(parts, "FROM")
		parts = append(parts, formatIdentifier(stmt.From))
	}

	return strings.Join(parts, " ") + ";"
}

// formatUseStmt formats a USE statement
func formatUseStmt(stmt *UseStmt) string {
	if stmt == nil {
		return ""
	}

	var parts []string
	parts = append(parts, "USE")
	parts = append(parts, formatIdentifier(stmt.DatabaseName))

	return strings.Join(parts, " ") + ";"
}

// formatExplainStmt formats an EXPLAIN statement
func formatExplainStmt(stmt *ExplainStmt) string {
	if stmt == nil {
		return ""
	}

	var parts []string
	parts = append(parts, "EXPLAIN")

	if stmt.Stmt != nil {
		// This would need to recursively format the inner statement
		parts = append(parts, formatExpression(stmt.Stmt))
	}

	return strings.Join(parts, " ") + ";"
}
