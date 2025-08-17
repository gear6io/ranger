package signals

import (
	"fmt"
	"testing"

	"github.com/TFMV/icebox/server/protocols/native/protocol"
)

func TestClientData(t *testing.T) {
	// Test creating a new client data message
	columns := []string{"id", "name", "email"}
	columnTypes := []string{"int", "string", "string"}
	rows := [][]interface{}{
		{1, "John Doe", "john@example.com"},
		{2, "Jane Smith", "jane@example.com"},
	}

	data := NewClientData("users", columns, columnTypes, rows)

	// Test Type method
	if data.Type() != protocol.ClientData {
		t.Errorf("Expected Type() to return ClientData, got %d", data.Type())
	}

	// Test Size method
	expectedSize := 4 + len("users") + 4 + 4 // table name + column count + row count
	for _, col := range columns {
		expectedSize += 4 + len(col)
	}
	for _, colType := range columnTypes {
		expectedSize += 4 + len(colType)
	}
	for _, row := range rows {
		for _, value := range row {
			strValue := fmt.Sprintf("%v", value)
			expectedSize += 4 + len(strValue)
		}
	}

	if data.Size() != expectedSize {
		t.Errorf("Expected Size() to return %d, got %d", expectedSize, data.Size())
	}

	// Test Pack method
	packed, err := data.Pack()
	if err != nil {
		t.Fatalf("Pack() failed: %v", err)
	}

	if len(packed) == 0 {
		t.Error("Pack() returned empty data")
	}

	// Test Unpack method
	newData := &ClientData{}
	err = newData.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() failed: %v", err)
	}

	// Verify all fields were unpacked correctly
	if newData.TableName != data.TableName {
		t.Errorf("TableName mismatch: expected %s, got %s", data.TableName, newData.TableName)
	}
	if len(newData.Columns) != len(data.Columns) {
		t.Errorf("Columns count mismatch: expected %d, got %d", len(data.Columns), len(newData.Columns))
	}
	if len(newData.ColumnTypes) != len(data.ColumnTypes) {
		t.Errorf("ColumnTypes count mismatch: expected %d, got %d", len(data.ColumnTypes), len(newData.ColumnTypes))
	}
	if len(newData.Rows) != len(data.Rows) {
		t.Errorf("Rows count mismatch: expected %d, got %d", len(data.Rows), len(newData.Rows))
	}

	// Verify column names
	for i, col := range data.Columns {
		if newData.Columns[i] != col {
			t.Errorf("Column %d name mismatch: expected %s, got %s", i, col, newData.Columns[i])
		}
	}

	// Verify column types
	for i, colType := range data.ColumnTypes {
		if newData.ColumnTypes[i] != colType {
			t.Errorf("Column %d type mismatch: expected %s, got %s", i, colType, newData.ColumnTypes[i])
		}
	}

	// Verify row data
	for i, row := range data.Rows {
		for j, value := range row {
			if fmt.Sprintf("%v", newData.Rows[i][j]) != fmt.Sprintf("%v", value) {
				t.Errorf("Row %d column %d value mismatch: expected %v, got %v", i, j, value, newData.Rows[i][j])
			}
		}
	}
}

func TestClientDataUnpackEmpty(t *testing.T) {
	data := &ClientData{}
	err := data.Unpack([]byte{})
	if err == nil {
		t.Error("Expected error when unpacking empty data")
	}
}

func TestClientDataUnpackInsufficient(t *testing.T) {
	data := &ClientData{}
	err := data.Unpack([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}) // Too short
	if err == nil {
		t.Error("Expected error when unpacking insufficient data")
	}
}
