package signals

import (
	"fmt"
	"testing"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

func TestServerData(t *testing.T) {
	// Test creating a new server data message
	columns := []Column{
		{Name: "id", Type: "int"},
		{Name: "name", Type: "string"},
		{Name: "email", Type: "string"},
	}
	rows := [][]interface{}{
		{1, "John Doe", "john@example.com"},
		{2, "Jane Smith", "jane@example.com"},
	}

	data := NewServerData(columns, rows)

	// Test Type method
	if data.Type() != protocol.ServerData {
		t.Errorf("Expected Type() to return ServerData, got %d", data.Type())
	}

	// Test Size method
	expectedSize := 8 // Column count (uvarint)
	for _, col := range columns {
		expectedSize += 8 + len(col.Name) + 8 + len(col.Type)
	}
	expectedSize += 16 // Data block and row count (uvarints)
	for _, row := range rows {
		for _, value := range row {
			strValue := fmt.Sprintf("%v", value)
			expectedSize += 8 + len(strValue)
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
	newData := &ServerData{}
	err = newData.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() failed: %v", err)
	}

	// Verify all fields were unpacked correctly
	if newData.ColumnCount != data.ColumnCount {
		t.Errorf("ColumnCount mismatch: expected %d, got %d", data.ColumnCount, newData.ColumnCount)
	}
	if newData.RowCount != data.RowCount {
		t.Errorf("RowCount mismatch: expected %d, got %d", data.RowCount, newData.RowCount)
	}

	// Verify columns
	if len(newData.Columns) != len(data.Columns) {
		t.Errorf("Columns count mismatch: expected %d, got %d", len(data.Columns), len(newData.Columns))
	}
	for i, col := range data.Columns {
		if newData.Columns[i].Name != col.Name {
			t.Errorf("Column %d name mismatch: expected %s, got %s", i, col.Name, newData.Columns[i].Name)
		}
		if newData.Columns[i].Type != col.Type {
			t.Errorf("Column %d type mismatch: expected %s, got %s", i, col.Type, newData.Columns[i].Type)
		}
	}

	// Verify rows
	if len(newData.Rows) != len(data.Rows) {
		t.Errorf("Rows count mismatch: expected %d, got %d", len(data.Rows), len(newData.Rows))
	}
	for i, row := range data.Rows {
		for j, value := range row {
			if fmt.Sprintf("%v", newData.Rows[i][j]) != fmt.Sprintf("%v", value) {
				t.Errorf("Row %d column %d value mismatch: expected %v, got %v", i, j, value, newData.Rows[i][j])
			}
		}
	}
}

func TestServerDataUnpackEmpty(t *testing.T) {
	data := &ServerData{}
	err := data.Unpack([]byte{})
	if err == nil {
		t.Error("Expected error when unpacking empty data")
	}
}

func TestServerDataUnpackInsufficient(t *testing.T) {
	data := &ServerData{}
	err := data.Unpack([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}) // Too short
	if err == nil {
		t.Error("Expected error when unpacking insufficient data")
	}
}
