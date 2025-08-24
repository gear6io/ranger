package signals

import (
	"fmt"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

// ClientData represents a client data message for batch insertion
type ClientData struct {
	TableName string
	Columns   []string
	Rows      [][]interface{}
}

// Type returns the signal type
func (d *ClientData) Type() protocol.SignalType {
	return protocol.ClientData
}

// Pack serializes the data message to bytes
func (d *ClientData) Pack() ([]byte, error) {
	// Calculate total size
	size := d.Size()
	buf := make([]byte, 0, size)

	// Pack table name (4 bytes length + string)
	tableBytes := []byte(d.TableName)
	tableLenBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(tableLenBytes, uint32(len(tableBytes)))
	buf = append(buf, tableLenBytes...)
	buf = append(buf, tableBytes...)

	// Pack column count (4 bytes)
	columnCount := uint32(len(d.Columns))
	columnCountBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(columnCountBytes, columnCount)
	buf = append(buf, columnCountBytes...)

	// Pack row count (4 bytes)
	rowCount := uint32(len(d.Rows))
	rowCountBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(rowCountBytes, rowCount)
	buf = append(buf, rowCountBytes...)

	// Pack column names (4 bytes length + string for each column)
	for _, col := range d.Columns {
		colBytes := []byte(col)
		colLenBytes := make([]byte, 4)
		protocol.WriteUint32BigEndian(colLenBytes, uint32(len(colBytes)))
		buf = append(buf, colLenBytes...)
		buf = append(buf, colBytes...)
	}

	// Column types are not sent - Ranger knows the schema

	// Pack row data (4 bytes length + string for each value)
	for _, row := range d.Rows {
		for _, value := range row {
			strValue := fmt.Sprintf("%v", value)
			valueBytes := []byte(strValue)
			valueLenBytes := make([]byte, 4)
			protocol.WriteUint32BigEndian(valueLenBytes, uint32(len(valueBytes)))
			buf = append(buf, valueLenBytes...)
			buf = append(buf, valueBytes...)
		}
	}

	return buf, nil
}

// Unpack deserializes the data message from bytes
func (d *ClientData) Unpack(data []byte) error {
	if len(data) < 12 { // minimum: 4 bytes table length + 4 bytes column count + 4 bytes row count
		return fmt.Errorf("insufficient data for client data")
	}

	pos := 0

	// Read table name length (4 bytes, big endian)
	if pos+4 > len(data) {
		return fmt.Errorf("insufficient data for table name length")
	}
	tableLen := protocol.ReadUint32BigEndian(data[pos:])
	pos += 4

	// Read table name
	if pos+int(tableLen) > len(data) {
		return fmt.Errorf("insufficient data for table name")
	}
	d.TableName = string(data[pos : pos+int(tableLen)])
	pos += int(tableLen)

	// Read column count (4 bytes, big endian)
	if pos+4 > len(data) {
		return fmt.Errorf("insufficient data for column count")
	}
	columnCount := protocol.ReadUint32BigEndian(data[pos:])
	pos += 4

	// Read row count (4 bytes, big endian)
	if pos+4 > len(data) {
		return fmt.Errorf("insufficient data for row count")
	}
	rowCount := protocol.ReadUint32BigEndian(data[pos:])
	pos += 4

	// Read column names
	d.Columns = make([]string, columnCount)
	for i := uint32(0); i < columnCount; i++ {
		if pos+4 > len(data) {
			return fmt.Errorf("insufficient data for column %d name length", i)
		}
		colLen := protocol.ReadUint32BigEndian(data[pos:])
		pos += 4

		if pos+int(colLen) > len(data) {
			return fmt.Errorf("insufficient data for column %d name", i)
		}
		d.Columns[i] = string(data[pos : pos+int(colLen)])
		pos += int(colLen)
	}

	// Column types are not read - Ranger knows the schema

	// Read row data
	d.Rows = make([][]interface{}, rowCount)
	for i := uint32(0); i < rowCount; i++ {
		d.Rows[i] = make([]interface{}, columnCount)
		for j := uint32(0); j < columnCount; j++ {
			if pos+4 > len(data) {
				return fmt.Errorf("insufficient data for row %d column %d value length", i, j)
			}
			valueLen := protocol.ReadUint32BigEndian(data[pos:])
			pos += 4

			if pos+int(valueLen) > len(data) {
				return fmt.Errorf("insufficient data for row %d column %d value", i, j)
			}
			d.Rows[i][j] = string(data[pos : pos+int(valueLen)])
			pos += int(valueLen)
		}
	}

	return nil
}

// Size returns the estimated size of the packed message
func (d *ClientData) Size() int {
	size := 4 + len(d.TableName) + 4 + 4 // table name + column count + row count

	// Add column names
	for _, col := range d.Columns {
		size += 4 + len(col)
	}

	// Add row data
	for _, row := range d.Rows {
		for _, value := range row {
			strValue := fmt.Sprintf("%v", value)
			size += 4 + len(strValue)
		}
	}

	return size
}

// NewClientData creates a new client data message
func NewClientData(tableName string, columns []string, rows [][]interface{}) *ClientData {
	return &ClientData{
		TableName: tableName,
		Columns:   columns,
		Rows:      rows,
	}
}

// Register registers this signal type in both registry and factory
func (d *ClientData) Register(registry *protocol.Registry, factory *protocol.SignalFactory) error {
	// Register in registry
	if err := registry.RegisterClientSignal(d, &protocol.SignalInfo{Name: "ClientData"}); err != nil {
		return fmt.Errorf("failed to register ClientData in registry: %w", err)
	}

	// Register constructor in factory
	factory.RegisterConstructor(protocol.ClientData, func() protocol.Signal {
		return &ClientData{}
	})

	return nil
}
