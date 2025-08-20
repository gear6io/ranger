package signals

import (
	"fmt"
	"strings"

	"github.com/TFMV/icebox/server/protocols/native/protocol"
)

// ServerData represents a server data message for query results
type ServerData struct {
	Columns     []Column
	Rows        [][]interface{}
	ColumnCount uint32
	RowCount    uint32
}

// Column represents a column in the result set
type Column struct {
	Name string
	Type string
}

// Type returns the signal type
func (d *ServerData) Type() protocol.SignalType {
	return protocol.ServerData
}

// Pack serializes the data message to bytes
func (d *ServerData) Pack() ([]byte, error) {
	// Calculate total size
	size := d.Size()
	buf := make([]byte, 0, size)

	// Pack column count (uvarint)
	columnCount := uint64(len(d.Columns))
	for columnCount >= 0x80 {
		buf = append(buf, byte(columnCount)|0x80)
		columnCount >>= 7
	}
	buf = append(buf, byte(columnCount))

	// Pack each column
	for _, col := range d.Columns {
		// Pack column name (uvarint length + string)
		nameBytes := []byte(col.Name)
		nameLen := uint64(len(nameBytes))
		for nameLen >= 0x80 {
			buf = append(buf, byte(nameLen)|0x80)
			nameLen >>= 7
		}
		buf = append(buf, byte(nameLen))
		buf = append(buf, nameBytes...)

		// Pack column type (uvarint length + string)
		typeBytes := []byte(col.Type)
		typeLen := uint64(len(typeBytes))
		for typeLen >= 0x80 {
			buf = append(buf, byte(typeLen)|0x80)
			typeLen >>= 7
		}
		buf = append(buf, byte(typeLen))
		buf = append(buf, typeBytes...)
	}

	// Pack data block (uvarint) - placeholder for now
	dataBlock := uint64(1)
	for dataBlock >= 0x80 {
		buf = append(buf, byte(dataBlock)|0x80)
		dataBlock >>= 7
	}
	buf = append(buf, byte(dataBlock))

	// Pack row count (uvarint)
	rowCount := uint64(len(d.Rows))
	for rowCount >= 0x80 {
		buf = append(buf, byte(rowCount)|0x80)
		rowCount >>= 7
	}
	buf = append(buf, byte(rowCount))

	// Pack row data for each column
	for colIdx := range d.Columns {
		// Collect all values for this column
		var columnValues []string
		for _, row := range d.Rows {
			if colIdx < len(row) {
				columnValues = append(columnValues, fmt.Sprintf("%v", row[colIdx]))
			} else {
				columnValues = append(columnValues, "")
			}
		}

		// Join values with comma and pack as single string
		columnData := strings.Join(columnValues, ",")
		dataBytes := []byte(columnData)
		dataLen := uint64(len(dataBytes))
		for dataLen >= 0x80 {
			buf = append(buf, byte(dataLen)|0x80)
			dataLen >>= 7
		}
		buf = append(buf, byte(dataLen))
		buf = append(buf, dataBytes...)
	}

	return buf, nil
}

// Unpack deserializes the data message from bytes
func (d *ServerData) Unpack(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty server data message")
	}

	pos := 0

	// Read column count (uvarint)
	columnCount, bytesRead := d.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read column count")
	}
	d.ColumnCount = uint32(columnCount)
	pos += bytesRead

	// Read columns
	d.Columns = make([]Column, d.ColumnCount)
	for i := uint32(0); i < d.ColumnCount; i++ {
		// Read column name length (uvarint)
		nameLen, bytesRead := d.readUvarint(data[pos:])
		if bytesRead == 0 {
			return fmt.Errorf("failed to read column %d name length", i)
		}
		pos += bytesRead

		// Read column name
		if pos+int(nameLen) > len(data) {
			return fmt.Errorf("insufficient data for column %d name", i)
		}
		d.Columns[i].Name = string(data[pos : pos+int(nameLen)])
		pos += int(nameLen)

		// Read column type length (uvarint)
		typeLen, bytesRead := d.readUvarint(data[pos:])
		if bytesRead == 0 {
			return fmt.Errorf("failed to read column %d type length", i)
		}
		pos += bytesRead

		// Read column type
		if pos+int(typeLen) > len(data) {
			return fmt.Errorf("insufficient data for column %d type", i)
		}
		d.Columns[i].Type = string(data[pos : pos+int(typeLen)])
		pos += int(typeLen)
	}

	// Read data block (uvarint) - skip for now
	_, bytesRead = d.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read data block")
	}
	pos += bytesRead

	// Read row count (uvarint)
	rowCount, bytesRead := d.readUvarint(data[pos:])
	if bytesRead == 0 {
		return fmt.Errorf("failed to read row count")
	}
	d.RowCount = uint32(rowCount)
	pos += bytesRead

	// Read row data for each column
	d.Rows = make([][]interface{}, d.RowCount)
	for i := uint32(0); i < d.RowCount; i++ {
		d.Rows[i] = make([]interface{}, d.ColumnCount)
	}

	for colIdx := uint32(0); colIdx < d.ColumnCount; colIdx++ {
		// Read data length (uvarint)
		dataLen, bytesRead := d.readUvarint(data[pos:])
		if bytesRead == 0 {
			return fmt.Errorf("failed to read column %d data length", colIdx)
		}
		pos += bytesRead

		// Read data
		if pos+int(dataLen) > len(data) {
			return fmt.Errorf("insufficient data for column %d data", colIdx)
		}
		columnData := string(data[pos : pos+int(dataLen)])
		pos += int(dataLen)

		// Parse comma-separated values
		values := strings.Split(columnData, ",")
		for rowIdx, value := range values {
			if rowIdx < int(d.RowCount) {
				d.Rows[rowIdx][colIdx] = strings.TrimSpace(value)
			}
		}
	}

	return nil
}

// Size returns the estimated size of the packed message
func (d *ServerData) Size() int {
	size := 0

	// Column count (uvarint)
	size += 8

	// Columns
	for _, col := range d.Columns {
		size += 8 + len(col.Name) + 8 + len(col.Type)
	}

	// Data block and row count (uvarints)
	size += 16

	// Row data
	for _, row := range d.Rows {
		for _, value := range row {
			strValue := fmt.Sprintf("%v", value)
			size += 8 + len(strValue)
		}
	}

	return size
}

// readUvarint reads a variable-length integer from the beginning of data
func (d *ServerData) readUvarint(data []byte) (uint64, int) {
	var value uint64
	var shift uint

	for i, b := range data {
		value |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return value, i + 1
		}
		shift += 7
		if shift >= 64 {
			return 0, 0 // overflow
		}
	}

	return 0, 0 // incomplete
}

// NewServerData creates a new server data message
func NewServerData(columns []Column, rows [][]interface{}) *ServerData {
	return &ServerData{
		Columns:     columns,
		Rows:        rows,
		ColumnCount: uint32(len(columns)),
		RowCount:    uint32(len(rows)),
	}
}

// Register registers this signal type in both registry and factory
func (d *ServerData) Register(registry *protocol.Registry, factory *protocol.SignalFactory) error {
	// Register in registry
	if err := registry.RegisterServerSignal(d, &protocol.SignalInfo{Name: "ServerData"}); err != nil {
		return fmt.Errorf("failed to register ServerData in registry: %w", err)
	}

	// Register constructor in factory
	factory.RegisterConstructor(protocol.ServerData, func() protocol.Signal {
		return &ServerData{}
	})

	return nil
}
