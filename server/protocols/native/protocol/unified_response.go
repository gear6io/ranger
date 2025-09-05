package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"
)

// UnifiedResponse represents a memory-efficient, consistent response format
// using byte arrays to minimize memory overhead and pointer chasing
type UnifiedResponse struct {
	// Core response data stored as byte array for memory efficiency
	Data []byte

	// Metadata for interpreting the data
	RowCount    int64
	ColumnCount int32
	QueryID     string
	Message     string
	Error       error

	// Offset pointers into Data for efficient access
	columnNamesOffset int32
	columnTypesOffset int32
	rowDataOffset     int32
}

// ResponseBuilder helps construct UnifiedResponse efficiently
type ResponseBuilder struct {
	buffer      []byte
	columnNames []string
	columnTypes []string
	rows        [][]interface{}
}

// NewResponseBuilder creates a new response builder
func NewResponseBuilder() *ResponseBuilder {
	return &ResponseBuilder{
		buffer: make([]byte, 0, 4096), // Start with 4KB buffer
	}
}

// SetColumns sets the column metadata
func (rb *ResponseBuilder) SetColumns(names, types []string) *ResponseBuilder {
	rb.columnNames = names
	rb.columnTypes = types
	return rb
}

// AddRow adds a data row
func (rb *ResponseBuilder) AddRow(row []interface{}) *ResponseBuilder {
	rb.rows = append(rb.rows, row)
	return rb
}

// Build constructs the final UnifiedResponse with optimized byte layout
func (rb *ResponseBuilder) Build(queryID, message string, err error) *UnifiedResponse {
	response := &UnifiedResponse{
		RowCount:    int64(len(rb.rows)),
		ColumnCount: int32(len(rb.columnNames)),
		QueryID:     queryID,
		Message:     message,
		Error:       err,
	}

	// Calculate required buffer size
	bufferSize := rb.calculateBufferSize()
	response.Data = make([]byte, 0, bufferSize)

	// Serialize column names
	response.columnNamesOffset = int32(len(response.Data))
	response.Data = rb.serializeStringArray(response.Data, rb.columnNames)

	// Serialize column types
	response.columnTypesOffset = int32(len(response.Data))
	response.Data = rb.serializeStringArray(response.Data, rb.columnTypes)

	// Serialize row data
	response.rowDataOffset = int32(len(response.Data))
	response.Data = rb.serializeRows(response.Data, rb.rows)

	return response
}

// calculateBufferSize estimates the required buffer size
func (rb *ResponseBuilder) calculateBufferSize() int {
	size := 0

	// Column names
	for _, name := range rb.columnNames {
		size += 4 + len(name) // 4 bytes length + string data
	}

	// Column types
	for _, typ := range rb.columnTypes {
		size += 4 + len(typ)
	}

	// Row data (estimate)
	if len(rb.rows) > 0 {
		avgRowSize := 0
		sampleRows := len(rb.rows)
		if sampleRows > 10 {
			sampleRows = 10 // Sample first 10 rows for estimation
		}

		for i := 0; i < sampleRows; i++ {
			for _, val := range rb.rows[i] {
				avgRowSize += rb.estimateValueSize(val)
			}
		}

		if sampleRows > 0 {
			avgRowSize = avgRowSize / sampleRows
			size += avgRowSize * len(rb.rows)
		}
	}

	return size + 1024 // Add some padding
}

// estimateValueSize estimates the serialized size of a value
func (rb *ResponseBuilder) estimateValueSize(val interface{}) int {
	switch v := val.(type) {
	case string:
		return 4 + len(v) // 4 bytes length + string data
	case int, int32, int64, float32, float64:
		return 8 // Max 8 bytes for numeric types
	case bool:
		return 1
	case nil:
		return 1 // Null marker
	default:
		return 16 // Conservative estimate for other types
	}
}

// serializeStringArray serializes a string array into the buffer
func (rb *ResponseBuilder) serializeStringArray(buffer []byte, strings []string) []byte {
	// Write count
	countBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBytes, uint32(len(strings)))
	buffer = append(buffer, countBytes...)

	// Write strings
	for _, str := range strings {
		// Write length
		lengthBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(lengthBytes, uint32(len(str)))
		buffer = append(buffer, lengthBytes...)

		// Write string data
		buffer = append(buffer, []byte(str)...)
	}

	return buffer
}

// serializeRows serializes row data into the buffer
func (rb *ResponseBuilder) serializeRows(buffer []byte, rows [][]interface{}) []byte {
	// Write row count
	countBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(countBytes, uint64(len(rows)))
	buffer = append(buffer, countBytes...)

	// Write each row
	for _, row := range rows {
		// Write column count for this row
		colCountBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(colCountBytes, uint32(len(row)))
		buffer = append(buffer, colCountBytes...)

		// Write each value
		for _, val := range row {
			buffer = rb.serializeValue(buffer, val)
		}
	}

	return buffer
}

// serializeValue serializes a single value into the buffer
func (rb *ResponseBuilder) serializeValue(buffer []byte, val interface{}) []byte {
	switch v := val.(type) {
	case nil:
		// Null marker
		buffer = append(buffer, 0)
	case string:
		// Type marker for string
		buffer = append(buffer, 1)
		// Length
		lengthBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(lengthBytes, uint32(len(v)))
		buffer = append(buffer, lengthBytes...)
		// Data
		buffer = append(buffer, []byte(v)...)
	case int64:
		// Type marker for int64
		buffer = append(buffer, 2)
		valueBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueBytes, uint64(v))
		buffer = append(buffer, valueBytes...)
	case int32:
		// Type marker for int32
		buffer = append(buffer, 3)
		valueBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(valueBytes, uint32(v))
		buffer = append(buffer, valueBytes...)
	case int:
		// Convert to int64
		return rb.serializeValue(buffer, int64(v))
	case float64:
		// Type marker for float64
		buffer = append(buffer, 4)
		valueBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueBytes, *(*uint64)(unsafe.Pointer(&v)))
		buffer = append(buffer, valueBytes...)
	case float32:
		// Type marker for float32
		buffer = append(buffer, 5)
		valueBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(valueBytes, *(*uint32)(unsafe.Pointer(&v)))
		buffer = append(buffer, valueBytes...)
	case bool:
		// Type marker for bool
		buffer = append(buffer, 6)
		if v {
			buffer = append(buffer, 1)
		} else {
			buffer = append(buffer, 0)
		}
	default:
		// Fallback to string representation
		return rb.serializeValue(buffer, fmt.Sprintf("%v", v))
	}

	return buffer
}

// GetColumns extracts column information from the response
func (ur *UnifiedResponse) GetColumns() (names []string, types []string, err error) {
	if ur.Data == nil {
		return nil, nil, fmt.Errorf("no data in response")
	}

	// Extract column names
	names, _, err = ur.deserializeStringArray(ur.columnNamesOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to deserialize column names: %w", err)
	}

	// Extract column types
	types, _, err = ur.deserializeStringArray(ur.columnTypesOffset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to deserialize column types: %w", err)
	}

	return names, types, nil
}

// GetRowIterator returns an iterator for efficient row access
func (ur *UnifiedResponse) GetRowIterator() *RowIterator {
	return &RowIterator{
		response: ur,
		offset:   ur.rowDataOffset,
		current:  -1,
	}
}

// deserializeStringArray deserializes a string array from the buffer
func (ur *UnifiedResponse) deserializeStringArray(offset int32) ([]string, int32, error) {
	if int(offset)+4 > len(ur.Data) {
		return nil, 0, fmt.Errorf("offset out of bounds")
	}

	// Read count
	count := binary.LittleEndian.Uint32(ur.Data[offset : offset+4])
	offset += 4

	strings := make([]string, count)
	for i := uint32(0); i < count; i++ {
		if int(offset)+4 > len(ur.Data) {
			return nil, 0, fmt.Errorf("string length offset out of bounds")
		}

		// Read string length
		length := binary.LittleEndian.Uint32(ur.Data[offset : offset+4])
		offset += 4

		if int(offset)+int(length) > len(ur.Data) {
			return nil, 0, fmt.Errorf("string data offset out of bounds")
		}

		// Read string data
		strings[i] = string(ur.Data[offset : offset+int32(length)])
		offset += int32(length)
	}

	return strings, offset, nil
}

// RowIterator provides efficient iteration over rows without allocating interface{} slices
type RowIterator struct {
	response *UnifiedResponse
	offset   int32
	current  int64
	rowCount int64
}

// Next advances to the next row
func (ri *RowIterator) Next() bool {
	if ri.current == -1 {
		// First call - read row count
		if int(ri.offset)+8 > len(ri.response.Data) {
			return false
		}
		ri.rowCount = int64(binary.LittleEndian.Uint64(ri.response.Data[ri.offset : ri.offset+8]))
		ri.offset += 8
	}

	ri.current++
	return ri.current < ri.rowCount
}

// ScanRow scans the current row into provided destinations
func (ri *RowIterator) ScanRow(dest ...interface{}) error {
	if ri.current < 0 || ri.current >= ri.rowCount {
		return fmt.Errorf("no current row")
	}

	// Read column count for this row
	if int(ri.offset)+4 > len(ri.response.Data) {
		return fmt.Errorf("column count offset out of bounds")
	}

	colCount := binary.LittleEndian.Uint32(ri.response.Data[ri.offset : ri.offset+4])
	ri.offset += 4

	if int(colCount) != len(dest) {
		return fmt.Errorf("destination count (%d) does not match column count (%d)", len(dest), colCount)
	}

	// Read each value
	for i := uint32(0); i < colCount; i++ {
		var err error
		ri.offset, err = ri.deserializeValue(ri.offset, dest[i])
		if err != nil {
			return fmt.Errorf("failed to deserialize value %d: %w", i, err)
		}
	}

	return nil
}

// deserializeValue deserializes a single value from the buffer
func (ri *RowIterator) deserializeValue(offset int32, dest interface{}) (int32, error) {
	if int(offset)+1 > len(ri.response.Data) {
		return offset, fmt.Errorf("type marker offset out of bounds")
	}

	typeMarker := ri.response.Data[offset]
	offset++

	switch typeMarker {
	case 0: // null
		// Set destination to nil/zero value
		return offset, nil
	case 1: // string
		if int(offset)+4 > len(ri.response.Data) {
			return offset, fmt.Errorf("string length offset out of bounds")
		}
		length := binary.LittleEndian.Uint32(ri.response.Data[offset : offset+4])
		offset += 4

		if int(offset)+int(length) > len(ri.response.Data) {
			return offset, fmt.Errorf("string data offset out of bounds")
		}

		str := string(ri.response.Data[offset : offset+int32(length)])
		offset += int32(length)

		if strPtr, ok := dest.(*string); ok {
			*strPtr = str
		} else {
			return offset, fmt.Errorf("destination is not *string")
		}
	case 2: // int64
		if int(offset)+8 > len(ri.response.Data) {
			return offset, fmt.Errorf("int64 offset out of bounds")
		}
		val := int64(binary.LittleEndian.Uint64(ri.response.Data[offset : offset+8]))
		offset += 8

		if intPtr, ok := dest.(*int64); ok {
			*intPtr = val
		} else {
			return offset, fmt.Errorf("destination is not *int64")
		}
	case 3: // int32
		if int(offset)+4 > len(ri.response.Data) {
			return offset, fmt.Errorf("int32 offset out of bounds")
		}
		val := int32(binary.LittleEndian.Uint32(ri.response.Data[offset : offset+4]))
		offset += 4

		if intPtr, ok := dest.(*int32); ok {
			*intPtr = val
		} else {
			return offset, fmt.Errorf("destination is not *int32")
		}
	case 4: // float64
		if int(offset)+8 > len(ri.response.Data) {
			return offset, fmt.Errorf("float64 offset out of bounds")
		}
		bits := binary.LittleEndian.Uint64(ri.response.Data[offset : offset+8])
		val := *(*float64)(unsafe.Pointer(&bits))
		offset += 8

		if floatPtr, ok := dest.(*float64); ok {
			*floatPtr = val
		} else {
			return offset, fmt.Errorf("destination is not *float64")
		}
	case 5: // float32
		if int(offset)+4 > len(ri.response.Data) {
			return offset, fmt.Errorf("float32 offset out of bounds")
		}
		bits := binary.LittleEndian.Uint32(ri.response.Data[offset : offset+4])
		val := *(*float32)(unsafe.Pointer(&bits))
		offset += 4

		if floatPtr, ok := dest.(*float32); ok {
			*floatPtr = val
		} else {
			return offset, fmt.Errorf("destination is not *float32")
		}
	case 6: // bool
		if int(offset)+1 > len(ri.response.Data) {
			return offset, fmt.Errorf("bool offset out of bounds")
		}
		val := ri.response.Data[offset] != 0
		offset++

		if boolPtr, ok := dest.(*bool); ok {
			*boolPtr = val
		} else {
			return offset, fmt.Errorf("destination is not *bool")
		}
	default:
		return offset, fmt.Errorf("unknown type marker: %d", typeMarker)
	}

	return offset, nil
}

// WriteTo implements io.WriterTo for efficient streaming
func (ur *UnifiedResponse) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(ur.Data)
	return int64(n), err
}

// Size returns the total memory footprint of the response
func (ur *UnifiedResponse) Size() int {
	return len(ur.Data) +
		len(ur.QueryID) +
		len(ur.Message) +
		int(unsafe.Sizeof(*ur))
}

// ToLegacyRows converts UnifiedResponse to legacy [][]interface{} format
// This is provided for backward compatibility during migration
func (ur *UnifiedResponse) ToLegacyRows() ([][]interface{}, error) {
	var rows [][]interface{}
	iterator := ur.GetRowIterator()

	columns, types, err := ur.GetColumns()
	if err != nil {
		return nil, err
	}

	for iterator.Next() {
		row := make([]interface{}, len(columns))

		// Create destination pointers based on column types
		dests := make([]interface{}, len(columns))
		for i, colType := range types {
			switch colType {
			case "string":
				var s string
				dests[i] = &s
			case "int64":
				var n int64
				dests[i] = &n
			case "int32":
				var n int32
				dests[i] = &n
			case "float64":
				var f float64
				dests[i] = &f
			case "float32":
				var f float32
				dests[i] = &f
			case "bool":
				var b bool
				dests[i] = &b
			default:
				var s string
				dests[i] = &s
			}
		}

		if err := iterator.ScanRow(dests...); err != nil {
			return nil, err
		}

		// Extract values from pointers
		for i, dest := range dests {
			switch ptr := dest.(type) {
			case *string:
				row[i] = *ptr
			case *int64:
				row[i] = *ptr
			case *int32:
				row[i] = *ptr
			case *float64:
				row[i] = *ptr
			case *float32:
				row[i] = *ptr
			case *bool:
				row[i] = *ptr
			}
		}

		rows = append(rows, row)
	}

	return rows, nil
}
