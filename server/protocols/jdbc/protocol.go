package jdbc

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/TFMV/icebox/pkg/errors"
)

// PostgreSQL message types
const (
	MessageTypeQuery         = 'Q'
	MessageTypeParse         = 'P'
	MessageTypeBind          = 'B'
	MessageTypeExecute       = 'E'
	MessageTypeDescribe      = 'D'
	MessageTypeClose         = 'C'
	MessageTypeSync          = 'S'
	MessageTypeTerminate     = 'X'
	MessageTypePassword      = 'p'
	MessageTypeStartup       = 0
	MessageTypeSSLRequest    = 80877103
	MessageTypeCancel        = 80877102
	MessageTypeGSSENCRequest = 80877104
)

// PostgreSQL response types
const (
	ResponseTypeReadyForQuery    = 'Z'
	ResponseTypeRowDescription   = 'T'
	ResponseTypeDataRow          = 'D'
	ResponseTypeCommandComplete  = 'C'
	ResponseTypeErrorResponse    = 'E'
	ResponseTypeNoticeResponse   = 'N'
	ResponseTypeParameterStatus  = 'S'
	ResponseTypeBackendKeyData   = 'K'
	ResponseTypeAuthenticationOK = 'R'
	ResponseTypeParseComplete    = '1'
	ResponseTypeBindComplete     = '2'
	ResponseTypeCloseComplete    = '3'
	ResponseTypeNoData           = 'n'
	ResponseTypePortalSuspended  = 's'
)

// Message represents a PostgreSQL wire protocol message
type Message struct {
	Type   byte
	Length int32
	Data   []byte
}

// ReadMessage reads a PostgreSQL message from the connection
func ReadMessage(reader io.Reader) (*Message, error) {
	// Read message type (1 byte)
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(reader, typeBuf); err != nil {
		return nil, errors.New(ErrMessageTypeReadFailed, "failed to read message type", err)
	}

	msgType := typeBuf[0]

	// Read message length (4 bytes)
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, lengthBuf); err != nil {
		return nil, errors.New(ErrMessageLengthReadFailed, "failed to read message length", err)
	}

	length := binary.BigEndian.Uint32(lengthBuf)

	// Read message data
	data := make([]byte, length-4) // Subtract 4 for the length field itself
	if length > 4 {
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, errors.New(ErrMessageDataReadFailed, "failed to read message data", err)
		}
	}

	return &Message{
		Type:   msgType,
		Length: int32(length),
		Data:   data,
	}, nil
}

// WriteMessage writes a PostgreSQL message to the connection
func WriteMessage(writer io.Writer, msgType byte, data []byte) error {
	// Calculate total length (1 byte type + 4 bytes length + data length)
	totalLength := 5 + len(data)

	// Write message type
	if _, err := writer.Write([]byte{msgType}); err != nil {
		return errors.New(ErrMessageTypeWriteFailed, "failed to write message type", err)
	}

	// Write message length
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(totalLength))
	if _, err := writer.Write(lengthBuf); err != nil {
		return errors.New(ErrMessageLengthWriteFailed, "failed to write message length", err)
	}

	// Write message data
	if len(data) > 0 {
		if _, err := writer.Write(data); err != nil {
			return errors.New(ErrMessageDataWriteFailed, "failed to write message data", err)
		}
	}

	return nil
}

// WriteErrorResponse writes an error response
func WriteErrorResponse(writer io.Writer, code, message string) error {
	// Format: field type + field value pairs, terminated by null byte
	data := fmt.Sprintf("S%s\x00C%s\x00M%s\x00\x00", code, code, message)
	return WriteMessage(writer, ResponseTypeErrorResponse, []byte(data))
}

// WriteNoticeResponse writes a notice response
func WriteNoticeResponse(writer io.Writer, message string) error {
	data := fmt.Sprintf("M%s\x00\x00", message)
	return WriteMessage(writer, ResponseTypeNoticeResponse, []byte(data))
}

// WriteCommandComplete writes a command complete response
func WriteCommandComplete(writer io.Writer, tag string) error {
	data := tag + "\x00"
	return WriteMessage(writer, ResponseTypeCommandComplete, []byte(data))
}

// WriteReadyForQuery writes a ready for query response
func WriteReadyForQuery(writer io.Writer, transactionStatus byte) error {
	return WriteMessage(writer, ResponseTypeReadyForQuery, []byte{transactionStatus})
}

// WriteAuthenticationOK writes an authentication OK response
func WriteAuthenticationOK(writer io.Writer) error {
	// Authentication OK (4 bytes of zeros)
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, 0)
	return WriteMessage(writer, ResponseTypeAuthenticationOK, data)
}

// WriteParameterStatus writes a parameter status response
func WriteParameterStatus(writer io.Writer, name, value string) error {
	data := name + "\x00" + value + "\x00"
	return WriteMessage(writer, ResponseTypeParameterStatus, []byte(data))
}

// WriteBackendKeyData writes backend key data
func WriteBackendKeyData(writer io.Writer, processID, secretKey int32) error {
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data[0:4], uint32(processID))
	binary.BigEndian.PutUint32(data[4:8], uint32(secretKey))
	return WriteMessage(writer, ResponseTypeBackendKeyData, data)
}

// WriteRowDescription writes a row description
func WriteRowDescription(writer io.Writer, columns []ColumnDescription) error {
	// Format: number of columns (2 bytes) + column descriptions
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, uint16(len(columns)))

	for _, col := range columns {
		// Column name (null-terminated)
		data = append(data, []byte(col.Name)...)
		data = append(data, 0)

		// Table OID (4 bytes) - use 0 for now
		tableOID := make([]byte, 4)
		binary.BigEndian.PutUint32(tableOID, 0)
		data = append(data, tableOID...)

		// Column attribute number (2 bytes) - use 0 for now
		attrNum := make([]byte, 2)
		binary.BigEndian.PutUint16(attrNum, 0)
		data = append(data, attrNum...)

		// Data type OID (4 bytes)
		typeOID := make([]byte, 4)
		binary.BigEndian.PutUint32(typeOID, uint32(col.TypeOID))
		data = append(data, typeOID...)

		// Data type size (2 bytes)
		typeSize := make([]byte, 2)
		binary.BigEndian.PutUint16(typeSize, uint16(col.TypeSize))
		data = append(data, typeSize...)

		// Type modifier (4 bytes) - use -1 for now
		typeMod := make([]byte, 4)
		binary.BigEndian.PutUint32(typeMod, 0xFFFFFFFF)
		data = append(data, typeMod...)

		// Format code (2 bytes) - use 0 for text format
		formatCode := make([]byte, 2)
		binary.BigEndian.PutUint16(formatCode, 0)
		data = append(data, formatCode...)
	}

	return WriteMessage(writer, ResponseTypeRowDescription, data)
}

// WriteDataRow writes a data row
func WriteDataRow(writer io.Writer, values []interface{}) error {
	// Format: number of columns (2 bytes) + column values
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, uint16(len(values)))

	for _, value := range values {
		if value == nil {
			// NULL value: -1 length
			nullLength := make([]byte, 4)
			binary.BigEndian.PutUint32(nullLength, 0xFFFFFFFF)
			data = append(data, nullLength...)
		} else {
			// Convert value to string
			valueStr := fmt.Sprintf("%v", value)
			valueBytes := []byte(valueStr)

			// Value length (4 bytes)
			valueLength := make([]byte, 4)
			binary.BigEndian.PutUint32(valueLength, uint32(len(valueBytes)))
			data = append(data, valueLength...)

			// Value data
			data = append(data, valueBytes...)
		}
	}

	return WriteMessage(writer, ResponseTypeDataRow, data)
}

// ColumnDescription represents a column in a row description
type ColumnDescription struct {
	Name     string
	TypeOID  int32
	TypeSize int16
}

// ParseStartupMessage parses a startup message
func ParseStartupMessage(reader io.Reader) (map[string]string, error) {
	// Read message length (4 bytes)
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, lengthBuf); err != nil {
		return nil, errors.New(ErrMessageLengthReadFailed, "failed to read startup message length", err)
	}

	length := binary.BigEndian.Uint32(lengthBuf)

	// Read message data
	data := make([]byte, length-4)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, errors.New(ErrMessageDataReadFailed, "failed to read startup message data", err)
	}

	// Parse key-value pairs
	params := make(map[string]string)
	pos := 0

	for pos < len(data) {
		// Find end of key
		keyEnd := pos
		for keyEnd < len(data) && data[keyEnd] != 0 {
			keyEnd++
		}
		if keyEnd >= len(data) {
			break
		}

		key := string(data[pos:keyEnd])
		pos = keyEnd + 1

		// Find end of value
		valueEnd := pos
		for valueEnd < len(data) && data[valueEnd] != 0 {
			valueEnd++
		}
		if valueEnd >= len(data) {
			break
		}

		value := string(data[pos:valueEnd])
		pos = valueEnd + 1

		params[key] = value
	}

	return params, nil
}

// WriteStartupResponse writes the initial startup response
func WriteStartupResponse(writer io.Writer) error {
	// Send authentication OK
	if err := WriteAuthenticationOK(writer); err != nil {
		return err
	}

	// Send parameter status messages
	params := map[string]string{
		"server_version":              "14.1 (Icebox)",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"TimeZone":                    "UTC",
		"server_encoding":             "UTF8",
		"integer_datetimes":           "on",
		"is_superuser":                "on",
		"session_authorization":       "icebox",
		"standard_conforming_strings": "on",
	}

	for name, value := range params {
		if err := WriteParameterStatus(writer, name, value); err != nil {
			return err
		}
	}

	// Send backend key data
	if err := WriteBackendKeyData(writer, 12345, 67890); err != nil {
		return err
	}

	// Send ready for query
	return WriteReadyForQuery(writer, 'I') // 'I' = idle
}
