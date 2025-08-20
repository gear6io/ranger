package native

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchInsertDataParsing(t *testing.T) {
	// Test the packet reading/writing functionality directly
	// Create a buffer to simulate the connection
	buf := &bytes.Buffer{}

	// Create packet writer and reader
	writer := NewPacketWriter(buf)
	reader := NewPacketReader(buf)

	// Test writing and reading a simple message
	messageType := byte(2) // ClientData
	payload := []byte("test_payload")

	// Write message using PacketWriter
	err := writer.WriteMessage(messageType, payload)
	assert.NoError(t, err)

	// Read message using PacketReader
	messageLength, err := reader.ReadUint32()
	assert.NoError(t, err)
	assert.Equal(t, uint32(len(payload)+1), messageLength) // +1 for message type

	readMessageType, err := reader.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, messageType, readMessageType)

	// Read payload
	readPayload := make([]byte, len(payload))
	_, err = io.ReadFull(reader.conn, readPayload)
	assert.NoError(t, err)
	assert.Equal(t, payload, readPayload)
}
