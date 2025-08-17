package protocol

import (
	"encoding/binary"
	"io"
)

// WriteUint32BigEndian writes a uint32 value in big endian format to the provided buffer
func WriteUint32BigEndian(buf []byte, value uint32) {
	binary.BigEndian.PutUint32(buf, value)
}

// ReadUint32BigEndian reads a uint32 value in big endian format from the provided buffer
func ReadUint32BigEndian(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

// WriteUint32BigEndianToWriter writes a uint32 value in big endian format to an io.Writer
func WriteUint32BigEndianToWriter(writer io.Writer, value uint32) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, value)
	_, err := writer.Write(buf)
	return err
}

// ReadUint32BigEndianFromReader reads a uint32 value in big endian format from an io.Reader
func ReadUint32BigEndianFromReader(reader io.Reader) (uint32, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf), nil
}

// WriteUint64BigEndian writes a uint64 value in big endian format to the provided buffer
func WriteUint64BigEndian(buf []byte, value uint64) {
	binary.BigEndian.PutUint64(buf, value)
}

// ReadUint64BigEndian reads a uint64 value in big endian format from the provided buffer
func ReadUint64BigEndian(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}
