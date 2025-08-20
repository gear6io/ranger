package signals

import (
	"fmt"

	"github.com/TFMV/icebox/server/protocols/native/protocol"
)

// ClientQuery represents a client query message
type ClientQuery struct {
	Query    string
	QueryID  string
	Database string
	User     string
	Password string
}

// Type returns the signal type
func (q *ClientQuery) Type() protocol.SignalType {
	return protocol.ClientQuery
}

// Pack serializes the query message to bytes
func (q *ClientQuery) Pack() ([]byte, error) {
	// Calculate total size
	size := q.Size()
	buf := make([]byte, 0, size)

	// Pack query string (4 bytes length + string)
	queryBytes := []byte(q.Query)
	queryLenBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(queryLenBytes, uint32(len(queryBytes)))
	buf = append(buf, queryLenBytes...)
	buf = append(buf, queryBytes...)

	// Pack query ID (4 bytes length + string)
	queryIDBytes := []byte(q.QueryID)
	queryIDLenBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(queryIDLenBytes, uint32(len(queryIDBytes)))
	buf = append(buf, queryIDLenBytes...)
	buf = append(buf, queryIDBytes...)

	// Pack database (4 bytes length + string)
	dbBytes := []byte(q.Database)
	dbLenBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(dbLenBytes, uint32(len(dbBytes)))
	buf = append(buf, dbLenBytes...)
	buf = append(buf, dbBytes...)

	// Pack user (4 bytes length + string)
	userBytes := []byte(q.User)
	userLenBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(userLenBytes, uint32(len(userBytes)))
	buf = append(buf, userLenBytes...)
	buf = append(buf, userBytes...)

	// Pack password (4 bytes length + string)
	pwdBytes := []byte(q.Password)
	pwdLenBytes := make([]byte, 4)
	protocol.WriteUint32BigEndian(pwdLenBytes, uint32(len(pwdBytes)))
	buf = append(buf, pwdLenBytes...)
	buf = append(buf, pwdBytes...)

	return buf, nil
}

// Unpack deserializes the query message from bytes
func (q *ClientQuery) Unpack(data []byte) error {
	if len(data) < 20 { // minimum: 4 bytes for each length field
		return fmt.Errorf("insufficient data for client query")
	}

	pos := 0

	// Read query length (4 bytes, big endian)
	if pos+4 > len(data) {
		return fmt.Errorf("insufficient data for query length")
	}
	queryLen := protocol.ReadUint32BigEndian(data[pos:])
	pos += 4

	// Read query string
	if pos+int(queryLen) > len(data) {
		return fmt.Errorf("insufficient data for query")
	}
	q.Query = string(data[pos : pos+int(queryLen)])
	pos += int(queryLen)

	// Read query ID length (4 bytes, big endian)
	if pos+4 > len(data) {
		return fmt.Errorf("insufficient data for query ID length")
	}
	queryIDLen := protocol.ReadUint32BigEndian(data[pos:])
	pos += 4

	// Read query ID
	if pos+int(queryIDLen) > len(data) {
		return fmt.Errorf("insufficient data for query ID")
	}
	q.QueryID = string(data[pos : pos+int(queryIDLen)])
	pos += int(queryIDLen)

	// Read database length (4 bytes, big endian)
	if pos+4 > len(data) {
		return fmt.Errorf("insufficient data for database length")
	}
	dbLen := protocol.ReadUint32BigEndian(data[pos:])
	pos += 4

	// Read database
	if pos+int(dbLen) > len(data) {
		return fmt.Errorf("insufficient data for database")
	}
	q.Database = string(data[pos : pos+int(dbLen)])
	pos += int(dbLen)

	// Read user length (4 bytes, big endian)
	if pos+4 > len(data) {
		return fmt.Errorf("insufficient data for user length")
	}
	userLen := protocol.ReadUint32BigEndian(data[pos:])
	pos += 4

	// Read user
	if pos+int(userLen) > len(data) {
		return fmt.Errorf("insufficient data for user")
	}
	q.User = string(data[pos : pos+int(userLen)])
	pos += int(userLen)

	// Read password length (4 bytes, big endian)
	if pos+4 > len(data) {
		return fmt.Errorf("insufficient data for password length")
	}
	pwdLen := protocol.ReadUint32BigEndian(data[pos:])
	pos += 4

	// Read password
	if pos+int(pwdLen) > len(data) {
		return fmt.Errorf("insufficient data for password")
	}
	q.Password = string(data[pos : pos+int(pwdLen)])

	return nil
}

// Size returns the estimated size of the packed message
func (q *ClientQuery) Size() int {
	// 4 bytes per length + string lengths
	return 4 + len(q.Query) + 4 + len(q.QueryID) + 4 + len(q.Database) + 4 + len(q.User) + 4 + len(q.Password)
}

// NewClientQuery creates a new client query message
func NewClientQuery(query, queryID, database, user, password string) *ClientQuery {
	return &ClientQuery{
		Query:    query,
		QueryID:  queryID,
		Database: database,
		User:     user,
		Password: password,
	}
}

// Register registers this signal type in both registry and factory
func (q *ClientQuery) Register(registry *protocol.Registry, factory *protocol.SignalFactory) error {
	// Register in registry
	if err := registry.RegisterClientSignal(q, &protocol.SignalInfo{Name: "ClientQuery"}); err != nil {
		return fmt.Errorf("failed to register ClientQuery in registry: %w", err)
	}

	// Register constructor in factory
	factory.RegisterConstructor(protocol.ClientQuery, func() protocol.Signal {
		return &ClientQuery{}
	})

	return nil
}
