package signals

import (
	"testing"

	"github.com/gear6io/ranger/server/protocols/native/protocol"
)

func TestClientQuery(t *testing.T) {
	// Test creating a new client query
	query := NewClientQuery("SELECT * FROM users", "query123", "testdb", "testuser", "testpass")

	// Test Type method
	if query.Type() != protocol.ClientQuery {
		t.Errorf("Expected Type() to return ClientQuery, got %d", query.Type())
	}

	// Test Size method
	expectedSize := 4 + len("SELECT * FROM users") + 4 + len("query123") + 4 + len("testdb") + 4 + len("testuser") + 4 + len("testpass")
	if query.Size() != expectedSize {
		t.Errorf("Expected Size() to return %d, got %d", expectedSize, query.Size())
	}

	// Test Pack method
	packed, err := query.Pack()
	if err != nil {
		t.Fatalf("Pack() failed: %v", err)
	}

	if len(packed) == 0 {
		t.Error("Pack() returned empty data")
	}

	// Test Unpack method
	newQuery := &ClientQuery{}
	err = newQuery.Unpack(packed)
	if err != nil {
		t.Fatalf("Unpack() failed: %v", err)
	}

	// Verify all fields were unpacked correctly
	if newQuery.Query != query.Query {
		t.Errorf("Query mismatch: expected %s, got %s", query.Query, newQuery.Query)
	}
	if newQuery.QueryID != query.QueryID {
		t.Errorf("QueryID mismatch: expected %s, got %s", query.QueryID, newQuery.QueryID)
	}
	if newQuery.Database != query.Database {
		t.Errorf("Database mismatch: expected %s, got %s", query.Database, newQuery.Database)
	}
	if newQuery.User != query.User {
		t.Errorf("User mismatch: expected %s, got %s", query.User, newQuery.User)
	}
	if newQuery.Password != query.Password {
		t.Errorf("Password mismatch: expected %s, got %s", query.Password, newQuery.Password)
	}
}

func TestClientQueryUnpackEmpty(t *testing.T) {
	query := &ClientQuery{}
	err := query.Unpack([]byte{})
	if err == nil {
		t.Error("Expected error when unpacking empty data")
	}
}

func TestClientQueryUnpackInsufficient(t *testing.T) {
	query := &ClientQuery{}
	err := query.Unpack([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}) // Too short
	if err == nil {
		t.Error("Expected error when unpacking insufficient data")
	}
}
