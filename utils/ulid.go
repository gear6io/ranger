package utils

import (
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

var (
	entropyLock sync.Mutex
)

// GenerateULID generates a new ULID with mutex protection
// This ensures no two ULIDs are the same even in concurrent scenarios
func GenerateULID() ulid.ULID {
	entropyLock.Lock()
	defer entropyLock.Unlock()

	return ulid.Make()
}

// GenerateULIDString generates a new ULID as a string
func GenerateULIDString() string {
	return GenerateULID().String()
}

// GenerateULIDWithTime generates a ULID with a specific timestamp
func GenerateULIDWithTime(t time.Time) ulid.ULID {
	entropyLock.Lock()
	defer entropyLock.Unlock()

	// For now, just generate a new ULID with current time
	// TODO: Implement proper timestamp-based generation
	return ulid.Make()
}

// ParseULID parses a ULID string
func ParseULID(s string) (ulid.ULID, error) {
	return ulid.Parse(s)
}

// MustParseULID parses a ULID string, panics on error
func MustParseULID(s string) ulid.ULID {
	return ulid.MustParse(s)
}
