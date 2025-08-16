package storage

import (
	"fmt"
	"strings"
)

// EngineType represents the type of storage engine
type EngineType string

const (
	// FILESYSTEM represents local filesystem storage
	FILESYSTEM EngineType = "FILESYSTEM"
	// MEMORY represents in-memory storage
	MEMORY EngineType = "MEMORY"
	// S3 represents S3-compatible object storage
	S3 EngineType = "S3"
)

// String returns the string representation of the engine type
func (e EngineType) String() string {
	return string(e)
}

// IsValid checks if the engine type is valid
func (e EngineType) IsValid() bool {
	switch e {
	case FILESYSTEM, MEMORY, S3:
		return true
	default:
		return false
	}
}

// ParseEngineType parses a string into an EngineType
func ParseEngineType(s string) (EngineType, error) {
	engineType := EngineType(s)
	if !engineType.IsValid() {
		return "", fmt.Errorf("invalid engine type: %s", s)
	}
	return engineType, nil
}

// MustParseEngineType parses a string into an EngineType, panics on error
func MustParseEngineType(s string) EngineType {
	engineType, err := ParseEngineType(s)
	if err != nil {
		panic(err)
	}
	return engineType
}

// ListValidEngineTypes returns a list of all valid engine types
func ListValidEngineTypes() []EngineType {
	return []EngineType{FILESYSTEM, MEMORY, S3}
}

// GetDefaultEngineType returns the default engine type
func GetDefaultEngineType() EngineType {
	return FILESYSTEM
}

// FromString converts a string to EngineType (case-insensitive)
func FromString(s string) (EngineType, error) {
	switch strings.ToUpper(s) {
	case "FILESYSTEM", "FS":
		return FILESYSTEM, nil
	case "MEMORY", "MEM":
		return MEMORY, nil
	case "S3":
		return S3, nil
	default:
		return "", fmt.Errorf("invalid engine type: %s", s)
	}
}

// MustFromString converts a string to EngineType, panics on error
func MustFromString(s string) EngineType {
	engineType, err := FromString(s)
	if err != nil {
		panic(err)
	}
	return engineType
}
