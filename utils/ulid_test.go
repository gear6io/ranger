package utils

import (
	"testing"
	"time"
)

func TestGenerateULID(t *testing.T) {
	// Test basic ULID generation
	ulid1 := GenerateULID()
	ulid2 := GenerateULID()

	if ulid1.String() == ulid2.String() {
		t.Error("Generated ULIDs should be different")
	}

	if len(ulid1.String()) != 26 {
		t.Errorf("ULID should be 26 characters, got %d", len(ulid1.String()))
	}
}

func TestGenerateULIDString(t *testing.T) {
	ulidStr := GenerateULIDString()

	if len(ulidStr) != 26 {
		t.Errorf("ULID string should be 26 characters, got %d", len(ulidStr))
	}
}

func TestGenerateULIDWithTime(t *testing.T) {
	now := time.Now()
	ulid := GenerateULIDWithTime(now)

	if len(ulid.String()) != 26 {
		t.Errorf("ULID should be 26 characters, got %d", len(ulid.String()))
	}
}

func TestParseULID(t *testing.T) {
	original := GenerateULID()
	parsed, err := ParseULID(original.String())

	if err != nil {
		t.Errorf("Failed to parse ULID: %v", err)
	}

	if original.String() != parsed.String() {
		t.Error("Parsed ULID should match original")
	}
}

func TestMustParseULID(t *testing.T) {
	original := GenerateULID()
	parsed := MustParseULID(original.String())

	if original.String() != parsed.String() {
		t.Error("Parsed ULID should match original")
	}
}
