package memory

import (
	"bytes"
	"io"
	"testing"

	"github.com/TFMV/icebox/pkg/errors"
)

func TestMemoryStorage_NewMemoryStorage(t *testing.T) {
	storage, err := NewMemoryStorage()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if storage == nil {
		t.Fatal("Expected storage instance, got nil")
	}
}

func TestMemoryStorage_Open_FileNotFound(t *testing.T) {
	storage, _ := NewMemoryStorage()

	_, err := storage.Open("nonexistent.txt")
	if err == nil {
		t.Fatal("Expected error for non-existent file")
	}

	// Check if it's our structured error
	if iceboxErr, ok := err.(*errors.Error); ok {
		if iceboxErr.Code.String() != ErrFileNotFound.String() {
			t.Errorf("Expected error code '%s', got: %s", ErrFileNotFound.String(), iceboxErr.Code.String())
		}
		if iceboxErr.Context["path"] != "nonexistent.txt" {
			t.Errorf("Expected path context 'nonexistent.txt', got: %s", iceboxErr.Context["path"])
		}
	} else {
		t.Fatal("Expected structured error from pkg/errors")
	}
}

func TestMemoryStorage_ReadFile_FileNotFound(t *testing.T) {
	storage, _ := NewMemoryStorage()

	_, err := storage.ReadFile("nonexistent.txt")
	if err == nil {
		t.Fatal("Expected error for non-existent file")
	}

	// Check if it's our structured error
	if iceboxErr, ok := err.(*errors.Error); ok {
		if iceboxErr.Code.String() != ErrFileNotFound.String() {
			t.Errorf("Expected error code '%s', got: %s", ErrFileNotFound.String(), iceboxErr.Code.String())
		}
		if iceboxErr.Context["path"] != "nonexistent.txt" {
			t.Errorf("Expected path context 'nonexistent.txt', got: %s", iceboxErr.Context["path"])
		}
	} else {
		t.Fatal("Expected structured error from pkg/errors")
	}
}

func TestMemoryStorage_Remove_FileNotFound(t *testing.T) {
	storage, _ := NewMemoryStorage()

	err := storage.Remove("nonexistent.txt")
	if err == nil {
		t.Fatal("Expected error for non-existent file")
	}

	// Check if it's our structured error
	if iceboxErr, ok := err.(*errors.Error); ok {
		if iceboxErr.Code.String() != ErrFileNotFound.String() {
			t.Errorf("Expected error code '%s', got: %s", ErrFileNotFound.String(), iceboxErr.Code.String())
		}
		if iceboxErr.Context["path"] != "nonexistent.txt" {
			t.Errorf("Expected path context 'nonexistent.txt', got: %s", iceboxErr.Context["path"])
		}
	} else {
		t.Fatal("Expected structured error from pkg/errors")
	}
}

func TestMemoryStorage_OpenForRead_FileNotFound(t *testing.T) {
	storage, _ := NewMemoryStorage()

	_, err := storage.OpenForRead("nonexistent.txt")
	if err == nil {
		t.Fatal("Expected error for non-existent file")
	}

	// Check if it's our structured error
	if iceboxErr, ok := err.(*errors.Error); ok {
		if iceboxErr.Code.String() != ErrFileNotFound.String() {
			t.Errorf("Expected error code '%s', got: %s", ErrFileNotFound.String(), iceboxErr.Code.String())
		}
		if iceboxErr.Context["path"] != "nonexistent.txt" {
			t.Errorf("Expected path context 'nonexistent.txt', got: %s", iceboxErr.Context["path"])
		}
	} else {
		t.Fatal("Expected structured error from pkg/errors")
	}
}

func TestMemoryStorage_PrepareTableEnvironment_AlreadyExists(t *testing.T) {
	storage, _ := NewMemoryStorage()

	// First time should succeed
	err := storage.PrepareTableEnvironment("test_db", "test_table")
	if err != nil {
		t.Fatalf("Expected no error on first creation, got: %v", err)
	}

	// Second time should also succeed (filesystem operations are idempotent)
	err = storage.PrepareTableEnvironment("test_db", "test_table")
	if err != nil {
		t.Fatalf("Expected no error on second creation (should be idempotent), got: %v", err)
	}

	// Verify the table environment exists
	exists, err := storage.Exists("tables/test_db/test_table/data/data.json")
	if err != nil {
		t.Fatalf("Expected no error checking table existence, got: %v", err)
	}
	if !exists {
		t.Fatal("Expected table environment to exist after creation")
	}
}

func TestMemoryStorage_StoreTableData_TableNotFound(t *testing.T) {
	storage, _ := NewMemoryStorage()

	// Try to store data for a non-existent table - should create it automatically
	err := storage.StoreTableData("test_db", "nonexistent_table", []byte("data"))
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify the data was stored
	data, err := storage.GetTableData("test_db", "nonexistent_table")
	if err != nil {
		t.Fatalf("Expected no error reading stored data, got: %v", err)
	}
	if !bytes.Equal(data, []byte("data")) {
		t.Fatalf("Expected stored data to match, got: %v, want: %v", data, []byte("data"))
	}
}

func TestMemoryStorage_GetTableData_TableNotFound(t *testing.T) {
	storage, _ := NewMemoryStorage()

	// Try to get data from a non-existent table - should return empty data
	data, err := storage.GetTableData("test_db", "nonexistent_table")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if !bytes.Equal(data, []byte("[]")) {
		t.Fatalf("Expected empty JSON array for non-existent table, got: %v", data)
	}
}

func TestMemoryStorage_GetTableData_DataNotFound(t *testing.T) {
	storage, _ := NewMemoryStorage()

	// Create table environment but don't store data
	err := storage.PrepareTableEnvironment("test_db", "test_table")
	if err != nil {
		t.Fatalf("Expected no error on table creation, got: %v", err)
	}

	// Try to get data from empty table - should return empty data
	data, err := storage.GetTableData("test_db", "test_table")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if !bytes.Equal(data, []byte("[]")) {
		t.Fatalf("Expected empty JSON array for new table, got: %v", data)
	}
}

func TestMemoryStorage_RemoveTableEnvironment_TableNotFound(t *testing.T) {
	storage, _ := NewMemoryStorage()

	// Try to remove a non-existent table - should not error
	err := storage.RemoveTableEnvironment("test_db", "nonexistent_table")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
}

func TestMemoryStorage_SuccessfulOperations(t *testing.T) {
	storage, _ := NewMemoryStorage()

	// Test successful file operations
	testData := []byte("test content")

	// Write file
	err := storage.WriteFile("test.txt", testData)
	if err != nil {
		t.Fatalf("Expected no error on WriteFile, got: %v", err)
	}

	// Check if file exists
	exists, err := storage.Exists("test.txt")
	if err != nil {
		t.Fatalf("Expected no error on Exists, got: %v", err)
	}
	if !exists {
		t.Fatal("Expected file to exist after WriteFile")
	}

	// Read file
	data, err := storage.ReadFile("test.txt")
	if err != nil {
		t.Fatalf("Expected no error on ReadFile, got: %v", err)
	}
	if !bytes.Equal(data, testData) {
		t.Fatalf("Expected data to match, got: %v, want: %v", data, testData)
	}

	// Test successful table operations
	err = storage.PrepareTableEnvironment("test_db", "test_table")
	if err != nil {
		t.Fatalf("Expected no error on PrepareTableEnvironment, got: %v", err)
	}

	err = storage.StoreTableData("test_db", "test_table", testData)
	if err != nil {
		t.Fatalf("Expected no error on StoreTableData, got: %v", err)
	}

	data, err = storage.GetTableData("test_db", "test_table")
	if err != nil {
		t.Fatalf("Expected no error on GetTableData, got: %v", err)
	}
	if !bytes.Equal(data, testData) {
		t.Fatalf("Expected table data to match, got: %v, want: %v", data, testData)
	}

	// Test streaming operations
	reader, err := storage.OpenForRead("test.txt")
	if err != nil {
		t.Fatalf("Expected no error on OpenForRead, got: %v", err)
	}
	defer reader.Close()

	writer, err := storage.OpenForWrite("test2.txt")
	if err != nil {
		t.Fatalf("Expected no error on OpenForWrite, got: %v", err)
	}

	_, err = io.Copy(writer, reader)
	if err != nil {
		t.Fatalf("Expected no error on copy operation, got: %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Expected no error on writer.Close, got: %v", err)
	}

	// Verify copied data
	data2, err := storage.ReadFile("test2.txt")
	if err != nil {
		t.Fatalf("Expected no error on ReadFile for copied file, got: %v", err)
	}
	if !bytes.Equal(data2, testData) {
		t.Fatalf("Expected copied data to match, got: %v, want: %v", data2, testData)
	}

	// Clean up
	err = storage.Remove("test.txt")
	if err != nil {
		t.Fatalf("Expected no error on Remove, got: %v", err)
	}

	err = storage.Remove("test2.txt")
	if err != nil {
		t.Fatalf("Expected no error on Remove, got: %v", err)
	}

	err = storage.RemoveTableEnvironment("test_db", "test_table")
	if err != nil {
		t.Fatalf("Expected no error on RemoveTableEnvironment, got: %v", err)
	}
}
