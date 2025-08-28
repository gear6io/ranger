package schema_manager

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/uptrace/bun"
)

// MockMetadataManager is a mock implementation of MetadataManagerInterface
type MockMetadataManager struct {
	mock.Mock
}

func (m *MockMetadataManager) GetBunDB() *bun.DB {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*bun.DB)
}

// MockBunDB is a mock implementation of bun.DB
type MockBunDB struct {
	mock.Mock
}

func (m *MockBunDB) NewSelect() *MockSelectQuery {
	args := m.Called()
	return args.Get(0).(*MockSelectQuery)
}

type MockSelectQuery struct {
	mock.Mock
}

func (m *MockSelectQuery) Model(model interface{}) *MockSelectQuery {
	m.Called(model)
	return m
}

func (m *MockSelectQuery) Where(query string, args ...interface{}) *MockSelectQuery {
	m.Called(query, args)
	return m
}

func (m *MockSelectQuery) Order(order string) *MockSelectQuery {
	m.Called(order)
	return m
}

func (m *MockSelectQuery) Scan(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
