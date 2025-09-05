package schema

import (
	"context"

	"github.com/gear6io/ranger/server/metadata/registry"
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

// MockRegistryStore is a mock implementation of RegistryStoreInterface
type MockRegistryStore struct {
	mock.Mock
}

func (m *MockRegistryStore) RetrieveAllSchemas(ctx context.Context) (map[string]*registry.SchemaData, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]*registry.SchemaData), args.Error(1)
}

func (m *MockRegistryStore) CreateSchemaDataLoader() func(ctx context.Context, database, tableName string) (*registry.SchemaData, error) {
	args := m.Called()
	return args.Get(0).(func(ctx context.Context, database, tableName string) (*registry.SchemaData, error))
}
