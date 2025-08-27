# 🏔️ **Iceberg Schema Management Package**

This package provides a comprehensive, Iceberg-native Schema Manager, Type Manager, and Type Validator system for Ranger.

## 🎯 **Features**

- **Iceberg Type System**: Complete support for all Iceberg types (primitive, nested, parameterized)
- **Schema Management**: Schema creation, evolution, and versioning with strict Iceberg compliance
- **Data Validation**: Comprehensive validation engine for data ingestion
- **Type Conversion**: Seamless conversion between Iceberg, Arrow, Parquet, and SQL types
- **Query Parser Integration**: Type resolution and inference for query parsing
- **Future-Ready**: Architecture designed for type promotion and advanced features

## 🏗️ **Package Structure**

```
server/schema/
├── types/           # Iceberg type definitions and interfaces
├── manager/         # Schema lifecycle and evolution management
├── validator/       # Data validation and schema validation
├── converter/       # Type conversion between systems
├── registry/        # Schema persistence and retrieval
├── parser/          # Query parser integration
├── future/          # Future features (type promotion, etc.)
├── errors/          # Error code definitions
├── config/          # Configuration options
└── utils/           # Common utilities and helpers
```

## 🚀 **Quick Start**

### **Creating a Schema**

```go
package main

import (
    "github.com/gear6io/ranger/server/schema"
    "github.com/gear6io/ranger/server/schema/types"
)

func main() {
    // Create a simple schema
    userSchema := &schema.Schema{
        ID: 1,
        Fields: []types.StructField{
            {ID: 1, Name: "id", Type: &types.Primitive{KindValue: types.IcebergLong}, Required: true},
            {ID: 2, Name: "name", Type: &types.Primitive{KindValue: types.IcebergString}, Required: false},
            {ID: 3, Name: "active", Type: &types.Primitive{KindValue: types.IcebergBoolean}, Required: false},
        },
    }
    
    // Validate the schema
    validator := schema.NewValidator()
    if err := validator.ValidateSchema(userSchema); err != nil {
        panic(err)
    }
}
```

### **Validating Data**

```go
// Validate data against schema
data := [][]interface{}{
    {1, "John Doe", true},
    {2, "Jane Smith", false},
}

if err := validator.ValidateBatch(data, userSchema); err != nil {
    panic(err)
}
```

### **Type Conversion**

```go
converter := schema.NewTypeConverter()

// Convert Iceberg type to Parquet
parquetType, err := converter.ToParquet(types.IcebergLong)
if err != nil {
    panic(err)
}

// Convert Iceberg type to Arrow
arrowType, err := converter.ToArrow(types.IcebergString)
if err != nil {
    panic(err)
}
```

## 🔧 **Configuration**

The package supports configuration through environment variables and configuration files:

```go
config := &schema.Config{
    Validation: schema.ValidationConfig{
        StrictMode: true,
        BatchSize: 10000,
        ParallelValidation: true,
    },
    Evolution: schema.EvolutionConfig{
        StrictCompliance: true,
        AllowTypePromotions: false, // Future feature
    },
}
```

## 📚 **Documentation**

- [Type System](types/README.md) - Complete Iceberg type reference
- [Schema Management](manager/README.md) - Schema lifecycle and evolution
- [Validation](validator/README.md) - Data validation rules and engine
- [Type Conversion](converter/README.md) - Converting between type systems
- [Registry](registry/README.md) - Schema persistence and retrieval
- [Parser Integration](parser/README.md) - Query parser type resolution

## 🧪 **Testing**

Run the test suite:

```bash
go test ./server/schema/...
```

Run benchmarks:

```bash
go test -bench=. ./server/schema/...
```

## 🔮 **Future Features**

- **Type Promotion System**: Automatic type widening and promotion
- **Schema Rollback**: Version history and rollback capabilities
- **Custom Validation Rules**: Extensible validation rule system
- **Schema Analytics**: Performance impact analysis and optimization

## 📄 **License**

This package is part of Ranger and follows the same license terms.
