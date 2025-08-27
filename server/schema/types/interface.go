package types

import (
	"fmt"
	"hash/fnv"
)

// IcebergTypeKind represents the kind of Iceberg type
type IcebergTypeKind string

const (
	// Primitive types
	IcebergBoolean     IcebergTypeKind = "boolean"
	IcebergInt         IcebergTypeKind = "int"
	IcebergLong        IcebergTypeKind = "long"
	IcebergFloat       IcebergTypeKind = "float"
	IcebergDouble      IcebergTypeKind = "double"
	IcebergString      IcebergTypeKind = "string"
	IcebergBinary      IcebergTypeKind = "binary"
	IcebergDate        IcebergTypeKind = "date"
	IcebergTime        IcebergTypeKind = "time"
	IcebergTimestamp   IcebergTypeKind = "timestamp"
	IcebergTimestamptz IcebergTypeKind = "timestamptz"
	IcebergUUID        IcebergTypeKind = "uuid"

	// Parameterized types
	IcebergDecimal IcebergTypeKind = "decimal"
	IcebergFixed   IcebergTypeKind = "fixed"

	// Nested types
	IcebergList   IcebergTypeKind = "list"
	IcebergMap    IcebergTypeKind = "map"
	IcebergStruct IcebergTypeKind = "struct"
)

// IcebergType is the base interface for all Iceberg types
type IcebergType interface {
	// Kind returns the type kind
	Kind() IcebergTypeKind

	// Children returns child types for nested types, empty slice for primitives
	Children() []IcebergType

	// String returns a string representation of the type
	String() string

	// Equals checks if this type equals another type
	Equals(other IcebergType) bool

	// Hash returns a hash value for the type
	Hash() uint64

	// IsPrimitive returns true if this is a primitive type
	IsPrimitive() bool

	// IsNested returns true if this is a nested type
	IsNested() bool

	// IsParameterized returns true if this is a parameterized type
	IsParameterized() bool
}

// TypeVisitor defines the visitor pattern for type traversal
type TypeVisitor interface {
	VisitPrimitive(primitive *Primitive) error
	VisitDecimal(decimal *Decimal) error
	VisitFixed(fixed *Fixed) error
	VisitList(list *List) error
	VisitMap(mapType *Map) error
	VisitStruct(structType *Struct) error
}

// AcceptVisitor allows types to accept visitors
type AcceptVisitor interface {
	Accept(visitor TypeVisitor) error
}

// baseType provides common functionality for all types
type baseType struct {
	kind IcebergTypeKind
}

func (b *baseType) Kind() IcebergTypeKind {
	return b.kind
}

func (b *baseType) IsPrimitive() bool {
	switch b.kind {
	case IcebergBoolean, IcebergInt, IcebergLong, IcebergFloat, IcebergDouble,
		IcebergString, IcebergBinary, IcebergDate, IcebergTime, IcebergTimestamp,
		IcebergTimestamptz, IcebergUUID:
		return true
	default:
		return false
	}
}

func (b *baseType) IsNested() bool {
	switch b.kind {
	case IcebergList, IcebergMap, IcebergStruct:
		return true
	default:
		return false
	}
}

func (b *baseType) IsParameterized() bool {
	switch b.kind {
	case IcebergDecimal, IcebergFixed:
		return true
	default:
		return false
	}
}

// hashString returns a hash of the given string
func hashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// hashType returns a hash of the type kind
func hashType(kind IcebergTypeKind) uint64 {
	return hashString(string(kind))
}

// TypeRegistry holds all supported types
type TypeRegistry struct {
	types map[IcebergTypeKind]IcebergType
}

// NewTypeRegistry creates a new type registry
func NewTypeRegistry() *TypeRegistry {
	registry := &TypeRegistry{
		types: make(map[IcebergTypeKind]IcebergType),
	}

	// Register all primitive types
	registry.Register(&Primitive{baseType: baseType{kind: IcebergBoolean}})
	registry.Register(&Primitive{baseType: baseType{kind: IcebergInt}})
	registry.Register(&Primitive{baseType: baseType{kind: IcebergLong}})
	registry.Register(&Primitive{baseType: baseType{kind: IcebergFloat}})
	registry.Register(&Primitive{baseType: baseType{kind: IcebergDouble}})
	registry.Register(&Primitive{baseType: baseType{kind: IcebergString}})
	registry.Register(&Primitive{baseType: baseType{kind: IcebergBinary}})
	registry.Register(&Primitive{baseType: baseType{kind: IcebergDate}})
	registry.Register(&Primitive{baseType: baseType{kind: IcebergTime}})
	registry.Register(&Primitive{baseType: baseType{kind: IcebergTimestamp}})
	registry.Register(&Primitive{baseType: baseType{kind: IcebergTimestamptz}})
	registry.Register(&Primitive{baseType: baseType{kind: IcebergUUID}})

	return registry
}

// Register adds a type to the registry
func (tr *TypeRegistry) Register(typ IcebergType) {
	tr.types[typ.Kind()] = typ
}

// Get retrieves a type by kind
func (tr *TypeRegistry) Get(kind IcebergTypeKind) (IcebergType, bool) {
	typ, exists := tr.types[kind]
	return typ, exists
}

// List returns all registered types
func (tr *TypeRegistry) List() []IcebergType {
	types := make([]IcebergType, 0, len(tr.types))
	for _, typ := range tr.types {
		types = append(types, typ)
	}
	return types
}

// Global type registry instance
var GlobalTypeRegistry = NewTypeRegistry()

// GetType returns a type from the global registry
func GetType(kind IcebergTypeKind) (IcebergType, error) {
	if typ, exists := GlobalTypeRegistry.Get(kind); exists {
		return typ, nil
	}
	return nil, fmt.Errorf("unknown type kind: %s", kind)
}
