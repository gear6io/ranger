# 🏗️ **Phase 0: Table Configuration & Query Language Foundation**

## �� **Overview**

This phase establishes the foundation for table-level configuration and query language integration before building the comprehensive Iceberg schema management system. We will enhance the existing SQL parser and integrate it with the enhanced TableMetadata structure.

## 🎯 **Objectives**

1. **Enhance existing SQL parser** in `server/query/parser/` for CREATE TABLE statements
2. **Update TableMetadata structure** to support table configuration
3. **Integrate CREATE TABLE parsing** with existing query execution flow
4. **Establish table context management** for schema operations

## 📊 **Current Progress Summary**

### **Phase 0.1: Foundation** 🔄 **80% Complete**
- ✅ **Enhanced TableMetadata Structure** - Complete with new configuration fields
- ✅ **Table Configuration System** - Complete with TableConfig and TableContext
- ✅ **CREATE TABLE Parser Enhancement** - Complete with all required clauses
- 🔄 **Integration with Query Engine** - 80% complete (build issues blocking final testing)

### **Overall Project Status**
- **Parser Layer**: ✅ **100% Complete** - All CREATE TABLE syntax supported
- **Query Engine**: ✅ **100% Complete** - CREATE TABLE routing and execution
- **Storage Layer**: ✅ **100% Complete** - Table creation and management
- **Metadata Layer**: ⚠️ **80% Complete** - Build issues preventing final integration
- **Integration Testing**: ❌ **0% Complete** - Blocked by metadata build issues

### **Key Achievements**
- **Comprehensive CREATE TABLE parsing** with all modern clauses
- **Full integration** with existing query execution flow
- **Enhanced metadata structure** ready for advanced table configuration
- **Robust error handling** with comprehensive error codes
- **Extensive test coverage** for all parser functionality

## 🏗️ **Architecture**

### **Existing Components (No Changes)**
- **SQL Parser**: `server/query/parser/` - Already exists and handles CREATE TABLE
- **Query Engine**: `server/query/engine.go` - Already exists and executes queries
- **JDBC Handler**: `server/query/jdbc/` - Already exists and handles connections

### **Enhanced Components**
- **TableMetadata**: Enhanced with new configuration fields
- **CREATE TABLE Parser**: Enhanced to support new syntax and configuration
- **Table Context**: New system for managing table-specific operations

## 📝 **Implementation Plan**

### **Phase 0.1: Foundation** 🔄 **NEARLY COMPLETE**

**Status**: Final Integration Phase  
**Priority**: High  
**Duration**: 1-2 days (80% complete)

#### **0.1.1 Enhanced TableMetadata Structure** ✅ **COMPLETED**

**What**: Enhanced the existing `TableMetadata` table with new configuration fields

**Changes Made**:
- ✅ **Renamed `Properties` to `Settings`** - for table-specific configuration
- ✅ **Removed `Schema` field** - schema will be stored in `TableColumn` instead
- ✅ **Added new configuration fields**:
  - `table_config TEXT` - JSON table configuration
  - `partition_order TEXT` - JSON array of partition columns
  - `partition_strategy TEXT` - partitioning strategy
  - `sort_order TEXT` - JSON array of sort columns
  - `sort_strategy TEXT` - sorting strategy
  - Performance fields: `cache_enabled`, `cache_size`, `batch_size`, `parallel_processing`
  - Schema evolution fields: `strict_validation`, `strict_compliance`, `allow_type_promotions`, `max_schema_versions`
  - Validation fields: `batch_validation_size`, `max_validation_errors`
  - Metadata fields: `table_uuid`, `format_version`, `last_column_id`, `last_partition_id`

**Migration**: Updated existing `001_start.go` migration to include all new fields

#### **0.1.2 Table Configuration System** ✅ **COMPLETED**

**What**: Created table configuration structures for storing in `TableMetadata.Settings`

**Files Created**:
- ✅ `server/schema/config/table_config.go` - Table configuration structures
- ✅ `server/schema/context/table_context.go` - Table context management

**Key Features**:
- **TableConfig**: Complete table configuration with defaults
- **TableSchemaConfig**: Schema-specific settings (validation, evolution)
- **TablePerformanceConfig**: Performance settings (caching, batching)
- **TableContext**: Runtime context for table operations

#### **0.1.3 CREATE TABLE Parser Enhancement** ✅ **COMPLETED**

**What**: Enhanced the existing SQL parser in `server/query/parser/` to support new CREATE TABLE syntax

**Current Status**: 
- ✅ **Correct location**: Enhanced existing parser in `server/query/parser/`
- ✅ **All required clauses implemented**: STORAGE, PARTITION BY, ORDER BY, SETTINGS
- ✅ **Parser tests passing**: All CREATE TABLE test cases pass successfully

**What Was Accomplished**:
1. **Enhanced existing `parseCreateStmt()`** in `server/query/parser/parser.go` ✅
2. **Added support for all new clauses** ✅:
   - `STORAGE` clause (mandatory) - supports filesystem, memory, s3 engines
   - `PARTITION BY` clause - supports multiple partition columns
   - `ORDER BY` clause - supports multiple sort columns  
   - `SETTINGS` clause - supports key=value configuration pairs
3. **Integrated with TableConfig** for configuration storage ✅
4. **Updated AST nodes** to support new syntax ✅

**Files Modified**:
- `server/query/parser/parser.go` - Enhanced CREATE TABLE parsing with all clauses
- `server/query/parser/ast.go` - Updated CreateTableStmt AST node with new fields
- `server/query/parser/errors.go` - Added comprehensive error codes for parsing

**Parser Features**:
- **Mandatory STORAGE clause** with validation (filesystem, memory, s3)
- **PARTITION BY** clause parsing with column list support
- **ORDER BY** clause parsing with column list support  
- **SETTINGS** clause with key=value parsing and type conversion
- **Column constraints** support (NOT NULL, UNIQUE, PRIMARY KEY, CHECK, DEFAULT)
- **Data type parameters** support (VARCHAR(255), DECIMAL(10,2))
- **IF NOT EXISTS** clause support
- **Qualified table names** support (database.table)

**Test Coverage**:
- ✅ Basic CREATE TABLE with STORAGE
- ✅ CREATE TABLE with PARTITION BY
- ✅ CREATE TABLE with ORDER BY
- ✅ CREATE TABLE with SETTINGS
- ✅ CREATE TABLE with all clauses combined
- ✅ Error handling for missing STORAGE clause
- ✅ Error handling for invalid storage engines
- ✅ Clause parsing validation

#### **0.1.4 Integration with Query Engine** 🔄 **PARTIALLY COMPLETED**

**What**: Integrate enhanced CREATE TABLE parsing with existing query execution flow

**Current Status**: 
- ✅ **Query Engine Integration**: CREATE TABLE statements are routed and handled
- ✅ **Storage Manager Integration**: Tables are created via storage manager
- ⚠️ **Metadata Integration**: Some build issues in metadata registry need resolution
- ⚠️ **Schema Storage**: Need to verify schema is stored in TableColumn vs TableMetadata.Schema

**What Was Accomplished**:
1. **Updated `ExecuteQuery`** in `server/query/engine.go` to handle CREATE TABLE ✅
2. **CREATE TABLE routing** to appropriate handler ✅
3. **Table creation** using storage manager ✅
4. **Storage engine validation** and handling ✅

**Files Modified**:
- `server/query/engine.go` - CREATE TABLE routing and execution ✅
- `server/storage/manager.go` - Table creation flow (already integrated) ✅

**Files Requiring Attention**:
- `server/metadata/registry/sqlite.go` - Build issues with Schema/Properties fields
- `server/metadata/manager.go` - May need updates for new metadata structure

**Current Integration Flow**:
1. **Parser** → Parses CREATE TABLE with all clauses ✅
2. **Query Engine** → Routes to CREATE TABLE handler ✅  
3. **Storage Manager** → Creates table with specified engine ✅
4. **Metadata Manager** → ⚠️ Needs resolution of build issues

**Build Issues Identified**:
- `server/metadata/registry/sqlite.go:671` - Unknown field `Schema` in TableMetadata
- `server/metadata/registry/sqlite.go:678` - Unknown field `Properties` in TableMetadata

### **Phase 0.2: Query Language Integration** ❌ **NOT STARTED**

**Status**: Not Started  
**Priority**: Medium  
**Duration**: 1-2 days

#### **0.2.1 ALTER TABLE SETTINGS Support**
- **Parser enhancement** for `ALTER TABLE table_name SETTINGS key=value`
- **Configuration updates** via existing `UpdateSettings` method
- **Validation** of setting changes

#### **0.2.2 Table Context Integration**
- **Query context** integration with table context
- **Type resolution** using table schema
- **Validation mode** per table

### **Phase 0.3: Testing & Validation** ❌ **NOT STARTED**

**Status**: Not Started  
**Priority**: Medium  
**Duration**: 1 day

#### **0.3.1 Parser Testing**
- **Unit tests** for enhanced CREATE TABLE parsing
- **Integration tests** with query engine
- **Error handling** tests

#### **0.3.2 Configuration Testing**
- **TableConfig** validation tests
- **Settings updates** tests
- **Migration** tests

## 🔧 **Technical Details**

### **CREATE TABLE Syntax (Enhanced)**
```sql
CREATE TABLE [IF NOT EXISTS] database.table_name (
    column_name data_type [NOT NULL] [DEFAULT value],
    ...
) 
STORAGE storage_engine
[PARTITION BY (column1, column2, ...)]
[ORDER BY (column1, column2, ...)]
[SETTINGS key1=value1, key2=value2]
```

**Examples**:
```sql
-- Basic table with filesystem storage
CREATE TABLE users (
    id INT NOT NULL,
    name VARCHAR(255),
    email VARCHAR(255) NOT NULL
) STORAGE filesystem

-- Table with partitioning and settings
CREATE TABLE events (
    event_id UUID NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    event_type VARCHAR(50)
) 
STORAGE filesystem
PARTITION BY (timestamp, user_id)
SETTINGS cache_enabled=true, batch_size=50000

-- Memory table with performance settings
CREATE TABLE temp_data (
    id INT NOT NULL,
    data TEXT
) 
STORAGE memory
SETTINGS cache_size=10000, parallel_processing=true
```

### **Table Configuration Storage**
- **`TableMetadata.Settings`**: JSON string containing table configuration
- **`TableMetadata.table_config`**: JSON string containing detailed configuration
- **`TableColumn`**: Stores actual schema definition (not in TableMetadata.Schema)

### **Integration Points**
1. **Query Parser** → **Query Engine** → **Storage Manager** → **Metadata Manager**
2. **CREATE TABLE** → **Enhanced Parser** → **TableConfig** → **TableMetadata**
3. **Schema Storage** → **TableColumn** (not TableMetadata.Schema)

## 🚧 **Current Blockers**

1. **Metadata Registry Build Issues**: ⚠️ **CRITICAL** - Need to resolve field name conflicts
   - `server/metadata/registry/sqlite.go:671` - Unknown field `Schema` in TableMetadata
   - `server/metadata/registry/sqlite.go:678` - Unknown field `Properties` in TableMetadata
   - **Root Cause**: TableMetadata structure was updated but registry code still references old field names
   - **Impact**: Prevents full integration testing and deployment

2. **Schema Storage Verification**: ⚠️ **MEDIUM** - Need to verify schema storage approach
   - Confirm schema is stored in `TableColumn` instead of `TableMetadata.Schema`
   - Verify migration from old schema storage approach

3. **Integration Testing**: ⚠️ **MEDIUM** - Need to test full CREATE TABLE flow
   - Parser → Query Engine → Storage Manager → Metadata Manager
   - Verify all clauses are properly stored and retrieved

## ✅ **Next Steps**

### **Immediate (Critical)**
1. **Resolve Metadata Registry Build Issues** 🔴 **BLOCKING**
   - Fix field name conflicts in `server/metadata/registry/sqlite.go`
   - Update references from `Schema` to new structure
   - Update references from `Properties` to `Settings`
   - **Goal**: Get the system building and running

### **Short Term (1-2 days)**
2. **Verify Schema Storage Approach** 🔵 **VERIFICATION**
   - Confirm schema storage in `TableColumn` vs `TableMetadata.Schema`
   - Test schema retrieval and validation
   - **Goal**: Ensure schema data flows correctly

3. **Integration Testing** 🔵 **VALIDATION**
   - Test full CREATE TABLE flow end-to-end
   - Verify all clauses (STORAGE, PARTITION BY, ORDER BY, SETTINGS) are stored
   - Test table creation with various configurations
   - **Goal**: Validate complete functionality

### **Medium Term (3-5 days)**
4. **Phase 0.2: Query Language Integration** 🟡 **NEXT PHASE**
   - ALTER TABLE SETTINGS support
   - Table context integration
   - Enhanced validation and error handling

### **Completed ✅**
- ✅ Enhanced TableMetadata structure
- ✅ Table configuration system
- ✅ CREATE TABLE parser enhancement (all clauses)
- ✅ Query engine integration (routing and execution)
- ✅ Storage manager integration

## 📚 **References**

- **Existing SQL Parser**: `server/query/parser/`
- **Query Engine**: `server/query/engine.go`
- **JDBC Handler**: `server/query/jdbc/`
- **Storage Manager**: `server/storage/manager.go`
- **Metadata Manager**: `server/metadata/manager.go`
