package config

import (
	"encoding/json"
	"fmt"
	"time"
)

// TableConfig represents the complete configuration for a table
// This will be stored as JSON in TableMetadata.Settings field
type TableConfig struct {
	// Table identification
	TableID   string `json:"table_id"`
	TableName string `json:"table_name"`
	Database  string `json:"database"`
	
	// Storage configuration
	StorageEngine string `json:"storage_engine"`
	StoragePath   string `json:"storage_path"`
	
	// Schema management settings
	Schema TableSchemaConfig `json:"schema"`
	
	// Performance settings
	Performance TablePerformanceConfig `json:"performance"`
	
	// Metadata
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Version   int       `json:"version"`
}

// TableSchemaConfig holds table-specific schema configuration
type TableSchemaConfig struct {
	// Validation settings
	StrictValidation      bool `json:"strict_validation"`
	BatchValidationSize   int  `json:"batch_validation_size"`
	MaxValidationErrors   int  `json:"max_validation_errors"`
	
	// Evolution settings
	StrictCompliance      bool `json:"strict_compliance"`
	AllowTypePromotions   bool `json:"allow_type_promotions"`
	RequireApproval       bool `json:"require_approval"`
	MaxSchemaVersions     int  `json:"max_schema_versions"`
	
	// Custom validation rules (future feature)
	CustomRules []ValidationRule `json:"custom_rules,omitempty"`
}

// TablePerformanceConfig holds table-specific performance configuration
type TablePerformanceConfig struct {
	// Caching
	CacheEnabled    bool `json:"cache_enabled"`
	CacheSize       int  `json:"cache_size"`
	CacheTTLSeconds int  `json:"cache_ttl_seconds"`
	
	// Batch processing
	BatchSize           int  `json:"batch_size"`
	ParallelProcessing  bool `json:"parallel_processing"`
}

// ValidationRule represents a custom validation rule (future feature)
type ValidationRule struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	RuleType    string                 `json:"rule_type"`
	Parameters  map[string]interface{} `json:"parameters"`
	Enabled     bool                   `json:"enabled"`
}

// DefaultTableConfig returns a new table configuration with conservative defaults
func DefaultTableConfig() *TableConfig {
	return &TableConfig{
		Schema: TableSchemaConfig{
			StrictValidation:      true,  // Strict mode by default
			BatchValidationSize:   10000,
			MaxValidationErrors:   100,
			StrictCompliance:      true,  // Strict Iceberg compliance
			AllowTypePromotions:   false, // Future feature
			RequireApproval:       false,
			MaxSchemaVersions:     1,     // No history preservation
		},
		Performance: TablePerformanceConfig{
			CacheEnabled:          true,
			CacheSize:            1000,
			CacheTTLSeconds:      3600,
			BatchSize:            10000,
			ParallelProcessing:   true,
		},
		Version: 1,
	}
}

// NewTableConfig creates a new table configuration with the given parameters
func NewTableConfig(tableID, tableName, database, storageEngine string) *TableConfig {
	config := DefaultTableConfig()
	config.TableID = tableID
	config.TableName = tableName
	config.Database = database
	config.StorageEngine = storageEngine
	config.CreatedAt = time.Now()
	config.UpdatedAt = time.Now()
	return config
}

// Validate ensures the configuration is valid
func (tc *TableConfig) Validate() error {
	if tc.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if tc.TableName == "" {
		return fmt.Errorf("table_name is required")
	}
	if tc.Database == "" {
		return fmt.Errorf("database is required")
	}
	if tc.StorageEngine == "" {
		return fmt.Errorf("storage_engine is required")
	}

	// Validate storage engine
	validEngines := map[string]bool{
		"filesystem": true,
		"memory":     true,
		"s3":         true,
	}
	if !validEngines[tc.StorageEngine] {
		return fmt.Errorf("invalid storage engine: %s", tc.StorageEngine)
	}

	// Validate schema config
	if err := tc.Schema.Validate(); err != nil {
		return err
	}

	// Validate performance config
	if err := tc.Performance.Validate(); err != nil {
		return err
	}

	return nil
}

// Validate ensures the schema configuration is valid
func (tsc *TableSchemaConfig) Validate() error {
	if tsc.BatchValidationSize <= 0 {
		return fmt.Errorf("batch_validation_size must be positive")
	}
	if tsc.MaxValidationErrors <= 0 {
		return fmt.Errorf("max_validation_errors must be positive")
	}
	if tsc.MaxSchemaVersions <= 0 {
		return fmt.Errorf("max_schema_versions must be positive")
	}
	return nil
}

// Validate ensures the performance configuration is valid
func (tpc *TablePerformanceConfig) Validate() error {
	if tpc.CacheSize <= 0 {
		return fmt.Errorf("cache_size must be positive")
	}
	if tpc.CacheTTLSeconds <= 0 {
		return fmt.Errorf("cache_ttl_seconds must be positive")
	}
	if tpc.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be positive")
	}
	return nil
}

// UpdateSettings updates specific configuration settings
func (tc *TableConfig) UpdateSettings(updates map[string]interface{}) error {
	for key, value := range updates {
		if err := tc.updateSetting(key, value); err != nil {
			return err
		}
	}
	tc.UpdatedAt = time.Now()
	tc.Version++
	return nil
}

// updateSetting updates a single configuration setting
func (tc *TableConfig) updateSetting(key string, value interface{}) error {
	switch key {
	// Schema settings
	case "strict_validation":
		if boolVal, ok := value.(bool); ok {
			tc.Schema.StrictValidation = boolVal
		} else {
			return fmt.Errorf("invalid value for strict_validation")
		}
	case "batch_validation_size":
		if intVal, ok := value.(int); ok {
			tc.Schema.BatchValidationSize = intVal
		} else {
			return fmt.Errorf("invalid value for batch_validation_size")
		}
	case "max_validation_errors":
		if intVal, ok := value.(int); ok {
			tc.Schema.MaxValidationErrors = intVal
		} else {
			return fmt.Errorf("invalid value for max_validation_errors")
		}
	case "strict_compliance":
		if boolVal, ok := value.(bool); ok {
			tc.Schema.StrictCompliance = boolVal
		} else {
			return fmt.Errorf("invalid value for strict_compliance")
		}
	case "allow_type_promotions":
		if boolVal, ok := value.(bool); ok {
			tc.Schema.AllowTypePromotions = boolVal
		} else {
			return fmt.Errorf("invalid value for allow_type_promotions")
		}
	case "require_approval":
		if boolVal, ok := value.(bool); ok {
			tc.Schema.RequireApproval = boolVal
		} else {
			return fmt.Errorf("invalid value for require_approval")
		}
	case "max_schema_versions":
		if intVal, ok := value.(int); ok {
			tc.Schema.MaxSchemaVersions = intVal
		} else {
			return fmt.Errorf("invalid value for max_schema_versions")
		}

	// Performance settings
	case "cache_enabled":
		if boolVal, ok := value.(bool); ok {
			tc.Performance.CacheEnabled = boolVal
		} else {
			return fmt.Errorf("invalid value for cache_enabled")
		}
	case "cache_size":
		if intVal, ok := value.(int); ok {
			tc.Performance.CacheSize = intVal
		} else {
			return fmt.Errorf("invalid value for cache_size")
		}
	case "cache_ttl_seconds":
		if intVal, ok := value.(int); ok {
			tc.Performance.CacheTTLSeconds = intVal
		} else {
			return fmt.Errorf("invalid value for cache_ttl_seconds")
		}
	case "batch_size":
		if intVal, ok := value.(int); ok {
			tc.Performance.BatchSize = intVal
		} else {
			return fmt.Errorf("invalid value for batch_size")
		}
	case "parallel_processing":
		if boolVal, ok := value.(bool); ok {
			tc.Performance.ParallelProcessing = boolVal
		} else {
			return fmt.Errorf("invalid value for parallel_processing")
		}

	default:
		return fmt.Errorf("unknown configuration setting: %s", key)
	}

	return nil
}

// Clone creates a deep copy of the configuration
func (tc *TableConfig) Clone() *TableConfig {
	if tc == nil {
		return nil
	}

	config := &TableConfig{
		TableID:      tc.TableID,
		TableName:    tc.TableName,
		Database:     tc.Database,
		StorageEngine: tc.StorageEngine,
		StoragePath:   tc.StoragePath,
		Schema: TableSchemaConfig{
			StrictValidation:      tc.Schema.StrictValidation,
			BatchValidationSize:   tc.Schema.BatchValidationSize,
			MaxValidationErrors:   tc.Schema.MaxValidationErrors,
			StrictCompliance:      tc.Schema.StrictCompliance,
			AllowTypePromotions:   tc.Schema.AllowTypePromotions,
			RequireApproval:       tc.Schema.RequireApproval,
			MaxSchemaVersions:     tc.Schema.MaxSchemaVersions,
		},
		Performance: TablePerformanceConfig{
			CacheEnabled:          tc.Performance.CacheEnabled,
			CacheSize:             tc.Performance.CacheSize,
			CacheTTLSeconds:       tc.Performance.CacheTTLSeconds,
			BatchSize:             tc.Performance.BatchSize,
			ParallelProcessing:    tc.Performance.ParallelProcessing,
		},
		CreatedAt: tc.CreatedAt,
		UpdatedAt: tc.UpdatedAt,
		Version:   tc.Version,
	}

	// Deep copy custom rules if they exist
	if tc.Schema.CustomRules != nil {
		config.Schema.CustomRules = make([]ValidationRule, len(tc.Schema.CustomRules))
		copy(config.Schema.CustomRules, tc.Schema.CustomRules)
	}

	return config
}

// ToJSON converts the configuration to JSON
func (tc *TableConfig) ToJSON() ([]byte, error) {
	return json.MarshalIndent(tc, "", "  ")
}

// FromJSON creates a configuration from JSON
func FromJSON(data []byte) (*TableConfig, error) {
	var config TableConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse configuration JSON: %w", err)
	}
	return &config, nil
}

// String returns a string representation of the configuration
func (tc *TableConfig) String() string {
	return fmt.Sprintf("TableConfig{TableID: %s, TableName: %s, Database: %s, StorageEngine: %s, Version: %d}",
		tc.TableID, tc.TableName, tc.Database, tc.StorageEngine, tc.Version)
}
