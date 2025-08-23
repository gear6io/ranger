package iceberg

// Avro schema definitions for Iceberg compliance
// Based on Apache Iceberg specification: https://iceberg.apache.org/spec/

const (
	// ManifestEntrySchema defines the Avro schema for manifest entries
	ManifestEntrySchema = `{
		"type": "record",
		"name": "manifest_entry",
		"namespace": "org.apache.iceberg",
		"fields": [
			{
				"name": "status",
				"type": "int",
				"doc": "Status of the file: 0 = existing, 1 = added, 2 = deleted"
			},
			{
				"name": "snapshot_id",
				"type": "long",
				"doc": "ID of the snapshot this entry belongs to"
			},
			{
				"name": "data_sequence_number",
				"type": "long",
				"doc": "Sequence number for the data file"
			},
			{
				"name": "file_sequence_number",
				"type": "long",
				"doc": "Sequence number for the manifest file"
			},
			{
				"name": "data_file",
				"type": {
					"type": "record",
					"name": "data_file",
					"fields": [
						{
							"name": "content",
							"type": "int",
							"doc": "Content type: 0 = data, 1 = position deletes, 2 = equality deletes"
						},
						{
							"name": "file_path",
							"type": "string",
							"doc": "Path to the data file"
						},
						{
							"name": "file_format",
							"type": "string",
							"doc": "Format of the file (e.g., PARQUET)"
						},
						{
							"name": "partition",
							"type": {
								"type": "map",
								"values": "string"
							},
							"doc": "Partition values as key-value pairs"
						},
						{
							"name": "record_count",
							"type": "long",
							"doc": "Number of records in the file"
						},
						{
							"name": "file_size_in_bytes",
							"type": "long",
							"doc": "Size of the file in bytes"
						},
						{
							"name": "column_sizes",
							"type": {
								"type": "map",
								"values": "long"
							},
							"doc": "Size of each column in bytes"
						},
						{
							"name": "value_counts",
							"type": {
								"type": "map",
								"values": "long"
							},
							"doc": "Number of values in each column"
						},
						{
							"name": "null_value_counts",
							"type": {
								"type": "map",
								"values": "long"
							},
							"doc": "Number of null values in each column"
						},
						{
							"name": "lower_bounds",
							"type": {
								"type": "map",
								"values": "bytes"
							},
							"doc": "Lower bounds for each column"
						},
						{
							"name": "upper_bounds",
							"type": {
								"type": "map",
								"values": "bytes"
							},
							"doc": "Upper bounds for each column"
						},
						{
							"name": "key_metadata",
							"type": "bytes",
							"doc": "Additional metadata for the file"
						},
						{
							"name": "split_offsets",
							"type": {
								"type": "array",
								"items": "long"
							},
							"doc": "Split offsets for the file"
						},
						{
							"name": "sort_order_id",
							"type": "int",
							"doc": "ID of the sort order used"
						}
					]
				}
			}
		]
	}`

	// ManifestFileSchema defines the Avro schema for manifest files
	ManifestFileSchema = `{
		"type": "record",
		"name": "manifest_file",
		"namespace": "org.apache.iceberg",
		"fields": [
			{
				"name": "manifest_type",
				"type": "string",
				"doc": "Type of manifest: ENTRY or DELETE"
			},
			{
				"name": "content",
				"type": "int",
				"doc": "Content type: 0 = data, 1 = deletes"
			},
			{
				"name": "sequence_number",
				"type": "long",
				"doc": "Sequence number for the manifest"
			},
			{
				"name": "min_sequence_number",
				"type": "long",
				"doc": "Minimum sequence number in the manifest"
			},
			{
				"name": "entries",
				"type": {
					"type": "array",
					"items": "org.apache.iceberg.manifest_entry"
				},
				"doc": "Array of manifest entries"
			}
		]
	}`

	// SnapshotSchema defines the Avro schema for table snapshots
	SnapshotSchema = `{
		"type": "record",
		"name": "snapshot",
		"namespace": "org.apache.iceberg",
		"fields": [
			{
				"name": "snapshot_id",
				"type": "long",
				"doc": "Unique identifier for the snapshot"
			},
			{
				"name": "timestamp_ms",
				"type": "long",
				"doc": "Timestamp when the snapshot was created"
			},
			{
				"name": "summary",
				"type": {
					"type": "map",
					"values": "string"
				},
				"doc": "Summary of changes in this snapshot"
			},
			{
				"name": "manifest_list",
				"type": "string",
				"doc": "Path to the manifest list file"
			},
			{
				"name": "schema_id",
				"type": "int",
				"doc": "ID of the table schema"
			},
			{
				"name": "parent_snapshot_id",
				"type": ["null", "long"],
				"doc": "ID of the parent snapshot, if any"
			},
			{
				"name": "manifests",
				"type": {
					"type": "array",
					"items": {
						"type": "record",
						"name": "manifest_file_info",
						"fields": [
							{
								"name": "manifest_path",
								"type": "string",
								"doc": "Path to the manifest file"
							},
							{
								"name": "manifest_length",
								"type": "long",
								"doc": "Length of the manifest file in bytes"
							},
							{
								"name": "partition_spec_id",
								"type": "int",
								"doc": "ID of the partition specification"
							},
							{
								"name": "added_files_count",
								"type": "int",
								"doc": "Number of added files"
							},
							{
								"name": "deleted_files_count",
								"type": "int",
								"doc": "Number of deleted files"
							},
							{
								"name": "added_rows_count",
								"type": "long",
								"doc": "Number of added rows"
							},
							{
								"name": "deleted_rows_count",
								"type": "long",
								"doc": "Number of deleted rows"
							}
						]
					}
				},
				"doc": "List of manifest files in this snapshot"
			}
		]
	}`
)

// SchemaRegistry holds all Avro schemas used by the Iceberg manager
type SchemaRegistry struct {
	ManifestEntry string
	ManifestFile  string
	Snapshot      string
}

// NewSchemaRegistry creates a new schema registry with all Iceberg schemas
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		ManifestEntry: ManifestEntrySchema,
		ManifestFile:  ManifestFileSchema,
		Snapshot:      SnapshotSchema,
	}
}

// GetSchema returns a schema by name
func (sr *SchemaRegistry) GetSchema(name string) string {
	switch name {
	case "manifest_entry":
		return sr.ManifestEntry
	case "manifest_file":
		return sr.ManifestFile
	case "snapshot":
		return sr.Snapshot
	default:
		return ""
	}
}
