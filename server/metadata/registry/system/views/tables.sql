-- System view for tables metadata
CREATE VIEW IF NOT EXISTS system_tables AS
SELECT 
    d.name as database_name,
    t.name as table_name,
    t.display_name,
    t.description,
    t.table_type,
    t.is_temporary,
    t.is_external,
    t.row_count,
    t.file_count,
    t.total_size,
    t.created_at,
    t.updated_at
FROM tables t 
JOIN databases d ON t.database_id = d.id 
WHERE t.deleted_at IS NULL AND d.deleted_at IS NULL;

