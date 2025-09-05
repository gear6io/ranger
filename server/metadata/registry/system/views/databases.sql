-- System view for databases metadata
CREATE VIEW IF NOT EXISTS system.databases AS 
SELECT 
    name as database_name,
    display_name,
    description,
    is_system,
    is_read_only,
    table_count,
    total_size,
    created_at,
    updated_at
FROM databases 
WHERE deleted_at IS NULL;

