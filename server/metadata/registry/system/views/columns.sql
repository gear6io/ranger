-- System view for columns metadata
CREATE VIEW IF NOT EXISTS system_columns AS
SELECT 
    d.name as database_name,
    t.name as table_name,
    c.column_name,
    c.display_name,
    c.data_type,
    c.is_nullable,
    c.is_primary,
    c.is_unique,
    c.default_value,
    c.description,
    c.ordinal_position,
    c.max_length,
    c.precision,
    c.scale,
    c.created_at,
    c.updated_at
FROM table_columns c
JOIN tables t ON c.table_id = t.id
JOIN databases d ON t.database_id = d.id
WHERE t.deleted_at IS NULL AND d.deleted_at IS NULL;

