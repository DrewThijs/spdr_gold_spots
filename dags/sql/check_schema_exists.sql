SELECT EXISTS (
    SELECT *
    FROM INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
    AND table_catalog = '{{ params.db_name }}'
    AND table_name = '{{ params.table_name }}'
)
