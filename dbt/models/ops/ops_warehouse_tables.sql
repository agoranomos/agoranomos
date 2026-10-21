-- The tables of the warehouse with their size, from the catalog of PostgreSQL: the estimate of the
-- rows of the last ANALYZE, and the bytes of the table with its indexes and TOAST. Views take no
-- room, and the catalog and the temporary schemas are left out.
select
    -- The catalog names them with the type name, which Metabase does not know: text.
    namespaces.nspname::text as schema_name,
    tables.relname::text as table_name,
    greatest(tables.reltuples, 0)::bigint as estimated_rows,
    pg_total_relation_size(tables.oid) as total_bytes
from pg_catalog.pg_class as tables
inner join pg_catalog.pg_namespace as namespaces on namespaces.oid = tables.relnamespace
where
    tables.relkind in ('r', 'p')
    and namespaces.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
    and namespaces.nspname not like 'pg\_temp\_%'
    and namespaces.nspname not like 'pg\_toast\_temp\_%'
