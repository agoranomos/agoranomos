-- The table of the migrations of the core (ADR-0038, ADR-0044): one row for each file of
-- postgres/migrations that db-init has applied, written in the transaction of the file
-- (postgres/migrate.sql). The schema core comes from warehouse.sql, which runs before.
CREATE TABLE core.schema_migrations (
  version integer PRIMARY KEY CHECK (version BETWEEN 1 AND 9999),
  name text NOT NULL CHECK (name ~ '^[a-z0-9_]+$'),
  checksum text NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
  applied_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE core.schema_migrations IS
'The migrations of the core that db-init has applied: one row for each file NNNN_name.sql of postgres/migrations, written in the same transaction as the migration.';
COMMENT ON COLUMN core.schema_migrations.version IS
'The number of the migration, the NNNN of its file: db-init applies them in this order, each once.';
COMMENT ON COLUMN core.schema_migrations.name IS
'The name of the migration, the part of its file name between the number and .sql.';
COMMENT ON COLUMN core.schema_migrations.checksum IS
'The SHA-256 of the file as it was applied, in hexadecimal: db-init stops when the file has another one.';
COMMENT ON COLUMN core.schema_migrations.applied_at IS
'When the transaction that applied the migration started.';
