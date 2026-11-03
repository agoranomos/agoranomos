-- One migration of the core (ADR-0038, ADR-0044). db-init runs this file as the superuser of the
-- warehouse once for each file of postgres/migrations that core.schema_migrations does not list, in
-- the order of their numbers, and gives it the path, the number, the name and the SHA-256 of the
-- file in the environment. The migration and its row in core.schema_migrations commit together or
-- not at all: psql stops at the first error, and the transaction ends with the connection.
\getenv migration_file MIGRATION_FILE
\getenv migration_version MIGRATION_VERSION
\getenv migration_name MIGRATION_NAME
\getenv migration_checksum MIGRATION_CHECKSUM

-- No notice for an object that a migration finds: the logs of db-init say what it did.
SET client_min_messages = warning;

BEGIN;
-- A migration waits at most a minute for a lock, so a start of the stack on which a query holds a
-- table of the core fails and can be repeated, instead of holding the queries that come after it.
SET LOCAL lock_timeout = '1min';
-- As migrator, the owner of the schema core, so that what the migration creates is its own and no
-- role that writes into the core can drop it (ADR-0035). SET LOCAL ends with the transaction.
SET LOCAL ROLE migrator;
\i :migration_file
INSERT INTO core.schema_migrations (version, name, checksum)
VALUES (:'migration_version', :'migration_name', :'migration_checksum');
COMMIT;
