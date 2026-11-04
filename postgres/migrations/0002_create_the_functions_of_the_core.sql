-- The functions of the core (ADR-0035, ADR-0036, ADR-0039, ADR-0046). They come first, as the
-- migrations after this one create the partitions of their tables through core.ensure_partition.
--
-- core.ensure_partition and core.drop_partition are SECURITY DEFINER: they run as migrator, which
-- owns them and the tables, for transformer, which may call them and owns no table of migrator.
-- Their text is the one that the check of partitions of #189 ran on PostgreSQL 18.6, with 123
-- partitions and eight readers, unchanged: the brief of #194 gives the reasons for each line.

CREATE FUNCTION core.ensure_partition(p_parent regclass, p_month date)
    RETURNS boolean
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog, pg_temp
    SET TimeZone = 'UTC'
    SET lock_timeout = '60s'
AS $function$
DECLARE
    v_schema name;
    v_parent name;
    v_key    name;
    v_from   date := date_trunc('month', p_month)::date;
    v_to     date := (date_trunc('month', p_month) + interval '1 month')::date;
    v_name   name;
    v_check  name;
BEGIN
    -- Only a range-partitioned table of the core that this role owns.
    SELECT n.nspname, c.relname, a.attname
      INTO v_schema, v_parent, v_key
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_partitioned_table p ON p.partrelid = c.oid
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = p.partattrs[0]
     WHERE c.oid = p_parent
       AND n.nspname = 'core'
       AND c.relowner = current_user::regrole
       AND p.partstrat = 'r'
       AND p.partnatts = 1;
    IF NOT FOUND OR p_month IS NULL THEN
        RAISE EXCEPTION 'no month of a partitioned table of the core in (%, %)', p_parent, p_month
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- A date that no platform can give must not make its own partition: the month fails loudly instead.
    IF v_from < date '2000-01-01' OR v_from > date_trunc('month', now()) + interval '2 months' THEN
        RAISE EXCEPTION 'month % is out of the range of the core', v_from
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    v_name  := format('%s_%s', v_parent, to_char(v_from, 'YYYY_MM'));
    v_check := format('%s_bounds', v_name);

    -- An existing partition takes no lock: a pre-hook calls this on every run.
    IF EXISTS (SELECT FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                WHERE i.inhparent = p_parent AND c.relname = v_name AND NOT i.inhdetachpending) THEN
        RETURN false;
    END IF;

    -- The lock that ATTACH takes anyway, taken first: two calls wait here, and the second sees the first.
    EXECUTE format('LOCK TABLE ONLY %I.%I IN SHARE UPDATE EXCLUSIVE MODE', v_schema, v_parent);
    IF EXISTS (SELECT FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                WHERE i.inhparent = p_parent AND c.relname = v_name AND NOT i.inhdetachpending) THEN
        RETURN false;
    END IF;

    EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL)', v_schema, v_name, v_schema, v_parent);
    EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I CHECK (%I >= %L AND %I < %L)',
                   v_schema, v_name, v_check, v_key, v_from, v_key, v_to);
    EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)',
                   v_schema, v_parent, v_schema, v_name, v_from, v_to);
    EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I', v_schema, v_name, v_check);
    EXECUTE format('GRANT MAINTAIN ON TABLE %I.%I TO transformer', v_schema, v_name);
    RETURN true;
END
$function$;
REVOKE ALL ON FUNCTION core.ensure_partition(regclass, date) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION core.ensure_partition(regclass, date) TO transformer;

CREATE FUNCTION core.drop_partition(p_parent regclass, p_month date)
    RETURNS boolean
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog, pg_temp
    SET lock_timeout = '2s'
AS $function$
DECLARE
    v_schema name;
    v_parent name;
    v_name   name;
BEGIN
    -- Only a journal: the facts are kept for good (ADR-0044), and this role runs as their owner.
    SELECT n.nspname, c.relname
      INTO v_schema, v_parent
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = p_parent
       AND c.relkind = 'p'
       AND n.nspname = 'core'
       AND c.relowner = current_user::regrole
       AND c.relname LIKE '%\_restatements';
    IF NOT FOUND OR p_month IS NULL THEN
        RAISE EXCEPTION 'no month of a journal of the core in (%, %)', p_parent, p_month
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    v_name := format('%s_%s', v_parent, to_char(p_month, 'YYYY_MM'));
    IF NOT EXISTS (SELECT FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                    WHERE i.inhparent = p_parent AND c.relname = v_name) THEN
        RETURN false;
    END IF;

    -- ACCESS EXCLUSIVE on the journal, for at most lock_timeout: a retry of the DAG takes the month later.
    EXECUTE format('DROP TABLE %I.%I', v_schema, v_name);
    RETURN true;
END
$function$;
REVOKE ALL ON FUNCTION core.drop_partition(regclass, date) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION core.drop_partition(regclass, date) TO transformer;

COMMENT ON FUNCTION core.ensure_partition(regclass, date) IS
'Creates the partition of the month of p_month of a table of the core that is partitioned by month, named <table>_YYYY_MM, and returns true, or returns false when the partition exists. It runs as migrator, which owns the partition it makes, and grants MAINTAIN on it to transformer. It refuses a table that migrator does not own or that is not partitioned by range on one column, and a month before 2000 or more than two months after the current one, in UTC. It attaches a table made beforehand, so the parent takes SHARE UPDATE EXCLUSIVE and readers do not wait. Only transformer may execute it.';
COMMENT ON FUNCTION core.drop_partition(regclass, date) IS
'Drops the partition of the month of p_month of a journal of the core, a partitioned table whose name ends in _restatements, and returns true, or returns false when the journal has no such partition. It refuses any other table, so no partition of a fact can be dropped through it. It runs as migrator, the owner of the journal, and waits at most 2 seconds for its lock before it fails. Only transformer may execute it.';

-- The freeze of the facts (ADR-0035): a trigger on each fact calls this for a row of a month that
-- ended before the window of restatements (0005_create_the_facts.sql), and it refuses the change,
-- unless agoranomos.thaw holds the ID of the current transaction. A load of history and data forget
-- are to set it, for their own transaction only, with
--   SELECT set_config('agoranomos.thaw', pg_current_xact_id()::text, true);
-- Any role may set a setting with a prefix of its own, so the setting is no right: only the roles
-- that may delete or update rows of a fact, transformer and migrator, meet the freeze at all. A
-- value that a session set for good, or in an earlier transaction, names another transaction and
-- lifts nothing, so the freeze is off for one transaction at most. pg_current_xact_id_if_assigned()
-- assigns no ID: a transaction that has set the value has one, and one that has not is refused.
CREATE FUNCTION core.refuse_a_change_of_a_frozen_month()
    RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    IF current_setting('agoranomos.thaw', true) = pg_current_xact_id_if_assigned()::text THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'the row of % in %.% is in a frozen month, which ended before the window of restatements',
                    OLD.report_date, TG_TABLE_SCHEMA, TG_TABLE_NAME
        USING ERRCODE = 'object_not_in_prerequisite_state',
              HINT = 'agoranomos.thaw set to the ID of the current transaction lifts the freeze for that transaction.';
END
$function$;
REVOKE ALL ON FUNCTION core.refuse_a_change_of_a_frozen_month() FROM PUBLIC;

COMMENT ON FUNCTION core.refuse_a_change_of_a_frozen_month() IS
'The function of the trigger freeze_the_months_before_the_window of each fact of the core: it refuses the DELETE or UPDATE of a row of a frozen month, unless the setting agoranomos.thaw holds the ID of the current transaction, as pg_current_xact_id() gives it. INSERT stays allowed.';
