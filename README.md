```sql

-- =====================================================================
-- Stored Procedure: purge_old_rows_non_partitioned
-- Description: For a NON‑partitioned base table, delete rows older than a
--              dynamic retention window based on the latest month present
--              in the data; delete dependent rows in the reference table
--              first; then optionally clean any remaining orphans.
-- Notes:
--   * Assumes an integer timestamp column `event_time_int` in both tables.
--   * Set `is_millis` TRUE when `event_time_int` is in milliseconds; FALSE if seconds.
--   * Keep this function OWNER as an admin and control EXECUTE via GRANTs.
-- =====================================================================
CREATE OR REPLACE PROCEDURE purge_old_rows_non_partitioned(
    schema_name   TEXT,
    base_table    TEXT,
    second_table  TEXT,
    fk_column     TEXT,           -- FK column in second_table referencing base_table.id
    months_to_keep INT DEFAULT 2, -- retention window (months)
    is_millis     BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    latest_available_date DATE;
    cutoff_date           DATE;
    cutoff_epoch          BIGINT;
    factor                INT := CASE WHEN is_millis THEN 1000 ELSE 1 END;
    del_ref_count         INT := 0;
    del_base_count        INT := 0;
    del_orphan_count      INT := 0;
BEGIN
    -- 1) Determine latest available month from the data in base_table
    EXECUTE format(
        'SELECT TO_DATE(TO_CHAR(MAX(TO_TIMESTAMP((event_time_int::BIGINT)/%s)), ''YYYY-MM-01''), ''YYYY-MM-DD'') FROM %I.%I',
        factor, schema_name, base_table
    ) INTO latest_available_date;

    IF latest_available_date IS NULL THEN
        RAISE NOTICE 'No data found in %.%, nothing to purge.', schema_name, base_table;
        RETURN;
    END IF;

    -- 2) Compute retention cutoff aligned to month start
    cutoff_date  := DATE_TRUNC(''month'', latest_available_date) - MAKE_INTERVAL(months := months_to_keep);
    cutoff_epoch := EXTRACT(EPOCH FROM cutoff_date);

    RAISE NOTICE 'Latest month in data: %; cutoff (>= purge) before: % (epoch %)', latest_available_date, cutoff_date, cutoff_epoch;

    -- 3) Delete dependent rows in second_table first (those referencing base rows to be purged)
    EXECUTE format(
        'DELETE FROM %I.%I st
         WHERE st.%I IN (
           SELECT id FROM %I.%I
           WHERE (event_time_int::BIGINT)/%s < %s
         )',
        schema_name, second_table,
        fk_column,
        schema_name, base_table,
        factor, cutoff_epoch
    );
    GET DIAGNOSTICS del_ref_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % dependent rows from %.% referencing purged base rows.', del_ref_count, schema_name, second_table;

    -- 4) Delete the old rows from base_table
    EXECUTE format(
        'DELETE FROM %I.%I
         WHERE (event_time_int::BIGINT)/%s < %s',
        schema_name, base_table,
        factor, cutoff_epoch
    );
    GET DIAGNOSTICS del_base_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % base rows from %.% older than %.', del_base_count, schema_name, base_table, cutoff_date;

    -- 5) Safety pass: remove any remaining orphans in second_table older than cutoff
    EXECUTE format(
        'DELETE FROM %I.%I st
         WHERE (st.event_time_int::BIGINT)/%s < %s
           AND NOT EXISTS (
             SELECT 1 FROM %I.%I ft WHERE ft.id = st.%I
           )',
        schema_name, second_table,
        factor, cutoff_epoch,
        schema_name, base_table, fk_column
    );
    GET DIAGNOSTICS del_orphan_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % additional orphan rows from %.% older than %.', del_orphan_count, schema_name, second_table, cutoff_date;

    RAISE NOTICE 'Non-partitioned purge completed.';
END;
$$;

-- Example manual call:
-- CALL purge_old_rows_non_partitioned('public', 'first_table', 'second_table', 'first_table_id', 2, TRUE);

-- Example pg_cron schedule (first Sunday of each month at 02:00):
-- SELECT cron.schedule(
--   'monthly_purge_non_partitioned',
--   '0 2 1-7 * 0',
--   $$ CALL purge_old_rows_non_partitioned('public','first_table','second_table','first_table_id', 2, TRUE); $$
-- );


```
-- Stored Procedure: drop_old_partitions
-- Description: Drops old partitions from a partitioned table and deletes orphan records from a referencing table based on dynamic retention logic.

CREATE OR REPLACE PROCEDURE drop_old_partitions(
    schema_name TEXT,
    base_table TEXT,
    second_table TEXT,
    fk_column TEXT,
    months_to_keep INT DEFAULT 2
)
LANGUAGE plpgsql
AS $$
DECLARE
    partition_name TEXT;
    partition_date TEXT;
    expected_year INT;
    expected_month INT;
    formatted_date DATE;
    mismatch_count INT;
    latest_available_date DATE;
BEGIN
    -- Step 1: Get the latest available month from base table
    EXECUTE format(
        'SELECT TO_DATE(TO_CHAR(MAX(TO_TIMESTAMP(event_time_int::BIGINT / 1000)), ''YYYY-MM-01''), ''YYYY-MM-DD'') 
         FROM %I.%I',
        schema_name, base_table
    ) INTO latest_available_date;

    IF latest_available_date IS NULL THEN
        RAISE NOTICE 'No data found in %.%, skipping partition cleanup.', schema_name, base_table;
        RETURN;
    END IF;

    RAISE NOTICE 'Latest available date in data: %', latest_available_date;

    -- Step 2: Loop through partitions
    FOR partition_name IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = schema_name
          AND tablename LIKE base_table || '_y%'
    LOOP
        -- Extract year and month from partition name
        partition_date := SPLIT_PART(partition_name, '_y', 2);
        expected_year := CAST(SPLIT_PART(partition_date, 'm', 1) AS INT);
        expected_month := CAST(SPLIT_PART(partition_date, 'm', 2) AS INT);

        -- Convert to date for comparison
        formatted_date := TO_DATE(
            REGEXP_REPLACE(partition_date, '([0-9]{4})m([0-9]{1,2})$', '\1-\2'),
            'YYYY-MM'
        );

        -- Step 3: Validate data within partition matches partition naming
        EXECUTE format(
            'SELECT COUNT(*) FROM %I.%I WHERE EXTRACT(YEAR FROM TO_TIMESTAMP(event_time_int::BIGINT / 1000)) != %s 
             OR EXTRACT(MONTH FROM TO_TIMESTAMP(event_time_int::BIGINT / 1000)) != %s',
            schema_name, partition_name, expected_year, expected_month
        ) INTO mismatch_count;

        IF mismatch_count > 0 THEN
            RAISE NOTICE 'Skipping partition %: % mismatched records found.', partition_name, mismatch_count;
            CONTINUE;
        END IF;

        -- Step 4: Drop if partition is older than retention threshold
        IF formatted_date < latest_available_date - MAKE_INTERVAL(months := months_to_keep) THEN
            RAISE NOTICE 'Dropping partition: %', partition_name;

            -- Step 5: Call orphan delete procedure for that month
            EXECUTE format(
                'CALL delete_orphan_records_by_month(''%I'', ''%I'', ''%I'', ''%I'', DATE ''%s'')',
                schema_name, second_table, base_table, fk_column, formatted_date
            );

            -- Delete dependent records
            EXECUTE format(
                'DELETE FROM %I.%I WHERE %I IN (SELECT id FROM %I.%I)',
                schema_name, second_table, fk_column, schema_name, partition_name
            );

            -- Drop the partition
            EXECUTE format('DROP TABLE %I.%I;', schema_name, partition_name);
        END IF;
    END LOOP;

    RAISE NOTICE 'Partition and orphan cleanup completed.';
END;
$$;

-- =====================================================================
-- Stored Procedure: purge_old_rows_non_partitioned
-- Description: For a NON‑partitioned base table, delete rows older than a
--              dynamic retention window based on the latest month present
--              in the data; delete dependent rows in the reference table
--              first; then optionally clean any remaining orphans.
-- Notes:
--   * Assumes an integer timestamp column `event_time_int` in both tables.
--   * Set `is_millis` TRUE when `event_time_int` is in milliseconds; FALSE if seconds.
--   * Keep this function OWNER as an admin and control EXECUTE via GRANTs.
-- =====================================================================
CREATE OR REPLACE PROCEDURE purge_old_rows_non_partitioned(
    schema_name    TEXT,
    base_table     TEXT,
    second_table   TEXT,
    id_column      TEXT,      -- common ID column name in BOTH tables
    ts_column      TEXT,      -- common timestamp INT column name in BOTH tables (epoch secs or millis)
    months_to_keep INT DEFAULT 2,
    is_millis      BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    latest_available_date DATE;
    cutoff_date           DATE;
    cutoff_epoch          BIGINT;   -- cutoff expressed in epoch SECONDS
    factor                INT := CASE WHEN is_millis THEN 1000 ELSE 1 END;
    del_ref_count         INT := 0;
    del_base_count        INT := 0;
    del_orphan_count      INT := 0;
BEGIN
    -- 1) Determine the latest month present in base_table using ts_column
    EXECUTE format(
        'SELECT TO_DATE(TO_CHAR(MAX(TO_TIMESTAMP((%I::BIGINT)/%s)), ''YYYY-MM-01''), ''YYYY-MM-DD'')
         FROM %I.%I',
        ts_column, factor, schema_name, base_table
    ) INTO latest_available_date;

    IF latest_available_date IS NULL THEN
        RAISE NOTICE 'No data found in %.%, nothing to purge.', schema_name, base_table;
        RETURN;
    END IF;

    -- 2) Compute retention cutoff aligned to month start, then convert to epoch seconds
    cutoff_date  := DATE_TRUNC(''month'', latest_available_date) - MAKE_INTERVAL(months := months_to_keep);
    cutoff_epoch := EXTRACT(EPOCH FROM cutoff_date);

    RAISE NOTICE 'Latest month in data: %; retention cutoff (purge anything older than): % (epoch %)',
                 latest_available_date, cutoff_date, cutoff_epoch;

    -- 3) Delete dependent rows in second_table FIRST, matching on (id_column, ts_column)
    --    Only those whose base row is older than cutoff
    EXECUTE format(
        'DELETE FROM %I.%I st
         USING %I.%I ft
         WHERE (ft.%I::BIGINT)/%s < %s
           AND st.%I = ft.%I
           AND st.%I = ft.%I',
        schema_name, second_table,
        schema_name, base_table,
        ts_column, factor, cutoff_epoch,
        id_column, id_column,
        ts_column, ts_column
    );
    GET DIAGNOSTICS del_ref_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % dependent rows from %.% referencing old base rows.', del_ref_count, schema_name, second_table;

    -- 4) Delete the old rows from base_table (older than cutoff)
    EXECUTE format(
        'DELETE FROM %I.%I
         WHERE (%I::BIGINT)/%s < %s',
        schema_name, base_table,
        ts_column, factor, cutoff_epoch
    );
    GET DIAGNOSTICS del_base_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % base rows from %.% older than %.', del_base_count, schema_name, base_table, cutoff_date;

    -- 5) Safety pass: remove any remaining ORPHANS i

-- Example manual call:
-- CALL purge_old_rows_non_partitioned('public', 'first_table', 'second_table', 'first_table_id', 2, TRUE);

-- Example pg_cron schedule (first Sunday of each month at 02:00):
-- SELECT cron.schedule(
--   'monthly_purge_non_partitioned',
--   '0 2 1-7 * 0',
--   $$ CALL purge_old_rows_non_partitioned('public','first_table','second_table','first_table_id', 2, TRUE); $$
-- );


```sql

```
```sql

```
```sql

```
```sql

```
