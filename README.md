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
```sql

```
```sql

```
```sql

```
```sql

```
