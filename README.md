SELECT event_time_int, 
       TO_TIMESTAMP(event_time_int) AS as_seconds,
       TO_TIMESTAMP(event_time_int::BIGINT / 1000) AS as_milliseconds
FROM second_table
ORDER BY event_time_int DESC
LIMIT 5;



-- After all partition drops completed
RAISE NOTICE 'All partition drops completed, now cleaning up orphan records...';

-- Step 6: Delete old orphaned records from reference table
```sql
EXECUTE format(
    'DELETE FROM %I.%I st
     WHERE TO_TIMESTAMP(st.event_time_int::BIGINT / 1000) < $1
       AND NOT EXISTS (
         SELECT 1 FROM %I.%I ft
         WHERE ft.id = st.%I
       )',
    schema_name, second_table,
    schema_name, base_table, fk_column
)
USING latest_available_date - MAKE_INTERVAL(months := months_to_keep);

RAISE NOTICE 'Orphan cleanup completed in %I.%I.', schema_name, second_table;
```
Procedure Name: delete_orphan_records_by_month
Inputs:

schema_name TEXT

second_table TEXT

first_table TEXT

fk_column TEXT — column in second_table pointing to first_table.id

month_start DATE — month to delete orphan records from (e.g. '2024-07-01')

```sql
CREATE OR REPLACE PROCEDURE delete_orphan_records_by_month(
    schema_name TEXT,
    second_table TEXT,
    first_table TEXT,
    fk_column TEXT,
    month_start DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    month_end DATE := (month_start + INTERVAL '1 month')::DATE;
    deleted_count INTEGER;
BEGIN
    RAISE NOTICE 'Deleting orphan records from %.% between % and %',
        schema_name, second_table, month_start, month_end;

    EXECUTE format(
        'DELETE FROM %I.%I st
         WHERE TO_TIMESTAMP(st.event_time_int::BIGINT / 1000) >= $1
           AND TO_TIMESTAMP(st.event_time_int::BIGINT / 1000) < $2
           AND NOT EXISTS (
             SELECT 1 FROM %I.%I ft
             WHERE ft.id = st.%I
           )',
        schema_name, second_table,
        schema_name, first_table, fk_column
    )
    USING month_start, month_end;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % orphan records from %.% for month %', deleted_count, schema_name, second_table, month_start;
END;
$$;

```
CREATE OR REPLACE PROCEDURE delete_orphan_records_by_month(
    schema_name TEXT,
    second_table TEXT,
    first_table TEXT,
    fk_column TEXT,
    month_start DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    month_end DATE := (month_start + INTERVAL '1 month')::DATE;
    deleted_count INTEGER;
    rec RECORD;
BEGIN
    RAISE NOTICE 'Checking orphan records in %.% for %', schema_name, second_table, month_start;

    -- Step 1: Print the records about to be deleted
    FOR rec IN EXECUTE format(
        'SELECT st.* FROM %I.%I st
         WHERE TO_TIMESTAMP(st.event_time_int::BIGINT / 1000) >= $1
           AND TO_TIMESTAMP(st.event_time_int::BIGINT / 1000) < $2
           AND NOT EXISTS (
             SELECT 1 FROM %I.%I ft
             WHERE ft.id = st.%I
           )',
        schema_name, second_table,
        schema_name, first_table, fk_column
    )
    USING month_start, month_end
    LOOP
        -- Print or log fields (adjust as needed)
        RAISE NOTICE 'Will delete orphan row: id=%, %=%',
            rec.id, fk_column, rec.(fk_column);  -- if fk_column is known and fixed
    END LOOP;

    -- Step 2: Delete the records
    EXECUTE format(
        'DELETE FROM %I.%I st
         WHERE TO_TIMESTAMP(st.event_time_int::BIGINT / 1000) >= $1
           AND TO_TIMESTAMP(st.event_time_int::BIGINT / 1000) < $2
           AND NOT EXISTS (
             SELECT 1 FROM %I.%I ft
             WHERE ft.id = st.%I
           )',
        schema_name, second_table,
        schema_name, first_table, fk_column
    )
    USING month_start, month_end;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % orphan records from %.% for month %', deleted_count, schema_name, second_table, month_start;
END;
$$;

```sql
DECLARE
    start_epoch BIGINT := EXTRACT(EPOCH FROM month_start);
    end_epoch   BIGINT := EXTRACT(EPOCH FROM month_end);
BEGIN
    EXECUTE format(
        'DELETE FROM %I.%I st
         WHERE (st.event_time_int::BIGINT / 1000) >= %s
           AND (st.event_time_int::BIGINT / 1000) < %s
           AND NOT EXISTS (
             SELECT 1 FROM %I.%I ft
             WHERE ft.id = st.%I
           )',
        schema_name, second_table,
        start_epoch,
        end_epoch,
        schema_name, first_table, fk_column
    );

```

```sql

```
