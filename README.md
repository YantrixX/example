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
    deleted_count INTEGER;
    orphan_start_epoch BIGINT;
    orphan_end_epoch BIGINT;
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

    -- Step 2: Calculate retention window for orphan deletion
    orphan_start_epoch := EXTRACT(EPOCH FROM DATE_TRUNC('month', latest_available_date - MAKE_INTERVAL(months := months_to_keep + 1)));
    orphan_end_epoch := EXTRACT(EPOCH FROM DATE_TRUNC('month', latest_available_date - MAKE_INTERVAL(months := months_to_keep)));

    -- Step 3: Loop through partitions
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

        -- Step 4: Validate data within partition matches partition naming
        EXECUTE format(
            'SELECT COUNT(*) FROM %I.%I WHERE EXTRACT(YEAR FROM TO_TIMESTAMP(event_time_int::BIGINT / 1000)) != %s 
             OR EXTRACT(MONTH FROM TO_TIMESTAMP(event_time_int::BIGINT / 1000)) != %s',
            schema_name, partition_name, expected_year, expected_month
        ) INTO mismatch_count;

        IF mismatch_count > 0 THEN
            RAISE NOTICE 'Skipping partition %: % mismatched records found.', partition_name, mismatch_count;
            CONTINUE;
        END IF;

        -- Step 5: Drop if partition is older than retention threshold
        IF formatted_date < latest_available_date - MAKE_INTERVAL(months := months_to_keep) THEN
            RAISE NOTICE 'Dropping partition: %', partition_name;

            -- Delete dependent records
            EXECUTE format(
                'DELETE FROM %I.%I WHERE %I IN (SELECT id FROM %I.%I)',
                schema_name, second_table, fk_column, schema_name, partition_name
            );

            -- Drop the partition
            EXECUTE format('DROP TABLE %I.%I;', schema_name, partition_name);
        END IF;
    END LOOP;

    -- Step 6: Clean orphan records from second_table within the dropped period
    RAISE NOTICE 'Deleting orphan records in %.% between epochs % and %', schema_name, second_table, orphan_start_epoch, orphan_end_epoch;

    EXECUTE format(
        'DELETE FROM %I.%I st
         WHERE (st.event_time_int::BIGINT / 1000) >= %s
           AND (st.event_time_int::BIGINT / 1000) < %s
           AND NOT EXISTS (
             SELECT 1 FROM %I.%I ft
             WHERE ft.id = st.%I
           )',
        schema_name, second_table,
        orphan_start_epoch, orphan_end_epoch,
        schema_name, base_table, fk_column
    );

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % orphan records from %.%', deleted_count, schema_name, second_table;

    RAISE NOTICE 'Partition and orphan cleanup completed.';
END;
$$;

```
```sql

```
```sql

```
```sql

```
```sql

```
```sql

```
