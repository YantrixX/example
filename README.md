SELECT event_time_int, 
       TO_TIMESTAMP(event_time_int) AS as_seconds,
       TO_TIMESTAMP(event_time_int::BIGINT / 1000) AS as_milliseconds
FROM second_table
ORDER BY event_time_int DESC
LIMIT 5;


-- After all partition drops completed
RAISE NOTICE 'All partition drops completed, now cleaning up orphan records...';

-- Step 6: Delete old orphaned records from reference table
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
