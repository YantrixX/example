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
DECLARE
    start_epoch BIGINT := EXTRACT(EPOCH FROM latest_available_date - MAKE_INTERVAL(months := months_to_keep));
    end_epoch   BIGINT := EXTRACT(EPOCH FROM latest_available_date);
BEGIN
    RAISE NOTICE 'Cleaning orphan records from %.% older than % months', schema_name, second_table, months_to_keep;

    EXECUTE format(
        'DELETE FROM %I.%I st
         WHERE (st.event_time_int::BIGINT / 1000) >= %s
           AND (st.event_time_int::BIGINT / 1000) < %s
           AND NOT EXISTS (
             SELECT 1 FROM %I.%I ft
             WHERE ft.id = st.%I
           )',
        schema_name, second_table,
        start_epoch, end_epoch,
        schema_name, first_table, fk_column
    );

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % orphan records from %.% for data before %', deleted_count, schema_name, second_table, latest_available_date;
END;

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
```sql

```
