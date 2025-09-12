
```sql
TO_TIMESTAMP((%I::BIGINT)/%s)
```

tries to cast a `timestamp` to `bigint`, which fails.

### Quick fixes

#### A) Getting the latest available month

If the column is a **timestamp**, don’t wrap it in `TO_TIMESTAMP` or cast to `BIGINT`:

```sql
-- timestamp column
EXECUTE format(
  'SELECT DATE_TRUNC(''month'', MAX(%I))::date FROM %I.%I',
  ts_column, schema_name, base_table
) INTO latest_available_date;
```

(Your previous line was correct only when the column is **epoch int**.)

#### B) Comparing against the cutoff in DELETEs

If the column is a **timestamp**, compare directly to a timestamp literal:

```sql
-- Prepare a timestamp cutoff
cutoff_date  := date_trunc('month', latest_available_date) - make_interval(months := months_to_keep);
-- use cutoff_date::timestamp in dynamic SQL

-- base table delete (timestamp column)
EXECUTE format(
  'DELETE FROM %I.%I WHERE %I < %L::timestamp',
  schema_name, base_table,
  ts_column, cutoff_date  -- %L injects a literal safely
);

-- dependent delete (timestamp column)
EXECUTE format(
  'DELETE FROM %I.%I st USING %I.%I ft
   WHERE ft.%I < %L::timestamp
     AND st.%I = ft.%I
     AND st.%I = ft.%I',
  schema_name, second_table, schema_name, base_table,
  ts_column, cutoff_date,
  id_column, id_column,
  ts_column, ts_column
);

-- orphan cleanup (timestamp column)
EXECUTE format(
  'DELETE FROM %I.%I st
   WHERE st.%I < %L::timestamp
     AND NOT EXISTS (
       SELECT 1 FROM %I.%I ft
       WHERE ft.%I = st.%I AND ft.%I = st.%I
     )',
  schema_name, second_table,
  ts_column, cutoff_date,
  schema_name, base_table,
  id_column, id_column,
  ts_column, ts_column
);
```

### Support both types cleanly (recommended)

Add a flag to your proc and branch once:

```plpgsql
-- params
--   ts_is_epoch BOOLEAN DEFAULT TRUE   -- new
--   is_millis  BOOLEAN DEFAULT TRUE    -- if ts_is_epoch = TRUE

IF ts_is_epoch THEN
  -- epoch path (what you had): use (ts_column::bigint)/factor and cutoff_epoch
ELSE
  -- timestamp path (above snippets): use %I < %L::timestamp and DATE_TRUNC for latest month
END IF;
```
