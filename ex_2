

## ✅ Bulk Partition Extension Function

```sql
CREATE OR REPLACE FUNCTION aaa.extend_partitions_range(
    parent_table text,
    partition_column text,
    start_year int,
    start_month int,
    end_year int,
    end_month int
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    cur_date date;
    end_date date;
    cur_year int;
    cur_month int;
BEGIN
    -- Starting date (first of start_month)
    cur_date := make_date(start_year, start_month, 1);

    -- Ending date (first of end_month)
    end_date := make_date(end_year, end_month, 1);

    WHILE cur_date <= end_date LOOP
        cur_year  := extract(year from cur_date)::int;
        cur_month := extract(month from cur_date)::int;

        PERFORM aaa.extend_next_partition(parent_table, partition_column, cur_year, cur_month);

        -- Move to next month
        cur_date := (cur_date + interval '1 month')::date;
    END LOOP;
END;
$$;
```

---

## 🔹 Usage Examples

### 1. Create partitions for Oct–Dec 2025

```sql
SELECT aaa.extend_partitions_range('staging.yyy_table', 'aa_time', 2025, 10, 2025, 12);
```

→ Creates:

* `staging.yyy_table_y2025m10`
* `staging.yyy_table_y2025m11`
* `staging.yyy_table_y2025m12`

---

### 2. Create partitions for Jan 2026 – Jun 2026

```sql
SELECT aaa.extend_partitions_range('staging.yyy_table', 'aa_time', 2026, 1, 2026, 6);
```

