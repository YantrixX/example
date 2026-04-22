# COPY-Based Data Rectification: TEST → DEV

> **Geometry Handling**: Uses `ST_AsHexEWKB()` on export and `ST_GeomFromEWKB()` on import
> to avoid CSV corruption of geometry fields. Geometry is transferred as a hex string.

---

## Step 1 — Identify the Delta (Run on BOTH servers)

```sql
-- === RUN ON TEST ===
SELECT partition_month, COUNT(*) AS row_count
FROM point_table
GROUP BY partition_month
ORDER BY partition_month;

-- === RUN ON DEV ===
SELECT partition_month, COUNT(*) AS row_count
FROM point_table
GROUP BY partition_month
ORDER BY partition_month;

-- Compare the two result sets to identify which months diverge.
-- Note the mismatched partition_month values for use in subsequent steps.

-- Also check reference_table counts
-- === RUN ON BOTH ===
SELECT COUNT(*) AS total_refs FROM reference_table;
```

---

## Step 2 — Export affected point_table partitions from TEST (geometry-safe)

```sql
-- === RUN ON TEST ===
-- Replace '2026-03' with each mismatched partition_month.
-- Adjust column list to match your actual schema.
-- The key trick: cast geometry to hex EWKB string for safe CSV transport.

COPY (
    SELECT
        point_id,
        partition_month,
        start_date,
        end_date,
        -- ... list all your non-geometry columns here ...
        ST_AsHexEWKB(geom) AS geom_hex   -- geometry → hex string
    FROM point_table
    WHERE partition_month = '2026-03'
       OR (partition_month = '2026-04' AND start_date < '2026-04-01')  -- spill rows
) TO '/tmp/point_export_2026_03.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');
```

---

## Step 3 — Export affected reference_table rows from TEST

```sql
-- === RUN ON TEST ===
-- Export only references linked to the affected points.
-- Adjust join condition to your actual FK relationship.

COPY (
    SELECT r.*
    FROM reference_table r
    INNER JOIN point_table p ON r.point_id = p.point_id
    WHERE p.partition_month = '2026-03'
       OR (p.partition_month = '2026-04' AND p.start_date < '2026-04-01')
) TO '/tmp/ref_export_2026_03.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');
```

---

## Step 4 — Transfer files from TEST server to DEV server

```bash
# Option A: Direct SCP (if network allows)
scp test-server:/tmp/point_export_2026_03.csv /tmp/
scp test-server:/tmp/ref_export_2026_03.csv /tmp/
scp /tmp/point_export_2026_03.csv dev-server:/tmp/
scp /tmp/ref_export_2026_03.csv dev-server:/tmp/

# Option B: Via Azure Blob (if servers are isolated)
# -- On TEST server --
az storage blob upload \
    --account-name <storage_account> \
    --container-name data-transfer \
    --file /tmp/point_export_2026_03.csv \
    --name point_export_2026_03.csv \
    --auth-mode login

az storage blob upload \
    --account-name <storage_account> \
    --container-name data-transfer \
    --file /tmp/ref_export_2026_03.csv \
    --name ref_export_2026_03.csv \
    --auth-mode login

# -- On DEV server --
az storage blob download \
    --account-name <storage_account> \
    --container-name data-transfer \
    --name point_export_2026_03.csv \
    --file /tmp/point_export_2026_03.csv \
    --auth-mode login

az storage blob download \
    --account-name <storage_account> \
    --container-name data-transfer \
    --name ref_export_2026_03.csv \
    --file /tmp/ref_export_2026_03.csv \
    --auth-mode login
```

---

## Step 5 — Delete affected data in DEV (within a transaction)

```sql
-- === RUN ON DEV ===
BEGIN;

-- Delete references first (FK dependency)
DELETE FROM reference_table r
USING point_table p
WHERE r.point_id = p.point_id
  AND (
      p.partition_month = '2026-03'
      OR (p.partition_month = '2026-04' AND p.start_date < '2026-04-01')
  );

-- Then delete the points
DELETE FROM point_table
WHERE partition_month = '2026-03'
   OR (partition_month = '2026-04' AND start_date < '2026-04-01');

-- Verify counts are zero for affected partition
SELECT COUNT(*) FROM point_table WHERE partition_month = '2026-03';

COMMIT;
```

---

## Step 6 — Import into DEV (geometry-safe: hex → geometry)

```sql
-- === RUN ON DEV ===

-- 6a. Create a temporary staging table for points (text column for geometry)
CREATE TEMP TABLE point_staging (
    point_id        BIGINT,
    partition_month TEXT,
    start_date      TIMESTAMP,
    end_date        TIMESTAMP,
    -- ... all your non-geometry columns with matching types ...
    geom_hex        TEXT          -- hex EWKB string, NOT geometry
);

-- 6b. COPY the CSV into the staging table
COPY point_staging
FROM '/tmp/point_export_2026_03.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

-- 6c. Insert from staging → point_table, converting hex back to geometry
INSERT INTO point_table (
    point_id,
    partition_month,
    start_date,
    end_date,
    -- ... all your non-geometry columns ...
    geom
)
SELECT
    point_id,
    partition_month,
    start_date,
    end_date,
    -- ... all your non-geometry columns ...
    ST_GeomFromEWKB(decode(geom_hex, 'hex')) AS geom   -- hex string → geometry
FROM point_staging;

-- 6d. Drop the staging table
DROP TABLE point_staging;

-- 6e. Import reference_table (no geometry — straight COPY)
COPY reference_table (
    point_id,
    reference_id
    -- ... list all reference_table columns ...
)
FROM '/tmp/ref_export_2026_03.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');
```

---

## Step 7 — Verify the fix

```sql
-- === RUN ON DEV ===

-- 7a. Row counts should now match TEST
SELECT partition_month, COUNT(*) AS row_count
FROM point_table
GROUP BY partition_month
ORDER BY partition_month;

-- 7b. Every point must have at least one reference
SELECT COUNT(*) AS orphan_points
FROM point_table p
LEFT JOIN reference_table r ON p.point_id = r.point_id
WHERE r.point_id IS NULL;
-- Expected: 0

-- 7c. Geometry integrity check — no NULLs, valid geometries
SELECT COUNT(*) AS null_geom
FROM point_table
WHERE geom IS NULL
  AND partition_month = '2026-03';
-- Expected: 0

SELECT COUNT(*) AS invalid_geom
FROM point_table
WHERE NOT ST_IsValid(geom)
  AND partition_month = '2026-03';
-- Expected: 0

-- 7d. Spot-check a few rows
SELECT point_id, partition_month, ST_AsText(geom), start_date, end_date
FROM point_table
WHERE partition_month = '2026-03'
LIMIT 5;
```

---

## Notes

- **Repeat Steps 2–7** for each mismatched `partition_month`.
- If `reference_table` has its own geometry column, apply the same `ST_AsHexEWKB` / `ST_GeomFromEWKB` pattern.
- Run during a maintenance window with ETL jobs disabled on DEV.
- Take a DEV snapshot/backup before Step 5.
