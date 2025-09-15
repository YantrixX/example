
---

## 🔹 Step 1: Prepare & Rename

1. Identify the target table and its usage (query patterns, keys, FKs, views).
2. **Rename the existing table** to keep a preserved copy:

   ```sql
   ALTER TABLE mytable RENAME TO mytable_local;
   ```

   * Data stays intact.
   * Application queries will fail temporarily until the new `mytable` is created.
   * Schedule this during a maintenance window or freeze writes.

---

## 🔹 Step 2: Create New Distributed Table

1. Recreate the schema for `mytable` with the same columns, types, and primary key.

   ```sql
   CREATE TABLE mytable (
       id BIGINT,
       tenant_id INT,
       event_time TIMESTAMPTZ,
       value NUMERIC,
       PRIMARY KEY (id)
   );
   ```
2. Convert it to a **distributed + partitioned table**. Example:

   ```sql
   SELECT create_distributed_table(
       'mytable',
       'tenant_id',
       colocate_with => 'none',
       shard_count => 32,
       partition_column => 'event_time',
       partition_method => 'range'
   );
   ```

---

## 🔹 Step 3: Load Data

1. Copy data from the renamed local table into the new distributed table:

   ```sql
   INSERT INTO mytable
   SELECT * FROM mytable_local;
   ```
2. Validate row counts:

   ```sql
   SELECT count(*) FROM mytable_local;
   SELECT count(*) FROM mytable;
   ```

---

## 🔹 Step 4: Reapply Indexes & Constraints

1. Recreate indexes:

   ```sql
   CREATE INDEX idx_event_time ON mytable (event_time);
   CREATE INDEX idx_tenant_event ON mytable (tenant_id, event_time);
   ```
2. Re‑establish any supported constraints (PK, unique).
   ⚠️ Foreign keys across distributed tables may need to be dropped or handled at application level.

---

## 🔹 Step 5: Reapply Permissions & Dependencies

1. Re‑apply grants:

   ```sql
   GRANT SELECT, INSERT, UPDATE, DELETE ON mytable TO app_user;
   ```
2. Update/recreate views or materialised views that depended on `mytable`.
3. Adjust application queries if they assumed the old structure.

---

## 🔹 Step 6: Validate & Switch Over

1. Run functional queries against the new `mytable` to confirm performance and correctness.
2. If needed, colocate related tables:

   ```sql
   SELECT create_distributed_table('orders', 'tenant_id', colocate_with => 'mytable');
   ```
3. Once validated, drop `mytable_local` (optional) or keep it as a backup.

   ```sql
   DROP TABLE mytable_local;
   ```

---

