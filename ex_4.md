## 🚀 Cluster Reconfiguration Plan: Coordinator → 2-Node Cluster

---

### ✅ **Step 0: Preconditions**

* You're currently using **only the coordinator node** (Citus in local mode).
* Some tables are **local** (non-distributed).
* You want to **add 1+ worker nodes**, **distribute some tables**, and optionally **partition** them.

---

### 🔹 **Step 1: Add a Worker Node**

* Use Azure portal or CLI to **scale out** your Cosmos DB for PostgreSQL cluster.
* Add at least one **worker node** to convert the cluster to distributed mode.

> ⚠️ **Important**:
> Once a worker is added, the coordinator can no longer act as a worker — all distributed tables will now live only on the workers.

---

### 🔹 **Step 2: Check Cluster State**

Run on coordinator:

```sql
-- Check worker nodes
SELECT * FROM citus_get_active_worker_nodes();

-- Ensure metadata syncing
SELECT * FROM pg_dist_node;
```

✅ You should see at least 2 nodes: coordinator and 1+ workers.

---

### 🔹 **Step 3: Decide Table Roles**

For each table:

* **Keep as local** → remains on coordinator (no sharding).
* **Convert to reference table** → replicated to all nodes.
* **Convert to distributed table** → sharded across workers (optional: with partitioning inside shards).

> 🔍 Evaluate:
>
> * Joins and usage patterns.
> * Best distribution column (e.g. `tenant_id`).
> * Partitioning need (e.g. `event_time`).

---

### 🔹 **Step 4: Migrate Existing Tables**

Repeat per table:

#### ✅ Rename existing local table:

```sql
ALTER TABLE mytable RENAME TO mytable_local;
```

#### ✅ Recreate schema:

```sql
CREATE TABLE mytable (
  id BIGINT,
  tenant_id INT,
  event_time TIMESTAMPTZ,
  payload JSONB,
  PRIMARY KEY (id, tenant_id)
);
```

#### ✅ Distribute the table:

```sql
SELECT create_distributed_table(
  'mytable',             -- table name
  'tenant_id',           -- distribution column
  colocate_with => 'none',
  shard_count => 32
);
```

#### ✅ (Optional) Enable partitioning inside shard:

```sql
ALTER TABLE mytable PARTITION BY RANGE (event_time);
```

Create initial partitions:

```sql
CREATE TABLE mytable_2025_08 PARTITION OF mytable
  FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
```

(Use auto-partitioning function from earlier for convenience.)

#### ✅ Load data:

```sql
INSERT INTO mytable
SELECT * FROM mytable_local;
```

---

### 🔹 **Step 5: Recreate Indexes, Grants, Views**

* Reapply indexes (especially on `event_time`, `tenant_id`)
* Reapply any `GRANT`s:

  ```sql
  GRANT SELECT, INSERT ON mytable TO your_app_user;
  ```
* Update views / foreign keys / materialised views accordingly.

---

### 🔹 **Step 6: Validate Everything**

* Check counts match:

  ```sql
  SELECT count(*) FROM mytable_local;
  SELECT count(*) FROM mytable;
  ```
* Run common application queries to test:

  * Single-tenant queries (→ shard pruning).
  * Time-based queries (→ partition pruning).

---

### 🔹 **Step 7: Optional - Cleanup**

* If all good, drop local table:

  ```sql
  DROP TABLE mytable_local;
  ```

---

## 🔧 Maintenance Tips (Post-Reconfig)

| Task                                  | Frequency | Notes                                     |
| ------------------------------------- | --------- | ----------------------------------------- |
| Create future partitions              | Monthly   | Use `create_monthly_partitions()`         |
| Drop old partitions                   | Monthly   | Use retention function (optional)         |
| Monitor query performance             | Ongoing   | Look for cross-shard joins                |
| Rebalance shards if adding more nodes | As needed | `SELECT rebalance_table_shards('table');` |

👉 Want a zip of SQL templates (rename, distribute, partition, grant, validate) for applying to multiple tables?
