* **Citus distribution (sharding across nodes)** → by `tenant_id`
* **Postgres native declarative partitioning (inside each shard)** → by `event_time` (monthly partitions)

---

## 🔹 Step 1. Create Base Table

This is the schema definition, but initially a *local table*.

```sql
CREATE TABLE events (
    id BIGSERIAL,
    tenant_id INT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    payload JSONB,
    PRIMARY KEY (id, tenant_id)   -- composite PK works better with distributed tables
);
```

---

## 🔹 Step 2. Distribute the Table

Tell Citus to shard this table across workers by `tenant_id`:

```sql
SELECT create_distributed_table(
    'events',
    'tenant_id',
    colocate_with => 'none',
    shard_count => 32
);
```

Now `events` is distributed across shards (by `tenant_id`).

---

## 🔹 Step 3. Add Native Partitioning Inside Shards

Inside each shard, we add **time‑based partitions**.
⚠️ Since `events` is already distributed, the partitioning definition must be applied to each shard template — Citus will propagate it when creating new shards.

```sql
-- Convert base distributed table to partitioned
ALTER TABLE events
PARTITION BY RANGE (event_time);

-- Create partitions (inside each shard)
CREATE TABLE events_2025_07 PARTITION OF events
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');

CREATE TABLE events_2025_08 PARTITION OF events
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');

-- Optional: default partition for late/early data
CREATE TABLE events_default PARTITION OF events DEFAULT;
```

At this point, each shard of `events` is actually a partitioned table under the covers.

---

## 🔹 Step 4. Indexes (Per Partition)

You can add indexes per partition (Postgres does not automatically propagate them):

```sql
CREATE INDEX ON events_2025_07 (tenant_id, event_time);
CREATE INDEX ON events_2025_08 (tenant_id, event_time);
```

For maintenance, you can script index creation on each new monthly partition.

---

## 🔹 Step 5. Data Insert Example

```sql
INSERT INTO events (tenant_id, event_time, payload)
VALUES
  (101, '2025-07-15 10:00:00+00', '{"type": "click"}'),
  (101, '2025-08-02 12:00:00+00', '{"type": "purchase"}');
```

* Citus routes by `tenant_id` → chooses shard.
* Native partitioning inside shard routes by `event_time` → chooses monthly partition.

---

## 🔹 Step 6. Query Example

```sql
-- Coordinator will prune shards first, then Postgres prunes partitions inside shard
SELECT count(*)
FROM events
WHERE tenant_id = 101
  AND event_time BETWEEN '2025-07-01' AND '2025-07-31';
```

This query:

1. Hits only shards for tenant `101`.
2. Inside each shard, hits only partition `events_2025_07`.

---

✅ **Result**:

* Horizontal scale from sharding.
* Lifecycle management and query pruning from partitioning.

---
