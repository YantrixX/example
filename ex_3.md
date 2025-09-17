

### Why normal users lose access

1. When you `CREATE TABLE ... (LIKE ... INCLUDING ALL)` you inherit the schema, ownership, and permissions from the original.
   At this stage, grants to your “normal” users still work.

2. But when you call `SELECT create_distributed_table(...)` or `create_distributed_table(..., colocate_with => ...)` and then `create_reference_table(...)` or add partitions:

   * Citus rewrites metadata and may change ownership at the coordinator level.
   * On worker nodes, it creates **shards (foreign tables)** or partitions under the hood.
     Those new shard tables are owned by the **citus system user** and by default, **your grants don’t propagate** to them.

3. So your normal users can still “see” the parent table, but queries fail because they don’t have access to the underlying shard/partition tables.

---

### How to fix it

You’ll need to **reapply permissions after distribution/partitioning**, not only on the parent table but also on the shards.

Options:

1. **Grant schema-wide privileges**
   If your normal users need read/write to everything in the schema:

   ```sql
   GRANT USAGE ON SCHEMA my_schema TO my_user;
   GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA my_schema TO my_user;
   GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA my_schema TO my_user;
   ```

2. **Future shard propagation**
   Ensure future shards inherit permissions automatically:

   ```sql
   ALTER DEFAULT PRIVILEGES IN SCHEMA my_schema
     GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO my_user;
   ALTER DEFAULT PRIVILEGES IN SCHEMA my_schema
     GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO my_user;
   ```

   This way, any new shard or partition created by Citus will automatically grant the right privileges.

3. **Fix existing shards**
   For already-distributed tables, run:

   ```sql
   SELECT run_command_on_workers($cmd$
     GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA my_schema TO my_user;
   $cmd$);
   ```

   This applies grants on worker shards.

---

### Practical workflow

When converting:

1. `CREATE TABLE new_table (LIKE old_table INCLUDING ALL);`
2. `SELECT create_distributed_table('new_table', 'distribution_column', 'hash');`
3. Add partitions if needed (`CREATE TABLE ... PARTITION OF ...`).
4. Reapply grants (schema + default privileges + worker shard grants).

