# Reload & Backfill Scripts

This folder contains utility scripts for reloading and backfilling data in the analytical warehouse.

## Scenarios

### 1. **Full Warehouse Reset**
Complete reset of all tables and checkpoints — useful for testing or full historical reload.
```bash
python full_warehouse_reset.py
```

### 2. **Reset Specific Table**
Truncate a specific table and optionally reset its checkpoint.
```bash
python reset_table.py --table dim_customer --reset-checkpoint
```

### 3. **Reset Checkpoints**
Reset Kafka checkpoints to replay from a specific offset or earliest.
```bash
python reset_checkpoints.py --consumer customers --offset earliest
python reset_checkpoints.py --consumer products --offset 1000
```

### 4. **Backfill from Source**
Re-sync data from source database without losing historical records.
```bash
python backfill_table.py --table dim_customer --from-date 2024-01-01
```

## Before Running Scripts

1. **Stop all streaming jobs** — Ensure no active Spark streams are writing
2. **Backup checkpoint state** — Save current checkpoint locations if you may need to replay
3. **Review the impact** — These scripts modify/truncate data

## Checkpoint Locations

Checkpoints are stored in the path defined by `config["checkpoint_base"]`:
- `customers/` — Customers stream checkpoint
- `products/` — Products stream checkpoint
- `orders/` — Orders stream checkpoint
- `order_items/` — Order items stream checkpoint

## Recovery Patterns

### Pattern 1: Replay Last N Hours
```bash
# Stop all streams
# Reset checkpoints to earliest
python reset_checkpoints.py --consumer all --offset earliest

# Modify Kafka topic retention or use offset tracking to replay
# Restart streams — they will re-process from earliest
```

### Pattern 2: Fix a Specific Table
```bash
# Truncate only the affected table
python reset_table.py --table fact_orders

# Reset just that consumer's checkpoint
python reset_checkpoints.py --consumer orders --offset earliest

# Restart only that stream to reprocess
```

### Pattern 3: Backfill Historical Data
```bash
# Use backfill script to reload data for a date range
python backfill_table.py --table dim_customer --from-date 2024-01-01 --to-date 2024-12-31
```

## Important Notes

- All truncation is **permanent** — ensure you have backups
- Checkpoints are managed by Spark — deleting them forces replay from the offset
- On restart, streams will process **all** Kafka messages since the checkpoint, which can be slow for large backlogs
- Monitor database connections and resource usage during bulk reloads
