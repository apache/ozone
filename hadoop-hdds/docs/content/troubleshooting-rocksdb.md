---
title: "Troubleshooting RocksDB in Ozone"
weight: 180
---

This page is a quick operator runbook for common RocksDB issues in Ozone components (OM, Recon, Datanode metadata, Snapshot DBs).

## Quick Checks
- Verify native library: `ozone debug check-native` (loads RocksDB JNI and prints lib path).
- Confirm options in effect: `ozone debug ldb --db=<path> list_column_families` then `ozone debug ldb --db=<path> get_live_files_metadata` to see block size, compression, etc.
- Check open files and max_open_files: `ls -1 <db>/` and compare to `ozone.om.snapshot.rocksdb.max.open.files` / `ozone.om.rocksdb.max.open.files`.

## Symptom → Action
- **Compaction stalls / write stalls**
  - Look for `Stall` and `Slowdown` counters in RocksDBStats metrics and OM logs.
  - Run `ozone debug om compaction-log-dag --db=<path>` to inspect compaction DAG; prune if needed with `ozone repair ldb manual-compaction` (stop service first).
  - Reduce L0 buildup: increase `target_file_size_base`, tune `level0_slowdown_writes_trigger`, or increase `max_background_jobs` via ini file (`hdds.datanode.metadata.rocksdb.ini` / `ozone.om.rocksdb.ini`).

- **High latency / iterator scans slow**
  - Enable block cache metrics; check `rocksdb.block.cache.*` in JMX.
  - Use iterator lower/upper bounds to avoid full DB scans; ensure `ozone.metastore.rocksdb.statistics` is not OFF when debugging.

- **WAL corruption / cannot open DB**
  - Capture error code from RocksDB message; check if WAL replay failed.
  - Try `ozone debug ldb --db=<path> check` (read-only); if unrecoverable, restore from last valid checkpoint under snapshot content lock.

- **Options drift after upgrades**
  - Use `ozone tool rocksdb options` (preserves previous options) before upgrades; compare current `OPTIONS-*` files in DB dir.
  - Keep ini files checked into config mgmt; reapply on restart.

- **Checksum errors on SST**
  - Identify file via log; run `ozone debug ldb --db=<path> get_live_files_metadata | grep <sst>`.
  - If isolated, delete snapshot content lock and rebuild from latest checkpoint; otherwise trigger full re-replication/restore.

## Preventive Settings (per `ozone-default.xml`)
- `ozone.metastore.rocksdb.statistics` – enable StatsLevel for visibility (EXCEPT_DETAILED_TIMERS is a good default).
- `ozone.metastore.rocksdb.cf.write.buffer.size` – tune memtable per CF to match RAM and write rate.
- `ozone.om.snapshot.rocksdb.metrics.enabled` – keep ON for snapshot DBs unless perf testing.
- `hdds.datanode.metadata.rocksdb.cache.size` – size block cache per host for container DBs.

## Useful Commands
- List CFs: `ozone debug ldb --db=<path> list_column_families`
- Live files metadata: `ozone debug ldb --db=<path> get_live_files_metadata`
- Manual compaction (offline): `ozone repair ldb manual-compaction --db=<path> --column-family=<name>`
- Snapshot diff check DAG: `ozone debug om compaction-log-dag --db=<path>`

## When to Fall Back
- If compaction DAG traversal throws overflow guard, drop to full diff path and re-create DAG from latest checkpoints.
- If iterator-based diff fails repeatedly, toggle `performNonNativeDiff=true` and collect logs before re-enabling.

## What to Capture for Bugs
- Component (OM/Recon/Datanode), DB path, RocksDB version (`pom.xml` shows baseline), full stack trace, `OPTIONS-*` files, and last 200 lines of log around failure.
