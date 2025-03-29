---
title: Aggressive DB Compaction with Minimal Degradation
summary: Automatically compactRange on RocksDB with statistics of SST File
date: 2025-03-27
jira: HDDS-12682
status: accepted
author: Peter Lee
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Aggressive DB Compaction with Minimal Degradation

## Short Introduction

Use the `numEntries` and `numDeletion` in [TableProperties](https://github.com/facebook/rocksdb/blob/main/java/src/main/java/org/rocksdb/TableProperties.java#L12) which stores statistics for each SST as "guidance" to determine how to split tables into finer ranges for compaction.

## Motivation

Our current approach of compacting entire column families directly would significantly impact online performance through excessive write amplification. After researching TiKV and RocksDB compaction mechanisms, it's clear we need a more sophisticated solution that better balances maintenance operations with user workloads.

TiKV runs background tasks for compaction and logically splits key ranges into table regions (with default size limits of 256MB per region), allowing gradual scanning and compaction of known ranges. While we can use the built-in `TableProperties` in SST files to check metrics like `num_entries` and `num_deletion`, these only represent operation counts without deduplicating keys. TiKV addresses this with a custom `MVCTablePropertiesCollector` for more accurate results, but unfortunately, the Java API doesn't currently support custom collectors, forcing us to rely on built-in statistics.

For the Ozone Manager implementation, we face a different challenge since OM lacks the concept of size-based key range splits. The most logical division we can use is the bucket prefix (file table). For FSO buckets, we can further divide key ranges based on directory `parent_id`, enabling more granular and targeted compaction that minimizes disruption to ongoing operations.

By implementing bucket-level compaction with proper paging mechanisms like `next_bucket` and potentially `next_parent_id` for directory-related tables, we can achieve more efficient storage utilization while maintaining performance. The Java APIs currently provide enough support to implement these ideas, making this approach viable for Ozone Manager.

## Proposed Changes

### RocksDB Java API Used

- [`public Map<String, TableProperties> getPropertiesOfTablesInRange(final ColumnFamilyHandle columnFamilyHandle, final List<Range> ranges)`](https://github.com/facebook/rocksdb/blob/934cf2d40dc77905ec565ffec92bb54689c3199c/java/src/main/java/org/rocksdb/RocksDB.java#L4575)
    - Given a list of `Range`, returns a map of `TableProperties` in these ranges.
- [TableProperties](https://github.com/facebook/rocksdb/blob/main/java/src/main/java/org/rocksdb/TableProperties.java#L12)
    - Statistical data for one SST file.
- [Range](https://github.com/facebook/rocksdb/blob/934cf2d40dc77905ec565ffec92bb54689c3199c/java/src/main/java/org/rocksdb/Range.java)
    - Contains one start [slice](https://javadoc.io/doc/org.rocksdb/rocksdbjni/6.20.3/org/rocksdb/Slice.html) and one end slice.

### New Configuration Set

Introduce four new configuration strings:
- `bucket_compact_check_interval`: Interval (ms) to check whether to start compaction for a region.
- `bucket_compact_max_entries_sum`: Upper bound of num_entries sum from all SST files in one compaction range. Default value is 1000000.
- `bucket_compact_tombstone_percentage`: Only compact range when `num_entries * tombstone_percentage / 100 <= num_deletion`. Default value is 30.
- `bucket_compact_min_tombstones`: Minimum number of tombstones to trigger manual compaction. Default value is 10000.

### Create Compactor For Each Table

Create new compactor instances for each table, including `KEY_TABLE`, `DELETED_TABLE`, `DELETED_DIR_TABLE`, `DIRECTORY_TABLE`, and `FILE_TABLE`. Run these background workers using a scheduled executor with configured interval and a random start time to spread out the workload.

### (Optional) CacheIterator Support for Seek with Prefix

1. The current interface of bucketIterator in `OMMetadataManager` returns a CacheIterator for bucket table (with `FULL_TABLE_CACHE` in non-snapshot metadata manager), but the cache iterator currently doesn't support seeking with prefix. Since FullTableCache uses ConcurrentSkipList as cache, we can support seeking with prefix in $O(\log{n})$ time.
    - If seeking with prefix is called on partial table cache, it should raise an unsupported operation error.
2. However, since BucketIterator doesn't require high performance, using the seekable table iterator in `TypedTable` might be sufficient.

### Support RocksDatabase to get range stats

```java
public class KeyRange {
    private final String startKey;
    private final String endKey;

    public Range toRocksRange() {
        return new Range(new Slice(stringToBytes(startKey)), new Slice(stringToBytes(endKey)));
    }
}

public class KeyRangeStats {
    // Can support more fields in the future
    int numEntries;
    int numDeletion;

    public static KeyRangeStats fromTableProperties(TableProperties properties) {
        ...
    }

    // Make this mergeable for continuous ranges
    public void add(KeyRangeStats other) {
        this.numEntries += other.numEntries;
        this.numDeletion += other.numDeletion;
    }
}

public class RocksDatabase {
    List<KeyRangeStats> getRangeStats(ColumnFamilyHandle columnFamilyHandle, KeyRange range) {
        Map<String, TableProperties> tableProperties = getPropertiesOfTablesInRange(columnFamilyHandle, range.toRocksRange());
        List<KeyRangeStats> stats = new ArrayList<>();
        for (TableProperties properties : tableProperties.values()) {
            stats.add(KeyRangeStats.fromTableProperties(properties));
        }
        return stats;
    }
}
```

### Two Types of Compactors

#### Compactor for OBS and Legacy Layout

For the following tables, since the bucket key prefix is consecutive, if there are consecutive buckets that need compaction, merge them. Note that we still need to keep the range key sum below the configured limit.

| Column Family  | Key                              | Value             |
| -------------- | -------------------------------- | ----------------- |
| `keyTable`     | `/volumeName/bucketName/keyName` | `KeyInfo`         |
| `deletedTable` | `/volumeName/bucketName/keyName` | `RepeatedKeyInfo` |

Pseudo code:

```java
class BucketCompactor {
    private final OMMetadataManager metadataMgr;

    // Pagination key
    // These fields would have values if the compaction range of the previous bucket is too large, 
    // and the range of that bucket is split down.
    // This could also be encapsulated to be shared between OBS and FSO compactor
    private BucketInfo nextBucket;
    private String nextKey;

    private Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> getBucketIterator() {
        iterator = metadataMgr.getBucketIterator(nextBucket);
        // Reset if iterator reaches the end
        if (!iterator.hasNext()) iterator.seekToFirst();
        return iterator;
    }

    // Run with scheduled executor
    private void run() {
        iterator = getBucketIterator();
        List<Range> ranges = collectNeedCompactionRanges(iterator, db, threshold);
    }

    // Check the SST properties for each bucket, and compact a bucket if it contains too many RocksDB tombstones.
    // Merge multiple neighboring buckets that need compacting into a single range.
    private List<Range> collectNeedCompactionRanges(Iterator bucketIterator, DBstore db, int minTombstoneThreshold, int maxEntriesSum) {
        List<Range> ranges = new ArrayList<>();

        while (bucketIterator.hasNext()) {
            if (nextBucket == null) {
                // Handle pagination
            }

            Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry = bucketIterator.next();
            if (/* Bucket range not too large or only one SST covers the whole bucket */) {
                // See if the range of this bucket needs compaction
            } else {
                // 1. Use binary search to find the **end key** of the bucket that's below the numEntriesSum limit,
                //    where the sum of numEntries of all SSTs in this range[startKey, **endKey**] is below the limit
                // 2. See if the range of this bucket needs compaction
                // 3. Set pagination key to the **end key**
            }

            // Merge ranges if there are continuous ranges that need compaction and don't exceed the maxEntriesSum limit
        }
    }

    private boolean needCompact(KeyRangeStats mergedRangeStats, int minTombstoneThreshold, int maxEntriesSum) {
        if (mergedRangeStats.numDeletion < minTombstoneThreshold) {
            return false;
        }

        return mergedRangeStats.numEntries * tombstone_percentage / 100 <= mergedRangeStats.numDeletion;
    }
}
```

#### Compactor for FSO Layout

For the following tables, since the bucket key prefix is **not** consecutive, we won't merge different key ranges from different buckets.

| Column Family     | Key                                            | Value     |
| ----------------- | ---------------------------------------------- | --------- |
| `directoryTable`  | `/volumeId/bucketId/parentId/dirName`          | `DirInfo` |
| `fileTable`       | `/volumeId/bucketId/parentId/fileName`         | `KeyInfo` |
| `deletedDirTable` | `/volumeId/bucketId/parentId/dirName/objectId` | `KeyInfo` |

Pseudo code:

```java
class FSOBucketCompactor {
    // Share the same logic with OBS compactor
    // **But don't merge different key ranges from different buckets**
}
```

## Test Plan

- Unit tests
- Need some benchmarks

### Benchmark

#### Manual Compaction on Range (This proposal)

#### Built-in `CompactOnDeletionCollector` with different argument sets

`CompactOnDeletionCollector` is a built-in collector in RocksDB that marks an SST file as needing compaction when the number of deletions is greater than a threshold in a specific sliding window.

## Documentation Plan

We should set some heuristics based on benchmark: https://cs-people.bu.edu/mathan/publications/edbt25-wei.pdf

- `bucket_compact_check_interval`: Interval (ms) to check whether to start compaction for a region.
- `bucket_compact_max_entries_sum`: Upper bound of num_entries sum from all SST files in one compaction range. Default value is 1000000.
- `bucket_compact_tombstone_percentage`: Only compact range when `num_entries * tombstone_percentage / 100 <= num_deletion`. Default value is 30.
- `bucket_compact_min_tombstones`: Minimum number of tombstones to trigger manual compaction. Default value is 10000.

## Additional Note

1. Once RocksDB Java supports custom `TablePropertiesCollector`, we should leverage that to do finer key range splits.
