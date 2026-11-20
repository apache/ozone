/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.PerfLevel;
import org.rocksdb.RocksDB;

/**
 * Benchmark of listing an emptied FSO folder that is followed in key order by the tombstones of
 * other emptied folders, comparing an unbounded prefix iterator with one bounded at the prefix.
 *
 * <p>Each folder {@code /1001/2001/<parentId>/} once held one file, which was deleted. Folders are
 * created in sequence, so listing one folder without an upper bound skips the tombstones of every
 * later folder until it reaches the single live key at the end of the table.
 *
 * <p>Run with:
 * <pre>
 *   mvn test -pl :hdds-server-framework \
 *     -Dtest=TestRDBTablePrefixIteratorBenchmark \
 *     -Dgroups=benchmark -Dexcluded-test-groups= \
 *     -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 */
@Tag("benchmark")
public class TestRDBTablePrefixIteratorBenchmark {

  private static final String TABLE = "fileTable";
  private static final int EMPTIED_FOLDERS = 200_000;
  /** Listed folders are taken from the first half, so each unbounded listing skips 100k to 200k tombstones. */
  private static final int LISTED_FOLDERS = 50;
  private static final int ROUNDS = 3;
  private static final long FIRST_PARENT_ID = 1_000_000;
  private static final byte[] VALUE = new byte[200];

  @TempDir
  private File tempDir;

  /** Where the deleted file records are when the folders are listed. */
  private enum Layout {
    /** The record was dropped by flush; only the tombstone remains on disk. */
    TOMBSTONE_ONLY,
    /** The record was compacted to the bottom level before it was deleted; the tombstone sits above it. */
    TOMBSTONE_ABOVE_RECORD,
    /** Tombstones and records were compacted away. */
    COMPACTED
  }

  @Test
  public void benchmarkListEmptiedFolder() throws Exception {
    System.out.println();
    System.out.printf("%d emptied folders, %d listings x %d rounds per row%n", EMPTIED_FOLDERS, LISTED_FOLDERS, ROUNDS);
    System.out.printf("%-24s  %-9s  %14s  %12s  %12s%n", "Layout", "Iterator", "skipped/list", "median", "p99");
    for (Layout layout : Layout.values()) {
      try (ManagedDBOptions options = new ManagedDBOptions();
           ManagedColumnFamilyOptions cfOptions = new ManagedColumnFamilyOptions()) {
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        try (RDBStore store = newStore(layout, options, cfOptions)) {
          final RDBTable table = store.getTable(TABLE);
          final Result unbounded = measure(store, table, false);
          final Result bounded = measure(store, table, true);
          unbounded.print(layout, "unbounded");
          bounded.print(layout, "bounded");

          assertThat(bounded.skippedPerListing).isLessThanOrEqualTo(1);
          if (layout == Layout.COMPACTED) {
            assertThat(unbounded.skippedPerListing).isZero();
          } else {
            assertThat(unbounded.skippedPerListing).isGreaterThanOrEqualTo(EMPTIED_FOLDERS / 2);
            assertThat(bounded.medianNanos * 10).isLessThan(unbounded.medianNanos);
          }
        }
      }
    }
  }

  /** Overhead of the upper bound when there are no tombstones: folders whose files are all live. */
  @Test
  public void benchmarkListLiveFolder() throws Exception {
    final int folders = 2_000;
    final int filesPerFolder = 50;
    try (ManagedDBOptions options = new ManagedDBOptions();
         ManagedColumnFamilyOptions cfOptions = new ManagedColumnFamilyOptions()) {
      options.setCreateIfMissing(true);
      options.setCreateMissingColumnFamilies(true);
      final Set<TableConfig> tables = new HashSet<>();
      tables.add(new TableConfig(StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY), cfOptions));
      tables.add(new TableConfig(TABLE, cfOptions));
      try (RDBStore store = TestRDBStore.newRDBStore(new File(tempDir, "LIVE"), options, tables)) {
        final RDBTable table = store.getTable(TABLE);
        try (BatchOperation batch = store.initBatchOperation()) {
          for (int i = 0; i < folders; i++) {
            for (int f = 0; f < filesPerFolder; f++) {
              table.putWithBatch(batch, StringUtils.string2Bytes("/1001/2001/" + (FIRST_PARENT_ID + i) + "/part-"
                  + (10_000 + f)), VALUE);
            }
          }
          store.commitBatchOperation(batch);
        }
        store.getDb().flush(TABLE);
        store.compactTable(TABLE);

        System.out.println();
        System.out.printf("%d live folders x %d files, every folder listed per round, %d rounds per row%n",
            folders, filesPerFolder, ROUNDS);
        System.out.printf("%-24s  %-9s  %14s  %12s  %12s%n", "Listing", "Iterator", "entries/list", "median", "p99");
        for (int entries : new int[] {1, filesPerFolder}) {
          final long[] unbounded = new long[folders * ROUNDS];
          final long[] bounded = new long[folders * ROUNDS];
          // Round 0 is warm-up. Both iterators list each folder back to back, alternating which goes first.
          for (int round = 0; round <= ROUNDS; round++) {
            for (int i = 0; i < folders; i++) {
              final boolean boundedFirst = (i & 1) == 0;
              final long first = listLive(store, table, boundedFirst, folderPrefix(i), entries);
              final long second = listLive(store, table, !boundedFirst, folderPrefix(i), entries);
              if (round > 0) {
                final int index = (round - 1) * folders + i;
                bounded[index] = boundedFirst ? first : second;
                unbounded[index] = boundedFirst ? second : first;
              }
            }
          }
          final String listing = entries == 1 ? "FIRST_ENTRY" : "WHOLE_FOLDER";
          printLive(listing, "unbounded", entries, unbounded);
          printLive(listing, "bounded", entries, bounded);
        }
      }
    }
  }

  /** @return the time to create a prefix iterator and read the given number of entries. */
  private static long listLive(RDBStore store, RDBTable table, boolean bounded, byte[] prefix, int entries)
      throws Exception {
    final long start = System.nanoTime();
    try (Table.KeyValueIterator<byte[], byte[]> iter = bounded
        ? table.iterator(prefix, IteratorType.KEY_AND_VALUE)
        : new RDBStoreByteArrayIterator(store.getDb().newIterator(table.getColumnFamily(), false), table,
            prefix, IteratorType.KEY_AND_VALUE)) {
      int read = 0;
      while (read < entries && iter.hasNext()) {
        iter.next();
        read++;
      }
      assertThat(read).isEqualTo(entries);
    }
    return System.nanoTime() - start;
  }

  private static void printLive(String listing, String iterator, int entries, long[] nanos) {
    Arrays.sort(nanos);
    System.out.printf("%-24s  %-9s  %14d  %12s  %12s%n", listing, iterator, entries,
        Result.format(nanos[nanos.length / 2]), Result.format(nanos[(int) (nanos.length * 0.99)]));
  }

  private RDBStore newStore(Layout layout, ManagedDBOptions options, ManagedColumnFamilyOptions cfOptions)
      throws Exception {
    final Set<TableConfig> tables = new HashSet<>();
    tables.add(new TableConfig(StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY), cfOptions));
    tables.add(new TableConfig(TABLE, cfOptions));
    final RDBStore store = TestRDBStore.newRDBStore(new File(tempDir, layout.name()), options, tables);
    final RDBTable table = store.getTable(TABLE);
    if (layout == Layout.TOMBSTONE_ABOVE_RECORD) {
      try (BatchOperation batch = store.initBatchOperation()) {
        for (int i = 0; i < EMPTIED_FOLDERS; i++) {
          table.putWithBatch(batch, fileKey(i), VALUE);
        }
        store.commitBatchOperation(batch);
      }
      store.compactTable(TABLE);
      try (BatchOperation batch = store.initBatchOperation()) {
        for (int i = 0; i < EMPTIED_FOLDERS; i++) {
          table.deleteWithBatch(batch, fileKey(i));
        }
        store.commitBatchOperation(batch);
      }
    } else {
      // Put and delete in the same memtable, so the flush drops the record and keeps the tombstone.
      try (BatchOperation batch = store.initBatchOperation()) {
        for (int i = 0; i < EMPTIED_FOLDERS; i++) {
          table.putWithBatch(batch, fileKey(i), VALUE);
          table.deleteWithBatch(batch, fileKey(i));
        }
        store.commitBatchOperation(batch);
      }
    }
    table.put(StringUtils.string2Bytes("/1001/2001/9999999/part-00000"), VALUE);
    store.getDb().flush(TABLE);
    if (layout == Layout.COMPACTED) {
      // Without kForce, a flushed file moved to the bottom level is not rewritten and keeps its tombstones.
      try (ManagedCompactRangeOptions compactOptions = new ManagedCompactRangeOptions()) {
        compactOptions.setBottommostLevelCompaction(ManagedCompactRangeOptions.BottommostLevelCompaction.kForce);
        store.compactTable(TABLE, compactOptions);
      }
    }
    return store;
  }

  private static Result measure(RDBStore store, RDBTable table, boolean bounded) throws Exception {
    final RocksDB db = store.getDb().getManagedRocksDb().get();
    final long[] nanos = new long[LISTED_FOLDERS * ROUNDS];
    long skipped = 0;
    db.setPerfLevel(PerfLevel.ENABLE_COUNT);
    try {
      // Round 0 warms up the block cache and the JIT and is not recorded.
      for (int round = 0; round <= ROUNDS; round++) {
        for (int i = 0; i < LISTED_FOLDERS; i++) {
          final byte[] prefix = folderPrefix(i * (EMPTIED_FOLDERS / 2 / LISTED_FOLDERS));
          db.getPerfContext().reset();
          final long start = System.nanoTime();
          try (Table.KeyValueIterator<byte[], byte[]> iter = bounded
              ? table.iterator(prefix, IteratorType.KEY_ONLY)
              : new RDBStoreByteArrayIterator(store.getDb().newIterator(table.getColumnFamily(), false), table,
                  prefix, IteratorType.KEY_ONLY)) {
            assertFalse(iter.hasNext());
          }
          final long elapsed = System.nanoTime() - start;
          if (round > 0) {
            nanos[(round - 1) * LISTED_FOLDERS + i] = elapsed;
            skipped += db.getPerfContext().getInternalDeleteSkippedCount();
          }
        }
      }
    } finally {
      db.setPerfLevel(PerfLevel.DISABLE);
    }
    Arrays.sort(nanos);
    return new Result(skipped / nanos.length, nanos[nanos.length / 2], nanos[(int) (nanos.length * 0.99)]);
  }

  private static byte[] folderPrefix(int i) {
    return StringUtils.string2Bytes("/1001/2001/" + (FIRST_PARENT_ID + i) + "/");
  }

  private static byte[] fileKey(int i) {
    return StringUtils.string2Bytes("/1001/2001/" + (FIRST_PARENT_ID + i) + "/part-00000");
  }

  private static final class Result {
    private final long skippedPerListing;
    private final long medianNanos;
    private final long p99Nanos;

    Result(long skippedPerListing, long medianNanos, long p99Nanos) {
      this.skippedPerListing = skippedPerListing;
      this.medianNanos = medianNanos;
      this.p99Nanos = p99Nanos;
    }

    void print(Layout layout, String iterator) {
      System.out.printf("%-24s  %-9s  %14d  %12s  %12s%n",
          layout, iterator, skippedPerListing, format(medianNanos), format(p99Nanos));
    }

    private static String format(long nanos) {
      return nanos >= 1_000_000 ? String.format("%.2f ms", nanos / 1e6) : String.format("%.1f µs", nanos / 1e3);
    }
  }
}
