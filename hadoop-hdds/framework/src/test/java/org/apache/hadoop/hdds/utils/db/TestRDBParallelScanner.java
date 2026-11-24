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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.cache.TableCache;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.RocksDB;

/**
 * Tests for {@link RDBParallelScanner} and {@link RDBSplitIterator}.
 */
public class TestRDBParallelScanner {
  private static final String CF = "First";
  private static final String PREFIXED_CF = "Prefixed";
  private static final ParallelScanConfig ENABLED =
      new ParallelScanConfig(true, 8);

  @TempDir
  private File tempDir;
  private RDBStore store;
  private ManagedDBOptions options;
  private final List<ManagedColumnFamilyOptions> cfOptionsList = new ArrayList<>();
  private RDBTable table;

  @BeforeEach
  public void setUp() throws Exception {
    options = new ManagedDBOptions();
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);
    final Set<TableConfig> configs = new HashSet<>();
    final ManagedColumnFamilyOptions defaultCfOpts = new ManagedColumnFamilyOptions();
    final ManagedColumnFamilyOptions firstCfOpts = new ManagedColumnFamilyOptions();
    final ManagedColumnFamilyOptions prefixedCfOpts = new ManagedColumnFamilyOptions();
    prefixedCfOpts.useFixedLengthPrefixExtractor(4);
    cfOptionsList.add(defaultCfOpts);
    cfOptionsList.add(firstCfOpts);
    cfOptionsList.add(prefixedCfOpts);
    configs.add(new TableConfig(
        StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY), defaultCfOpts));
    configs.add(new TableConfig(CF, firstCfOpts));
    configs.add(new TableConfig(PREFIXED_CF, prefixedCfOpts));
    store = TestRDBStore.newRDBStore(tempDir, options, configs, 1000);
    table = newTable(ENABLED);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (store != null) {
      store.close();
    }
    for (ManagedColumnFamilyOptions cfOpts : cfOptionsList) {
      cfOpts.close();
    }
    cfOptionsList.clear();
    if (options != null) {
      options.close();
    }
  }

  private RDBTable newTable(ParallelScanConfig config) {
    final RocksDatabase db = store.getDb();
    return new RDBTable(db, db.getColumnFamily(CF), null, config);
  }

  static byte[] key(String prefix, int i) {
    return String.format("%s%08d", prefix, i).getBytes(UTF_8);
  }

  /** Write keys in batches, flushing after each to create multiple SSTs. */
  private List<String> populate(String prefix, int total, int ssts)
      throws Exception {
    final List<String> keys = new ArrayList<>();
    final int perSst = Math.max(1, total / ssts);
    for (int i = 0; i < total; i++) {
      final byte[] k = key(prefix, i);
      table.put(k, ("v" + i).getBytes(UTF_8));
      keys.add(new String(k, UTF_8));
      if ((i + 1) % perSst == 0) {
        store.flushDB();
      }
    }
    store.flushDB();
    return keys;
  }

  private List<String> sequentialScan(byte[] prefix) throws Exception {
    final List<String> keys = new ArrayList<>();
    try (Table.KeyValueIterator<byte[], byte[]> it = table.iterator(prefix)) {
      while (it.hasNext()) {
        keys.add(new String(it.next().getKey(), UTF_8));
      }
    }
    return keys;
  }

  private static final class Collected {
    private final RDBParallelScanner.Result result;
    private final List<String> keys;

    Collected(RDBParallelScanner.Result result, List<String> keys) {
      this.result = result;
      this.keys = keys;
    }
  }

  private Collected parallelScan(RDBTable t, byte[] prefix, int threads,
      java.util.function.Consumer<Table.KeyValue<byte[], byte[]>> hook)
      throws Exception {
    final ConcurrentLinkedQueue<String> out = new ConcurrentLinkedQueue<>();
    final RDBParallelScanner.Result r = t.parallelScanWithResult(prefix,
        threads, kv -> {
          if (hook != null) {
            hook.accept(kv);
          }
          out.add(new String(kv.getKey(), UTF_8));
        });
    final List<String> keys = new ArrayList<>(out);
    final int before = keys.size();
    Collections.sort(keys);
    assertEquals(before, new HashSet<>(keys).size(), "duplicates found");
    assertEquals(r.getKeys(), before);
    // every iterator closed exactly once, snapshot released
    assertEquals(r.getSplits(), r.getIteratorsOpened());
    assertEquals(r.getIteratorsOpened(), r.getIteratorsClosed());
    assertTrue(r.isSnapshotReleased());
    return new Collected(r, keys);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 4, 8})
  public void testParallelEqualsSequential(int threads) throws Exception {
    populate("k", 20_000, 8);
    final List<String> expected = sequentialScan(null);
    assertEquals(20_000, expected.size());
    final Collected c = parallelScan(table, null, threads, null);
    assertEquals(expected, c.keys);
    if (threads > 1) {
      assertTrue(c.result.getSplits() > 1, "expected splits: " + c.result);
      assertTrue(c.result.getThreads() > 1, c.result.toString());
      assertTrue(c.result.getThreads() <= threads);
    }
  }

  @Test
  public void testDisabledConfigUsesSingleThread() throws Exception {
    populate("k", 5_000, 4);
    final Collected c = parallelScan(newTable(ParallelScanConfig.DISABLED),
        null, 8, null);
    assertEquals(1, c.result.getThreads());
    assertEquals(1, c.result.getSplits());
    assertEquals(sequentialScan(null), c.keys);
  }

  @Test
  public void testThreadsCappedByConfig() throws Exception {
    populate("k", 20_000, 8);
    final Collected c = parallelScan(newTable(new ParallelScanConfig(true, 2)),
        null, 16, null);
    assertEquals(2, c.result.getThreads());
    assertEquals(sequentialScan(null), c.keys);
  }

  @Test
  public void testWritesDuringScanNotVisible() throws Exception {
    populate("k", 20_000, 8);
    final List<String> expected = sequentialScan(null);
    final AtomicBoolean written = new AtomicBoolean();
    // threads < splits, so some split iterators are created after the writes:
    // only the shared snapshot keeps them consistent.
    final Collected c = parallelScan(table, null, 2, kv -> {
      if (written.compareAndSet(false, true)) {
        try {
          for (int i = 0; i < 20_000; i += 7) {
            table.put(key("k", i), "changed".getBytes(UTF_8));
            table.put(key("k", i + 1_000_000), "new".getBytes(UTF_8));
            table.delete(key("k", i + 3));
          }
          table.put("a-before-all".getBytes(UTF_8), new byte[0]);
          table.put("z-after-all".getBytes(UTF_8), new byte[0]);
          store.flushDB();
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    });
    assertTrue(c.result.getSplits() > c.result.getThreads());
    assertEquals(expected, c.keys);
    // and the writes really happened
    assertTrue(sequentialScan(null).contains("z-after-all"));
  }

  @Test
  public void testCompactionDuringScanDoesNotChangeResult() throws Exception {
    populate("k", 20_000, 8);
    // overwrite and delete some keys, then flush, so compaction has work
    for (int i = 0; i < 20_000; i += 5) {
      table.delete(key("k", i));
    }
    store.flushDB();
    final List<String> expected = sequentialScan(null);
    final int sstsBefore = store.getDb().getLiveFilesMetaData().size();
    final AtomicBoolean compacted = new AtomicBoolean();
    final Collected c = parallelScan(table, null, 4, kv -> {
      if (compacted.compareAndSet(false, true)) {
        try {
          store.getDb().compactRange(CF);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    });
    assertTrue(compacted.get());
    assertEquals(expected, c.keys);
    assertTrue(store.getDb().getLiveFilesMetaData().size() < sstsBefore,
        "compaction did not run");
  }

  @Test
  public void testPrefixScanNonNullStartKey() throws Exception {
    populate("a", 3_000, 3);
    populate("b", 6_000, 6);
    populate("c", 3_000, 3);
    final byte[] prefix = "b".getBytes(UTF_8);
    final List<String> expected = sequentialScan(prefix);
    assertEquals(6_000, expected.size());
    final Collected c = parallelScan(table, prefix, 4, null);
    assertEquals(expected, c.keys);
    for (String k : c.keys) {
      assertTrue(k.startsWith("b"), k);
    }
  }

  @Test
  public void testBoundaryEqualToSstEndpoint() throws Exception {
    final List<String> all = populate("k", 8_000, 8);
    // Pick a prefix whose start key is exactly an SST smallest key.
    final byte[] firstOfSecondSst = key("k", 1_000);
    final List<KeyRange> splits = RDBParallelScanner.computeSplits(
        store.getDb(), table.getColumnFamily(),
        new KeyRange(firstOfSecondSst, null), 100);
    assertTrue(splits.size() > 1);
    assertEquals(new String(firstOfSecondSst, UTF_8),
        new String(splits.get(0).getStart(), UTF_8));
    // A full scan: every SST endpoint is a boundary; check no gaps/dups.
    final Collected c = parallelScan(table, null, 8, null);
    assertEquals(all, c.keys);
    // each split iterator respects its bounds
    for (KeyRange r : RDBParallelScanner.computeSplits(store.getDb(),
        table.getColumnFamily(), new KeyRange(null, null), 100)) {
      try (RDBSplitIterator it = new RDBSplitIterator(store.getDb(),
          table.getColumnFamily(), null, r)) {
        while (it.hasNext()) {
          assertTrue(r.contains(it.next().getKey()), r.toString());
        }
      }
    }
  }

  @Test
  public void testEmptyTable() throws Exception {
    final Collected c = parallelScan(table, null, 4, null);
    assertEquals(0, c.keys.size());
    assertEquals(1, c.result.getSplits());
    final Collected p = parallelScan(table, "x".getBytes(UTF_8), 4, null);
    assertEquals(0, p.keys.size());
  }

  @Test
  public void testConsumerFailureClosesEverything() throws Exception {
    populate("k", 20_000, 8);
    final IllegalStateException failure = new IllegalStateException("boom");
    final IllegalStateException thrown = assertThrows(
        IllegalStateException.class, () -> table.parallelScan(null, 4, kv -> {
          throw failure;
        }));
    assertSame(failure, thrown);
    // the DB is still usable and no snapshot/iterator leaked a DB reference
    assertEquals(20_000, sequentialScan(null).size());
  }

  @Test
  public void testTypedTableParallelScan() throws Exception {
    populate("k", 5_000, 4);
    final TypedTable<String, String> typed = new TypedTable<>(table,
        StringCodec.get(), StringCodec.get(), TableCache.CacheType.NO_CACHE);
    final ConcurrentLinkedQueue<String> out = new ConcurrentLinkedQueue<>();
    final long n = typed.parallelScan("k", 4, kv -> {
      assertTrue(kv.getValueByteSize() > 0);
      out.add(kv.getKey());
    });
    assertEquals(5_000, n);
    final List<String> keys = new ArrayList<>(out);
    Collections.sort(keys);
    assertEquals(sequentialScan(null), keys);
  }

  @Test
  public void testIteratorTypeKeyOnlyAndValueOnly() throws Exception {
    populate("k", 4_000, 4);
    final Codec<String> failingCodec = new Codec<String>() {
      @Override
      public Class<String> getTypeClass() {
        return String.class;
      }

      @Override
      public byte[] toPersistedFormat(String object) throws CodecException {
        return StringCodec.get().toPersistedFormat(object);
      }

      @Override
      public String fromPersistedFormat(byte[] rawData) throws CodecException {
        throw new CodecException("Should not decode unused component");
      }

      @Override
      public String copyObject(String object) {
        return object;
      }
    };

    final TypedTable<String, String> keyOnlyTable = new TypedTable<>(
        table, StringCodec.get(), failingCodec, TableCache.CacheType.NO_CACHE);
    final ConcurrentLinkedQueue<String> keys = new ConcurrentLinkedQueue<>();
    assertEquals(4_000, keyOnlyTable.parallelScan("k", 4, IteratorType.KEY_ONLY, kv -> {
      assertNull(kv.getValue());
      keys.add(kv.getKey());
    }));
    assertEquals(4_000, keys.size());

    final TypedTable<String, String> valueOnlyTable = new TypedTable<>(
        table, failingCodec, StringCodec.get(), TableCache.CacheType.NO_CACHE);
    final ConcurrentLinkedQueue<String> values = new ConcurrentLinkedQueue<>();
    assertEquals(4_000, valueOnlyTable.parallelScan("k", 4, IteratorType.VALUE_ONLY, kv -> {
      assertNull(kv.getKey());
      assertTrue(kv.getValueByteSize() > 0);
      values.add(kv.getValue());
    }));
    assertEquals(4_000, values.size());
  }

  @Test
  public void testFailFastAbortDoesNotWaitForSlowFirstSplit() throws Exception {
    populate("k", 8_000, 8);
    final byte[] slowSplitKey = key("k", 0);
    final CountDownLatch splitZeroEntered = new CountDownLatch(1);
    final CountDownLatch failureTriggered = new CountDownLatch(1);
    final IllegalStateException boom = new IllegalStateException("fail-fast");
    final long startNs = System.nanoTime();

    final IllegalStateException thrown = assertThrows(IllegalStateException.class,
        () -> table.parallelScan(null, 4, kv -> {
          if (java.util.Arrays.equals(kv.getKey(), slowSplitKey)) {
            splitZeroEntered.countDown();
            try {
              assertTrue(failureTriggered.await(15, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          } else {
            try {
              assertTrue(splitZeroEntered.await(15, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            failureTriggered.countDown();
            throw boom;
          }
        }));
    assertSame(boom, thrown);
    final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
    assertTrue(elapsedMs < 10_000, "Abort took too long: " + elapsedMs + " ms");
  }

  @Test
  public void testPrefixExtractorColumnFamilyParallelScan() throws Exception {
    final RocksDatabase db = store.getDb();
    final RDBTable prefixedTable = new RDBTable(db, db.getColumnFamily(PREFIXED_CF), null, ENABLED);
    final List<String> expected = new ArrayList<>();
    for (int p = 0; p < 8; p++) {
      final String pfx = String.format("p%03d", p);
      for (int i = 0; i < 500; i++) {
        final String k = String.format("%s-%04d", pfx, i);
        prefixedTable.put(k.getBytes(UTF_8), ("v" + i).getBytes(UTF_8));
        expected.add(k);
      }
      store.flushDB();
    }
    Collections.sort(expected);
    final Collected c = parallelScan(prefixedTable, null, 4, null);
    assertTrue(c.result.getSplits() > 1);
    assertEquals(expected, c.keys);
  }

  @Test
  public void testInMemoryTableDefaultParallelScan() throws Exception {
    final InMemoryTestTable<String, String> mem = new InMemoryTestTable<>();
    mem.put("a/1", "v1");
    mem.put("b/1", "v2");
    mem.put("b/2", "v3");
    final List<String> found = new ArrayList<>();
    assertEquals(2, mem.parallelScan("b/", 4, IteratorType.KEY_ONLY, kv -> {
      assertNull(kv.getValue());
      found.add(kv.getKey());
    }));
    assertEquals(java.util.Arrays.asList("b/1", "b/2"), found);
  }
}
