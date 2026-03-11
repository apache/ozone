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

import static org.apache.hadoop.hdds.utils.db.IteratorType.KEY_AND_VALUE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;

/**
 * Tests that RDBStoreAbstractIterator handles concurrent DB close safely.
 */
public class TestRDBStoreIteratorWithDBClose {

  private static final String TABLE_NAME = "TestTable";
  private static final int ENTRY_COUNT = 100;

  private RDBStore rdbStore;
  private ManagedDBOptions options;

  @BeforeEach
  public void setUp(@TempDir File tempDir) throws Exception {
    options = TestRDBStore.newManagedDBOptions();
    Set<TableConfig> configSet = new HashSet<>();
    // RocksDB always requires the default column family to be present
    configSet.add(new TableConfig(
        StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ManagedColumnFamilyOptions()));
    configSet.add(new TableConfig(TABLE_NAME, new ManagedColumnFamilyOptions()));
    rdbStore = TestRDBStore.newRDBStore(tempDir, options, configSet);

    RDBTable table = rdbStore.getTable(TABLE_NAME);
    for (int i = 0; i < ENTRY_COUNT; i++) {
      byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
      byte[] value = ("value-" + i).getBytes(StandardCharsets.UTF_8);
      table.put(key, value);
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (rdbStore != null && !rdbStore.isClosed()) {
      rdbStore.close();
    }
    if (options != null) {
      options.close();
    }
  }

  /**
   * Validates the fast-fail check (isClosed guard).
   */
  @Test
  public void testHasNextReturnsFalseAfterDBClosed() throws Exception {
    RDBTable table = rdbStore.getTable(TABLE_NAME);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch closeStarted = new CountDownLatch(1);
    Future<?> closeFuture;

    try (Table.KeyValueIterator<byte[], byte[]> iter =
        table.iterator((byte[]) null, KEY_AND_VALUE)) {

      assertTrue(iter.hasNext(),
          "Iterator should have entries before DB is closed");

      // Simulate failVolume() from StorageVolumeChecker's thread
      closeFuture = executor.submit(() -> {
        closeStarted.countDown();
        rdbStore.close(); // blocks until iterator releases dbRef
      });

      // Wait for close thread to set isClosed = true
      assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
      Thread.sleep(50);

      // Fast-fail: hasNext() must return false once isClosed = true
      assertFalse(iter.hasNext(),
          "hasNext() must return false immediately after DB is closed");

    } // iter.close() called here → dbRef released → closeFuture unblocks

    closeFuture.get(5, TimeUnit.SECONDS);
    executor.shutdown();
  }

  /**
   * Validates the acquire/release mechanism.
   */
  @Test
  public void testDBPhysicalCloseWaitsForIterator() throws Exception {
    RDBTable table = rdbStore.getTable(TABLE_NAME);
    Table.KeyValueIterator<byte[], byte[]> iter =
        table.iterator((byte[]) null, KEY_AND_VALUE);

    AtomicBoolean dbCloseCompleted = new AtomicBoolean(false);
    CountDownLatch closeStarted = new CountDownLatch(1);

    // Background thread simulates failVolume() → closeDbStore()
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> closeFuture = executor.submit(() -> {
      closeStarted.countDown();
      rdbStore.close();          // blocks in waitAndClose() until counter == 0
      dbCloseCompleted.set(true);
    });

    // Wait for close thread to start and reach waitAndClose()
    assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
    Thread.sleep(50);

    // DB must NOT be physically closed yet — iterator holds dbRef (counter > 0)
    assertFalse(dbCloseCompleted.get(),
        "DB should not be physically closed while iterator is still open");

    // Release the iterator — decrements RocksDatabase.counter to 0
    iter.close();

    // Now waitAndClose() can proceed — DB physically closes
    closeFuture.get(5, TimeUnit.SECONDS);
    assertTrue(dbCloseCompleted.get(),
        "DB should be physically closed after iterator is released");

    executor.shutdown();
  }

  /**
   * Validates the end-to-end race scenario.
   *
   * BackgroundContainerDataScanner iterates while StorageVolumeChecker
   * concurrently triggers failVolume(). The scan must exit cleanly without
   * any exception or native crash — hasNext() returns false once the DB is
   * closed, and the iterator releases its dbRef allowing the DB to close.
   */
  @Test
  public void testConcurrentDBCloseAndScanExitsCleanly() throws Exception {
    RDBTable table = rdbStore.getTable(TABLE_NAME);

    CountDownLatch scanStarted = new CountDownLatch(1);
    AtomicBoolean scanCompleted = new AtomicBoolean(false);

    // Scanner thread — simulates BackgroundContainerDataScanner
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> scanFuture = executor.submit((Callable<Void>) () -> {
      try (Table.KeyValueIterator<byte[], byte[]> iter =
          table.iterator((byte[]) null, KEY_AND_VALUE)) {
        scanStarted.countDown();
        while (iter.hasNext()) {
          iter.next();
          Thread.sleep(1); // slow scan to maximise chance of DB close racing
        }
      }
      scanCompleted.set(true);
      return null;
    });

    // Wait for scan to start, then trigger failVolume() concurrently
    assertTrue(scanStarted.await(5, TimeUnit.SECONDS));
    rdbStore.close(); // simulates failVolume() → closeDbStore()

    assertDoesNotThrow(() -> scanFuture.get(10, TimeUnit.SECONDS),
        "Scan should exit cleanly without throwing when DB is closed concurrently");
    assertTrue(scanCompleted.get(),
        "Scan loop should complete (via hasNext() returning false), not hang");

    executor.shutdown();
  }

  /**
   * Validates that closing an iterator after DB close does not throw.
   */
  @Test
  public void testIteratorCloseAfterDBCloseDoesNotThrow() throws Exception {
    RDBTable table = rdbStore.getTable(TABLE_NAME);
    Table.KeyValueIterator<byte[], byte[]> iter =
        table.iterator((byte[]) null, KEY_AND_VALUE);

    CountDownLatch closeStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    // Simulate failVolume() from StorageVolumeChecker's thread
    Future<?> closeFuture = executor.submit(() -> {
      closeStarted.countDown();
      rdbStore.close(); // blocks until iter.close() releases dbRef
    });

    // Wait for close thread to mark isClosed = true
    assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
    Thread.sleep(50);

    // iter.close() is called after DB is marked closed — must not throw
    assertDoesNotThrow(iter::close,
        "Iterator close() after DB close must not throw");

    // iter.close() decremented counter to 0 — close thread can now finish
    closeFuture.get(5, TimeUnit.SECONDS);
    executor.shutdown();
  }
}
