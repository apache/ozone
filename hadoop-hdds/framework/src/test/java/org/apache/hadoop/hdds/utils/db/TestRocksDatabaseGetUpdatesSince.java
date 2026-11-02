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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedTransactionLogIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

/**
 * Tests that {@link RocksDatabase#getUpdatesSince(long)} does not close the DB
 * when a WAL file vanishes while RocksDB lists or opens the WALs (HDDS-8271).
 */
public class TestRocksDatabaseGetUpdatesSince {

  private static final String ARCHIVED_WAL_VANISHED =
      "while stat a file for size: /data/om/db/om.db/archive/000123.log: No such file or directory";
  private static final String LIVE_WAL_VANISHED =
      "while stat a file for size: /data/om/db/om.db/000123.log: No such file or directory";
  private static final String ARCHIVED_WAL_VANISHED_ON_OPEN =
      "While opening a file for sequentially reading: /data/om/db/om.db/archive/000123.log: No such file or directory";

  private TableConfig defaultTable;
  private RocksDatabase db;
  private final CountDownLatch dbClosed = new CountDownLatch(1);

  @BeforeEach
  public void setUp(@TempDir File tempDir) throws Exception {
    defaultTable = new TableConfig(StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ManagedColumnFamilyOptions());
    ManagedDBOptions options = spy(TestRDBStore.newManagedDBOptions());
    doAnswer(invocation -> {
      invocation.callRealMethod();
      // DB options are the last resource closed by RocksDatabase.
      dbClosed.countDown();
      return null;
    }).when(options).close();
    db = spy(RocksDatabase.open(tempDir, options, new ManagedWriteOptions(),
        Collections.singleton(defaultTable), false));
  }

  @AfterEach
  public void tearDown() throws Exception {
    db.close();
    assertTrue(dbClosed.await(10, TimeUnit.SECONDS), "Database cleanup must finish before closing table options");
    defaultTable.close();
  }

  static Stream<RocksDBException> vanishedWalErrors() {
    return Stream.of(ARCHIVED_WAL_VANISHED, LIVE_WAL_VANISHED, ARCHIVED_WAL_VANISHED_ON_OPEN)
        .map(TestRocksDatabaseGetUpdatesSince::ioError);
  }

  static Stream<RocksDBException> otherErrors() {
    return Stream.concat(
        Stream.of(
            "while stat a file for size: /data/om/db/om.db/000123.log: Input/output error",
            "While appending to file: /data/om/db/om.db/000123.log: No space left on device",
            "while stat a file for size: /data/om/db/om.db/000123.sst: No such file or directory",
            "While opening a file for sequentially reading: /data/om/db/om.db/MANIFEST-000001: "
                + "No such file or directory",
            "While opening a file for sequentially reading: /data/om/db/om.db/archive/000123.log: Permission denied")
            .map(TestRocksDatabaseGetUpdatesSince::ioError),
        Stream.of(ARCHIVED_WAL_VANISHED, LIVE_WAL_VANISHED, ARCHIVED_WAL_VANISHED_ON_OPEN)
            .map(message -> rocksDBException(Status.Code.Corruption, message)));
  }

  @ParameterizedTest
  @MethodSource("vanishedWalErrors")
  public void classifiesVanishedWal(RocksDBException e) {
    assertTrue(RocksDatabase.isVanishedWalFile(e));
  }

  @ParameterizedTest
  @MethodSource("otherErrors")
  public void doesNotClassifyOtherErrors(RocksDBException e) {
    assertFalse(RocksDatabase.isVanishedWalFile(e));
  }

  @Test
  public void doesNotClassifyMissingStatus() {
    assertFalse(RocksDatabase.isVanishedWalFile(new RocksDBException(ARCHIVED_WAL_VANISHED)));
  }

  @ParameterizedTest
  @MethodSource("vanishedWalErrors")
  public void retriesOnceAndKeepsDbOpenWhenWalVanishes(RocksDBException e) throws Exception {
    doThrow(e).doCallRealMethod().when(db).getUpdatesSinceFromRocksDB(anyLong());

    try (ManagedTransactionLogIterator iterator = db.getUpdatesSince(0)) {
      assertNotNull(iterator);
    }
    assertFalse(db.isClosed());
    verify(db, times(2)).getUpdatesSinceFromRocksDB(0);
  }

  @ParameterizedTest
  @MethodSource("vanishedWalErrors")
  public void keepsDbOpenWhenRetryFailsTheSameWay(RocksDBException e) throws Exception {
    doThrow(e).when(db).getUpdatesSinceFromRocksDB(anyLong());

    assertThrows(RocksDatabaseException.class, () -> db.getUpdatesSince(0));
    assertFalse(db.isClosed());
    verify(db, times(2)).getUpdatesSinceFromRocksDB(0);
  }

  @ParameterizedTest
  @MethodSource("otherErrors")
  public void closesDbWithoutRetryOnOtherErrors(RocksDBException e) throws Exception {
    doThrow(e).when(db).getUpdatesSinceFromRocksDB(anyLong());

    assertThrows(RocksDatabaseException.class, () -> db.getUpdatesSince(0));
    assertTrue(db.isClosed());
    verify(db, times(1)).getUpdatesSinceFromRocksDB(0);
  }

  @Test
  public void doesNotRetryWhenRocksDBSucceeds() throws Exception {
    doCallRealMethod().when(db).getUpdatesSinceFromRocksDB(anyLong());

    try (ManagedTransactionLogIterator iterator = db.getUpdatesSince(0)) {
      assertNotNull(iterator);
    }
    assertFalse(db.isClosed());
    verify(db, times(1)).getUpdatesSinceFromRocksDB(0);
  }

  private static RocksDBException ioError(String message) {
    return rocksDBException(Status.Code.IOError, message);
  }

  private static RocksDBException rocksDBException(Status.Code code, String message) {
    return new RocksDBException(message, new Status(code, Status.SubCode.None, message));
  }
}
