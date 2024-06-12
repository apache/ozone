/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.repair;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.function.Supplier;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TRANSACTION_INFO_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests TransactionInfoRepair.
 */
public class TestTransactionInfoRepair {


  private static final String DB_PATH = "testDBPath";
  private static final long TEST_TERM = 1;
  private static final long TEST_INDEX = 1;

  @Test
  public void testUpdateTransactionInfoTableSuccessful() throws Exception {
    ManagedRocksDB mdb = mockRockDB();
    try (GenericTestUtils.SystemOutCapturer outCapturer = new GenericTestUtils.SystemOutCapturer()) {
      testCommand(mdb, mock(ColumnFamilyHandle.class), () -> {
        try {
          return outCapturer.getOutput();
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
      }, new String[]{String.format("The original highest transaction Info was (t:%s, i:%s)",
          TEST_TERM, TEST_INDEX),
              String.format("The highest transaction info has been updated to: (t:%s, i:%s)",
                  TEST_TERM, TEST_INDEX)});
    }
  }

  @Test
  public void testCommandWhenTableNotInDBForGivenPath() throws Exception {
    ManagedRocksDB mdb = mockRockDB();
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> testCommand(mdb, null, null, new String[]{""}));
    assertThat(exception.getMessage()).contains(TRANSACTION_INFO_TABLE +
        " is not in a column family in DB for the given path");
  }

  @Test
  public void testCommandWhenFailToUpdateRocksDBForGivenPath() throws Exception {
    ManagedRocksDB mdb = mockRockDB();
    RocksDB rdb = mdb.get();

    doThrow(RocksDBException.class).when(rdb)
        .put(any(ColumnFamilyHandle.class), any(byte[].class), any(byte[].class));

    IOException exception = assertThrows(IOException.class,
        () -> testCommand(mdb, mock(ColumnFamilyHandle.class), null, new String[]{""}));
    assertThat(exception.getMessage()).contains("Failed to update RocksDB.");
    assertThat(exception.getCause()).isInstanceOf(RocksDBException.class);
  }


  private void testCommand(ManagedRocksDB mdb, ColumnFamilyHandle columnFamilyHandle, Supplier<String> capturer,
                           String[] messages) throws Exception {
    try (MockedStatic<ManagedRocksDB> mocked = mockStatic(ManagedRocksDB.class);
         MockedStatic<RocksDBUtils> mockUtil = mockStatic(RocksDBUtils.class)) {
      mocked.when(() -> ManagedRocksDB.open(anyString(), anyList(), anyList())).thenReturn(mdb);
      mockUtil.when(() -> RocksDBUtils.getColumnFamilyHandle(anyString(), anyList()))
          .thenReturn(columnFamilyHandle);
      mockUtil.when(() ->
          RocksDBUtils.getValue(any(ManagedRocksDB.class), any(ColumnFamilyHandle.class), anyString(),
              any(Codec.class))).thenReturn(mock(TransactionInfo.class));

      mockTransactionInfo(mockUtil);

      TransactionInfoRepair cmd = spy(TransactionInfoRepair.class);
      RDBRepair rdbRepair = mock(RDBRepair.class);
      when(rdbRepair.getDbPath()).thenReturn(DB_PATH);
      when(cmd.getParent()).thenReturn(rdbRepair);
      cmd.setHighestTransactionTerm(TEST_TERM);
      cmd.setHighestTransactionIndex(TEST_INDEX);

      cmd.call();
      for (String message : messages) {
        assertThat(capturer.get()).contains(message);
      }
    }
  }

  private void mockTransactionInfo(MockedStatic<RocksDBUtils> mockUtil) {
    mockUtil.when(() ->
        RocksDBUtils.getValue(any(ManagedRocksDB.class), any(ColumnFamilyHandle.class), anyString(),
            any(Codec.class))).thenReturn(mock(TransactionInfo.class));

    TransactionInfo transactionInfo2 = mock(TransactionInfo.class);
    doReturn(TermIndex.valueOf(TEST_TERM, TEST_INDEX)).when(transactionInfo2).getTermIndex();
    mockUtil.when(() ->
        RocksDBUtils.getValue(any(ManagedRocksDB.class), any(ColumnFamilyHandle.class), anyString(),
            any(Codec.class))).thenReturn(transactionInfo2);
  }

  private ManagedRocksDB mockRockDB() {
    ManagedRocksDB db = mock(ManagedRocksDB.class);
    RocksDB rocksDB = mock(RocksDB.class);
    doReturn(rocksDB).when(db).get();
    return db;
  }

}
