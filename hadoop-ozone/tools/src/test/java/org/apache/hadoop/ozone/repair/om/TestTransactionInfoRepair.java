/*
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
package org.apache.hadoop.ozone.repair.om;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TRANSACTION_INFO_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/**
 * Tests TransactionInfoRepair.
 */
public class TestTransactionInfoRepair {


  private static final String DB_PATH = "testDBPath";
  private static final long TEST_TERM = 1;
  private static final long TEST_INDEX = 1;
  private GenericTestUtils.PrintStreamCapturer out;
  private GenericTestUtils.PrintStreamCapturer err;

  @BeforeEach
  void setup() {
    out = GenericTestUtils.captureOut();
    err = GenericTestUtils.captureErr();
  }

  @AfterEach
  void cleanup() {
    IOUtils.closeQuietly(out, err);
  }

  @Test
  public void testUpdateTransactionInfoTableSuccessful() {
    ManagedRocksDB mdb = mockRockDB();
    testCommand(mdb, mock(ColumnFamilyHandle.class));

    assertThat(out.getOutput())
        .contains(
            String.format("The original highest transaction Info was (t:%s, i:%s)", TEST_TERM, TEST_INDEX),
            String.format("The highest transaction info has been updated to: (t:%s, i:%s)", TEST_TERM, TEST_INDEX)
        );
  }

  @Test
  public void testCommandWhenTableNotInDBForGivenPath() {
    ManagedRocksDB mdb = mockRockDB();
    testCommand(mdb, null);
    assertThat(err.getOutput())
        .contains(TRANSACTION_INFO_TABLE + " is not in a column family in DB for the given path");
  }

  @Test
  public void testCommandWhenFailToUpdateRocksDBForGivenPath() throws Exception {
    ManagedRocksDB mdb = mockRockDB();
    RocksDB rdb = mdb.get();

    ColumnFamilyHandle mock = mock(ColumnFamilyHandle.class);
    doThrow(RocksDBException.class).when(rdb)
        .put(eq(mock), any(byte[].class), any(byte[].class));

    testCommand(mdb, mock);

    assertThat(err.getOutput())
        .contains("Failed to update RocksDB.");
  }


  private void testCommand(ManagedRocksDB mdb, ColumnFamilyHandle columnFamilyHandle) {
    try (MockedStatic<ManagedRocksDB> mocked = mockStatic(ManagedRocksDB.class);
         MockedStatic<RocksDBUtils> mockUtil = mockStatic(RocksDBUtils.class)) {
      mocked.when(() -> ManagedRocksDB.open(anyString(), anyList(), anyList())).thenReturn(mdb);
      mockUtil.when(() -> RocksDBUtils.getColumnFamilyHandle(anyString(), anyList()))
          .thenReturn(columnFamilyHandle);

      mockUtil.when(() -> RocksDBUtils.getValue(eq(mdb), eq(columnFamilyHandle), eq(TRANSACTION_INFO_KEY),
              eq(TransactionInfo.getCodec())))
          .thenReturn(mock(TransactionInfo.class));

      mockUtil.when(() -> RocksDBUtils.getValue(eq(mdb), eq(columnFamilyHandle), eq(TRANSACTION_INFO_KEY),
              eq(TransactionInfo.getCodec())))
          .thenReturn(mock(TransactionInfo.class));

      TransactionInfo transactionInfo2 = TransactionInfo.valueOf(TermIndex.valueOf(TEST_TERM, TEST_INDEX));
      mockUtil.when(() -> RocksDBUtils.getValue(eq(mdb), eq(columnFamilyHandle), eq(TRANSACTION_INFO_KEY),
              eq(TransactionInfo.getCodec())))
          .thenReturn(transactionInfo2);

      CommandLine cli = new OzoneRepair().getCmd();
      cli.execute(
          "om",
          "update-transaction",
          "--db", DB_PATH,
          "--term", String.valueOf(TEST_TERM),
          "--index", String.valueOf(TEST_INDEX));
    }
  }

  private ManagedRocksDB mockRockDB() {
    ManagedRocksDB db = mock(ManagedRocksDB.class);
    RocksDB rocksDB = mock(RocksDB.class);
    doReturn(rocksDB).when(db).get();
    return db;
  }

}
