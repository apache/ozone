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

package org.apache.hadoop.ozone.repair;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.ozone.test.IntLambda.withTextFromSystemIn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.managed.ManagedConfigOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.OptionsUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

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

  @ParameterizedTest
  @ValueSource(strings = {"om", "scm"})
  public void testUpdateTransactionInfoTableSuccessful(String component) {
    ManagedRocksDB mdb = mockRockDB();
    testCommand(component, mdb, mock(ColumnFamilyHandle.class));

    assertThat(out.getOutput())
        .contains(
            String.format("The original highest transaction Info was (t:%s, i:%s)", TEST_TERM, TEST_INDEX),
            String.format("The highest transaction info has been updated to: (t:%s, i:%s)", TEST_TERM, TEST_INDEX)
        );
  }

  @ParameterizedTest
  @ValueSource(strings = {"om", "scm"})
  public void testCommandWhenTableNotInDBForGivenPath(String component) {
    ManagedRocksDB mdb = mockRockDB();
    testCommand(component, mdb, null);
    assertThat(err.getOutput())
        .contains(getColumnFamilyName(component) + " is not in a column family in DB for the given path");
  }

  @ParameterizedTest
  @ValueSource(strings = {"om", "scm"})
  public void testCommandWhenFailToUpdateRocksDBForGivenPath(String component) throws Exception {
    ManagedRocksDB mdb = mockRockDB();
    RocksDB rdb = mdb.get();

    ColumnFamilyHandle mock = mock(ColumnFamilyHandle.class);
    doThrow(RocksDBException.class).when(rdb)
        .put(eq(mock), any(byte[].class), any(byte[].class));

    testCommand(component, mdb, mock);

    assertThat(err.getOutput())
        .contains("Failed to update RocksDB.");
  }

  private void testCommand(String component, ManagedRocksDB mdb, ColumnFamilyHandle columnFamilyHandle) {
    final String expectedColumnFamilyName = getColumnFamilyName(component);
    try (MockedStatic<ManagedRocksDB> mocked = mockStatic(ManagedRocksDB.class);
         MockedStatic<RocksDBUtils> mockUtil = mockStatic(RocksDBUtils.class);
         MockedStatic<OptionsUtil> mockOptionsUtil = mockStatic(OptionsUtil.class)) {
      mocked.when(() -> ManagedRocksDB.openWithLatestOptions(any(ManagedConfigOptions.class), any(DBOptions.class),
          anyString(), anyList(), anyList())).thenReturn(mdb);
      mockUtil.when(() -> RocksDBUtils.getColumnFamilyHandle(eq(expectedColumnFamilyName), anyList()))
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
      withTextFromSystemIn("y")
          .execute(() -> cli.execute(
              component,
              "update-transaction",
              "--db", DB_PATH,
              "--term", String.valueOf(TEST_TERM),
              "--index", String.valueOf(TEST_INDEX)
          ));
    }
  }

  private String getColumnFamilyName(String component) {
    switch (component) {
    case "om": return OMDBDefinition.TRANSACTION_INFO_TABLE_DEF.getName();
    case "scm": return SCMDBDefinition.TRANSACTIONINFO.getName();
    default: return "";
    }
  }

  private ManagedRocksDB mockRockDB() {
    ManagedRocksDB db = mock(ManagedRocksDB.class);
    RocksDB rocksDB = mock(RocksDB.class);
    doReturn(rocksDB).when(db).get();
    return db;
  }

}
