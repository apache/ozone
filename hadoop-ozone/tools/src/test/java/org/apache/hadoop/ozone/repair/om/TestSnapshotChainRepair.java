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

package org.apache.hadoop.ozone.repair.om;

import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_INFO_TABLE;
import static org.apache.ozone.test.IntLambda.withTextFromSystemIn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedConfigOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.OptionsUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import picocli.CommandLine;

/**
 * Tests SnapshotChainRepair.
 */
public class TestSnapshotChainRepair {

  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";
  private static final String DB_PATH = "testDBPath";

  private ManagedRocksDB managedRocksDB;
  private RocksDB rocksDB;
  private ColumnFamilyHandle columnFamilyHandle;

  private MockedStatic<ManagedRocksDB> mockedDB;
  private MockedStatic<RocksDBUtils> mockedUtils;
  private MockedStatic<OptionsUtil> mockedOptionsUtil;

  private GenericTestUtils.PrintStreamCapturer out;
  private GenericTestUtils.PrintStreamCapturer err;

  @BeforeEach
  public void setup() throws Exception {
    out = GenericTestUtils.captureOut();
    err = GenericTestUtils.captureErr();

    // Initialize static mocks
    mockedDB = mockStatic(ManagedRocksDB.class);
    mockedUtils = mockStatic(RocksDBUtils.class);
    mockedOptionsUtil = mockStatic(OptionsUtil.class);
  }

  @AfterEach
  public void tearDown() {
    IOUtils.closeQuietly(out, err, mockedDB, mockedUtils, mockedOptionsUtil);
  }

  private void setupMockDB(SnapshotInfo snapshotInfo,
      SnapshotInfo... snapshots) throws Exception {

    managedRocksDB = mock(ManagedRocksDB.class);
    rocksDB = mock(RocksDB.class);
    columnFamilyHandle = mock(ColumnFamilyHandle.class);

    when(managedRocksDB.get()).thenReturn(rocksDB);

    // Mock DB open
    mockedDB.when(() -> ManagedRocksDB.openWithLatestOptions(any(ManagedConfigOptions.class),
            any(DBOptions.class), eq(DB_PATH), eq(new ArrayList<>()), eq(new ArrayList<>())))
        .thenReturn(managedRocksDB);

    // Mock column family handle
    mockedUtils.when(() -> RocksDBUtils.getColumnFamilyHandle(
        eq(SNAPSHOT_INFO_TABLE), anyList()))
        .thenReturn(columnFamilyHandle);

    // Mock snapshot retrieval
    mockedUtils.when(() -> RocksDBUtils.getValue(
        eq(managedRocksDB),
        eq(columnFamilyHandle),
        anyString(),
        eq(SnapshotInfo.getCodec())))
        .thenReturn(snapshotInfo);

    // Mock iterator
    RocksIterator rocksIterator = mock(RocksIterator.class);
    when(rocksDB.newIterator(columnFamilyHandle)).thenReturn(rocksIterator);

    // Setup iterator behavior based on provided snapshots
    if (snapshots.length == 0) {
      when(rocksIterator.isValid()).thenReturn(false);
    } else {
      Boolean[] remainingValidResponses = new Boolean[snapshots.length + 1];
      Arrays.fill(remainingValidResponses, true);
      remainingValidResponses[remainingValidResponses.length - 1] = false;

      when(rocksIterator.isValid())
          .thenReturn(true, remainingValidResponses);

      ArrayList<byte[]> valueResponses = new ArrayList<>();
      for (SnapshotInfo snap : snapshots) {
        valueResponses.add(SnapshotInfo.getCodec().toPersistedFormat(snap));
      }
      byte[] firstValue = SnapshotInfo.getCodec().toPersistedFormat(snapshotInfo);
      byte[][] remainingValueResponses = valueResponses.toArray(new byte[0][]);
      when(rocksIterator.value())
          .thenReturn(firstValue, remainingValueResponses);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSuccessfulRepair(boolean dryRun) throws Exception {
    SnapshotInfo snapshotInfo = newSnapshot();
    SnapshotInfo globalPrevSnapshot = newSnapshot();
    SnapshotInfo pathPrevSnapshot = newSnapshot();

    List<String> argsList = new ArrayList<>(Arrays.asList(
        "om", "snapshot", "chain",
        VOLUME_NAME + "/" + BUCKET_NAME,
        snapshotInfo.getName(),
        "--db", DB_PATH,
        "--global-previous", globalPrevSnapshot.getSnapshotId().toString(),
        "--path-previous", pathPrevSnapshot.getSnapshotId().toString()));

    if (dryRun) {
      argsList.add("--dry-run");
    }

    setupMockDB(snapshotInfo, globalPrevSnapshot, pathPrevSnapshot);

    CommandLine cli = new OzoneRepair().getCmd();
    withTextFromSystemIn("y")
        .execute(() -> cli.execute(argsList.toArray(new String[0])));

    assertThat(out.getOutput()).contains("Updating SnapshotInfo to");

    verifyDbWrite(snapshotInfo, !dryRun);
  }

  @Test
  public void testGlobalPreviousMatchesSnapshotId() throws Exception {
    SnapshotInfo snapshotInfo = newSnapshot();
    SnapshotInfo pathPrevSnapshot = newSnapshot();

    String[] args = new String[] {
        "om", "snapshot", "chain",
        VOLUME_NAME + "/" + BUCKET_NAME,
        snapshotInfo.getName(),
        "--db", DB_PATH,
        // Use same ID for global previous to trigger error
        "--global-previous", snapshotInfo.getSnapshotId().toString(),
        "--path-previous", pathPrevSnapshot.getSnapshotId().toString(),
    };

    setupMockDB(snapshotInfo, pathPrevSnapshot);

    CommandLine cli = new OzoneRepair().getCmd();
    withTextFromSystemIn("y")
        .execute(() -> cli.execute(args));

    assertThat(err.getOutput()).contains("globalPreviousSnapshotId: '" + snapshotInfo.getSnapshotId() +
        "' is equal to given snapshot's ID");
    verifyDbWrite(snapshotInfo, false);
  }

  @Test
  public void testPathPreviousMatchesSnapshotId() throws Exception {
    SnapshotInfo snapshotInfo = newSnapshot();
    SnapshotInfo globalPrevSnapshot = newSnapshot();

    String[] args = new String[] {
        "om", "snapshot", "chain",
        VOLUME_NAME + "/" + BUCKET_NAME,
        snapshotInfo.getName(),
        "--db", DB_PATH,
        "--global-previous", globalPrevSnapshot.getSnapshotId().toString(),
        // Use same ID for path previous to trigger error
        "--path-previous", snapshotInfo.getSnapshotId().toString(),
    };

    setupMockDB(snapshotInfo, globalPrevSnapshot);

    CommandLine cli = new OzoneRepair().getCmd();
    withTextFromSystemIn("y")
        .execute(() -> cli.execute(args));

    assertThat(err.getOutput()).contains("pathPreviousSnapshotId: '" + snapshotInfo.getSnapshotId() +
        "' is equal to given snapshot's ID");
    verifyDbWrite(snapshotInfo, false);
  }

  @Test
  public void testGlobalPreviousDoesNotExist() throws Exception {
    String nonexistent = UUID.randomUUID().toString();

    SnapshotInfo snapshotInfo = newSnapshot();
    SnapshotInfo pathPrevSnapshot = newSnapshot();

    String[] args = new String[] {
        "om", "snapshot", "chain",
        VOLUME_NAME + "/" + BUCKET_NAME,
        snapshotInfo.getName(),
        "--db", DB_PATH,
        "--global-previous", nonexistent,
        "--path-previous", pathPrevSnapshot.getSnapshotId().toString(),
    };

    setupMockDB(snapshotInfo, pathPrevSnapshot);

    CommandLine cli = new OzoneRepair().getCmd();
    withTextFromSystemIn("y")
        .execute(() -> cli.execute(args));

    assertThat(err.getOutput()).contains("globalPreviousSnapshotId: '" + nonexistent +
        "' does not exist in snapshotInfoTable");
    verifyDbWrite(snapshotInfo, false);
  }

  @Test
  public void testPathPreviousDoesNotExist() throws Exception {
    String nonexistent = UUID.randomUUID().toString();

    SnapshotInfo snapshotInfo = newSnapshot();
    SnapshotInfo globalPrevSnapshot = newSnapshot();

    String[] args = new String[] {
        "om", "snapshot", "chain",
        VOLUME_NAME + "/" + BUCKET_NAME,
        snapshotInfo.getName(),
        "--db", DB_PATH,
        "--global-previous", globalPrevSnapshot.getSnapshotId().toString(),
        "--path-previous", nonexistent,
    };

    setupMockDB(snapshotInfo, globalPrevSnapshot);

    CommandLine cli = new OzoneRepair().getCmd();
    withTextFromSystemIn("y")
        .execute(() -> cli.execute(args));

    assertThat(err.getOutput()).contains("pathPreviousSnapshotId: '" + nonexistent +
        "' does not exist in snapshotInfoTable");
    verifyDbWrite(snapshotInfo, false);
  }

  private static SnapshotInfo newSnapshot() {
    String name = RandomStringUtils.insecure().nextAlphanumeric(10);
    return SnapshotInfo.newInstance(VOLUME_NAME, BUCKET_NAME, name, UUID.randomUUID(), 0);
  }

  private void verifyDbWrite(SnapshotInfo snapshotInfo, boolean updateExpected) throws RocksDBException, IOException {
    verify(rocksDB, times(updateExpected ? 1 : 0)).put(
        eq(columnFamilyHandle),
        eq(StringCodec.get().toPersistedFormat(snapshotInfo.getTableKey())),
        eq(SnapshotInfo.getCodec().toPersistedFormat(snapshotInfo)));
    if (updateExpected) {
      assertThat(out.get()).contains("Snapshot Info is updated");
    }
  }
}
