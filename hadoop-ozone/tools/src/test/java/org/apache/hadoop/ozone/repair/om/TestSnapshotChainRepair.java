/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.repair.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.apache.ozone.test.GenericTestUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.ColumnFamilyDescriptor;

import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.apache.ozone.test.IntLambda.withTextFromSystemIn;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_INFO_TABLE;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests SnapshotChainRepair.
 */
public class TestSnapshotChainRepair {

  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";

  private ManagedRocksDB managedRocksDB;
  private RocksDB rocksDB;
  private ColumnFamilyHandle columnFamilyHandle;

  private static final String DB_PATH = "testDBPath";

  private MockedStatic<ManagedRocksDB> mockedDB;
  private MockedStatic<RocksDBUtils> mockedUtils;

  private GenericTestUtils.PrintStreamCapturer out;
  private GenericTestUtils.PrintStreamCapturer err;

  @BeforeEach
  public void setup() throws Exception {
    out = GenericTestUtils.captureOut();
    err = GenericTestUtils.captureErr();

    // Initialize static mocks
    mockedDB = mockStatic(ManagedRocksDB.class);
    mockedUtils = mockStatic(RocksDBUtils.class);
  }

  @AfterEach
  public void tearDown() {
    IOUtils.closeQuietly(out, err, mockedDB, mockedUtils);
  }

  private void setupMockDB(SnapshotInfo snapshotInfo,
      List<SnapshotInfo> iteratorSnapshots) throws Exception {

    managedRocksDB = mock(ManagedRocksDB.class);
    rocksDB = mock(RocksDB.class);
    columnFamilyHandle = mock(ColumnFamilyHandle.class);

    when(managedRocksDB.get()).thenReturn(rocksDB);

    // Mock column family descriptors
    List<ColumnFamilyDescriptor> cfDescList = new ArrayList<>();
    cfDescList.add(new ColumnFamilyDescriptor(new byte[] {1}));

    mockedUtils.when(() -> RocksDBUtils.getColumnFamilyDescriptors(eq(DB_PATH)))
        .thenReturn(cfDescList);

    // Mock DB open
    mockedDB.when(() -> ManagedRocksDB.open(eq(DB_PATH), eq(cfDescList), eq(new ArrayList<>())))
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
    if (iteratorSnapshots.isEmpty()) {
      when(rocksIterator.isValid()).thenReturn(false);
    } else {
      Boolean[] remainingValidResponses = new Boolean[iteratorSnapshots.size()];
      for (int i = 0; i < iteratorSnapshots.size() - 1; i++) {
        remainingValidResponses[i] = true;
      }
      remainingValidResponses[iteratorSnapshots.size() - 1] = false;

      when(rocksIterator.isValid())
          .thenReturn(true, remainingValidResponses);

      ArrayList<byte[]> valueResponses = new ArrayList<>();
      for (SnapshotInfo snap : iteratorSnapshots) {
        try {
          valueResponses.add(SnapshotInfo.getCodec().toPersistedFormat(snap));
        } catch (IOException e) {
          Assertions.fail("Failed to serialize snapshot info");
        }
      }
      byte[] firstValue = valueResponses.get(0);
      byte[][] remainingValueResponses = valueResponses.subList(1, valueResponses.size()).toArray(new byte[0][]);
      when(rocksIterator.value())
          .thenReturn(firstValue, remainingValueResponses);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSuccessfulRepair(boolean dryRun) throws Exception {
    UUID globalPrevSnapshotId = UUID.randomUUID();
    UUID pathPrevSnapshotId = UUID.randomUUID();

    SnapshotInfo snapshotInfo = newSnapshot();
    SnapshotInfo globalPrevSnapshot = newSnapshot(globalPrevSnapshotId);
    SnapshotInfo pathPrevSnapshot = newSnapshot(pathPrevSnapshotId);

    List<SnapshotInfo> iteratorSnapshots = Arrays.asList(
        snapshotInfo, globalPrevSnapshot, pathPrevSnapshot);

    List<String> argsList = new ArrayList<>(Arrays.asList(
        "om", "snapshot", "chain",
        VOLUME_NAME + "/" + BUCKET_NAME,
        snapshotInfo.getName(),
        "--db", DB_PATH,
        "--global-previous", globalPrevSnapshotId.toString(),
        "--path-previous", pathPrevSnapshotId.toString()));

    if (dryRun) {
      argsList.add("--dry-run");
    }

    setupMockDB(snapshotInfo, iteratorSnapshots);

    CommandLine cli = new OzoneRepair().getCmd();
    withTextFromSystemIn("y")
        .execute(() -> cli.execute(argsList.toArray(new String[0])));

    String output = out.getOutput();
    assertTrue(output.contains("Updating SnapshotInfo to"));

    if (dryRun) {
      // Verify DB update was NOT called in dry run mode
      verify(rocksDB, never()).put(
          eq(columnFamilyHandle),
          eq(StringCodec.get().toPersistedFormat(snapshotInfo.getTableKey())),
          eq(SnapshotInfo.getCodec().toPersistedFormat(snapshotInfo)));
    } else {
      // Verify DB update was called with correct parameters
      verify(rocksDB).put(
          eq(columnFamilyHandle),
          eq(StringCodec.get().toPersistedFormat(snapshotInfo.getTableKey())),
          eq(SnapshotInfo.getCodec().toPersistedFormat(snapshotInfo)));
      assertTrue(output.contains("Snapshot Info is updated"));
    }
  }

  @Test
  public void testGlobalPreviousMatchesSnapshotId() throws Exception {
    UUID snapshotId = UUID.randomUUID();
    // Use same ID for global previous to trigger error
    UUID globalPrevSnapshotId = snapshotId;
    UUID pathPrevSnapshotId = UUID.randomUUID();

    SnapshotInfo snapshotInfo = newSnapshot(snapshotId);
    SnapshotInfo pathPrevSnapshot = newSnapshot(pathPrevSnapshotId);

    List<SnapshotInfo> iteratorSnapshots = Arrays.asList(
        snapshotInfo, pathPrevSnapshot);

    String[] args = new String[] {
        "om", "snapshot", "chain",
        VOLUME_NAME + "/" + BUCKET_NAME,
        snapshotInfo.getName(),
        "--db", DB_PATH,
        "--global-previous", globalPrevSnapshotId.toString(),
        "--path-previous", pathPrevSnapshotId.toString(),
    };

    setupMockDB(snapshotInfo, iteratorSnapshots);

    CommandLine cli = new OzoneRepair().getCmd();
    withTextFromSystemIn("y")
        .execute(() -> cli.execute(args));

    String errorOutput = err.getOutput();
    assertTrue(errorOutput.contains("globalPreviousSnapshotId: '" + globalPrevSnapshotId +
        "' is equal to given snapshot's ID"));
  }

  @Test
  public void testPathPreviousMatchesSnapshotId() throws Exception {
    UUID snapshotId = UUID.randomUUID();
    UUID globalPrevSnapshotId = UUID.randomUUID();
    // Use same ID for path previous to trigger error
    UUID pathPrevSnapshotId = snapshotId;

    SnapshotInfo snapshotInfo = newSnapshot(snapshotId);
    SnapshotInfo globalPrevSnapshot = newSnapshot(globalPrevSnapshotId);

    List<SnapshotInfo> iteratorSnapshots = Arrays.asList(
        snapshotInfo, globalPrevSnapshot);

    String[] args = new String[] {
        "om", "snapshot", "chain",
        VOLUME_NAME + "/" + BUCKET_NAME,
        snapshotInfo.getName(),
        "--db", DB_PATH,
        "--global-previous", globalPrevSnapshotId.toString(),
        "--path-previous", pathPrevSnapshotId.toString(),
    };

    setupMockDB(snapshotInfo, iteratorSnapshots);

    CommandLine cli = new OzoneRepair().getCmd();
    withTextFromSystemIn("y")
        .execute(() -> cli.execute(args));

    String errorOutput = err.getOutput();
    assertTrue(errorOutput.contains("pathPreviousSnapshotId: '" + pathPrevSnapshotId +
        "' is equal to given snapshot's ID"));
  }

  @Test
  public void testGlobalPreviousDoesNotExist() throws Exception {
    UUID globalPrevSnapshotId = UUID.randomUUID();
    UUID pathPrevSnapshotId = UUID.randomUUID();

    SnapshotInfo snapshotInfo = newSnapshot();
    SnapshotInfo pathPrevSnapshot = newSnapshot(pathPrevSnapshotId);

    List<SnapshotInfo> iteratorSnapshots = Arrays.asList(
        snapshotInfo, pathPrevSnapshot);

    String[] args = new String[] {
        "om", "snapshot", "chain",
        VOLUME_NAME + "/" + BUCKET_NAME,
        snapshotInfo.getName(),
        "--db", DB_PATH,
        "--global-previous", globalPrevSnapshotId.toString(),
        "--path-previous", pathPrevSnapshotId.toString(),
    };

    setupMockDB(snapshotInfo, iteratorSnapshots);

    CommandLine cli = new OzoneRepair().getCmd();
    withTextFromSystemIn("y")
        .execute(() -> cli.execute(args));

    String errorOutput = err.getOutput();
    assertTrue(errorOutput.contains("globalPreviousSnapshotId: '" + globalPrevSnapshotId +
        "' does not exist in snapshotInfoTable"));
  }

  @Test
  public void testPathPreviousDoesNotExist() throws Exception {
    UUID globalPrevSnapshotId = UUID.randomUUID();
    UUID pathPrevSnapshotId = UUID.randomUUID();

    SnapshotInfo snapshotInfo = newSnapshot();
    SnapshotInfo globalPrevSnapshot = newSnapshot(globalPrevSnapshotId);

    List<SnapshotInfo> iteratorSnapshots = Arrays.asList(
        snapshotInfo, globalPrevSnapshot);

    String[] args = new String[] {
        "om", "snapshot", "chain",
        VOLUME_NAME + "/" + BUCKET_NAME,
        snapshotInfo.getName(),
        "--db", DB_PATH,
        "--global-previous", globalPrevSnapshotId.toString(),
        "--path-previous", pathPrevSnapshotId.toString(),
    };

    setupMockDB(snapshotInfo, iteratorSnapshots);

    CommandLine cli = new OzoneRepair().getCmd();
    withTextFromSystemIn("y")
        .execute(() -> cli.execute(args));

    String errorOutput = err.getOutput();
    assertTrue(errorOutput.contains("pathPreviousSnapshotId: '" + pathPrevSnapshotId +
        "' does not exist in snapshotInfoTable"));
  }

  private static SnapshotInfo newSnapshot() {
    return newSnapshot(UUID.randomUUID());
  }

  private static SnapshotInfo newSnapshot(UUID snapshotId) {
    String name = RandomStringUtils.insecure().nextAlphanumeric(10);
    return SnapshotInfo.newInstance(VOLUME_NAME, BUCKET_NAME, name, snapshotId, 0);
  }
}
