/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.repair.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.ColumnFamilyDescriptor;

import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.ozone.test.IntLambda.withTextFromSystemIn;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_INFO_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSnapshotChainRepair {

  private SnapshotChainRepair repair;
  private OMMetadataManager omMetadataManager;
  private ByteArrayOutputStream outContent;
  private PrintStream originalOut;
  private ManagedRocksDB managedRocksDB;
  private RocksDB rocksDB;
  private ColumnFamilyHandle columnFamilyHandle;

  private static final String DB_PATH = "testDBPath";

  private MockedStatic<ManagedRocksDB> mockedDB;
  private MockedStatic<RocksDBUtils> mockedUtils;

  @BeforeEach
  public void setup() throws Exception {
    // Capture console output
    outContent = new ByteArrayOutputStream();
    originalOut = System.out;
    System.setOut(new PrintStream(outContent));

    // Initialize static mocks
    mockedDB = mockStatic(ManagedRocksDB.class);
    mockedUtils = mockStatic(RocksDBUtils.class);
  }

  @AfterEach
  public void tearDown() {
    if (mockedDB != null) {
      mockedDB.close();
    }
    if (mockedUtils != null) {
      mockedUtils.close();
    }
  }

  private void setupMockDB(SnapshotInfo snapshotInfo,
      SnapshotInfo globalPrevSnapshot,
      SnapshotInfo pathPrevSnapshot) throws Exception {

    managedRocksDB = mock(ManagedRocksDB.class);
    rocksDB = mock(RocksDB.class);
    columnFamilyHandle = mock(ColumnFamilyHandle.class);

    when(managedRocksDB.get()).thenReturn(rocksDB);

    // Mock column family descriptors
    List<ColumnFamilyDescriptor> cfDescList = new ArrayList<>();
    cfDescList.add(new ColumnFamilyDescriptor(SNAPSHOT_INFO_TABLE.getBytes()));

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

    // Setup iterator behavior
    when(rocksIterator.isValid())
        .thenReturn(true, true, true, false);
    when(rocksIterator.value())
        .thenReturn(SnapshotInfo.getCodec().toPersistedFormat(snapshotInfo))
        .thenReturn(SnapshotInfo.getCodec().toPersistedFormat(globalPrevSnapshot))
        .thenReturn(SnapshotInfo.getCodec().toPersistedFormat(pathPrevSnapshot));
  }

  @Test
  public void testSuccessfulRepair() throws Exception {
    // Create test data
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String snapshotName = "snap1";
    String globalPrevSnapshotName = "global-prev-snap1";
    String pathPrevSnapshotName = "path-prev-snap1";

    UUID snapshotId = UUID.randomUUID();
    UUID globalPrevSnapshotId = UUID.randomUUID();
    UUID pathPrevSnapshotId = UUID.randomUUID();

    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(volumeName, bucketName, snapshotName, snapshotId, 0);
    SnapshotInfo globalPrevSnapshot = SnapshotInfo.newInstance(volumeName, bucketName, globalPrevSnapshotName,
        globalPrevSnapshotId, 0);
    SnapshotInfo pathPrevSnapshot = SnapshotInfo.newInstance(volumeName, bucketName, pathPrevSnapshotName,
        pathPrevSnapshotId, 0);

    String[] args = new String[] {
        "om",
        "snapshot",
        "chain",
        volumeName + "/" + bucketName,
        snapshotName,
        "--db", DB_PATH,
        "--global-previous", globalPrevSnapshotId.toString(),
        "--path-previous", pathPrevSnapshotId.toString(),
    };

    setupMockDB(snapshotInfo, globalPrevSnapshot, pathPrevSnapshot);

    CommandLine cli = new OzoneRepair().getCmd();
    // Execute repair with y input for prompt
    withTextFromSystemIn("y")
        .execute(() -> cli.execute(args));

    // Verify DB update was called with correct parameters
    verify(rocksDB).put(
        eq(columnFamilyHandle),
        eq(StringCodec.get().toPersistedFormat(snapshotInfo.getTableKey())),
        eq(SnapshotInfo.getCodec().toPersistedFormat(snapshotInfo)));

    String output = outContent.toString();
    assertTrue(output.contains("Snapshot Info is updated"));
  }
}
