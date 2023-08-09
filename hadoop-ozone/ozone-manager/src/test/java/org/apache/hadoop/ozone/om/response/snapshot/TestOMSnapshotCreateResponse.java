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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.response.snapshot;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateSnapshotResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;

/**
 * This class tests OMSnapshotCreateResponse.
 */
public class TestOMSnapshotCreateResponse {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;
  private OzoneConfiguration ozoneConfiguration;

  @Before
  public void setup() throws Exception {
    ozoneConfiguration = new OzoneConfiguration();
    String fsPath = folder.newFolder().getAbsolutePath();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        fsPath);
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, null);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @After
  public void tearDown() {
    if (batchOperation != null) {
      batchOperation.close();
    }
  }

  @Test
  public void testAddToDBBatch() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String snapshotName = UUID.randomUUID().toString();
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(volumeName,
        bucketName,
        snapshotName,
        snapshotId,
        Time.now());

    // confirm table is empty
    Assert.assertEquals(0,
        omMetadataManager
        .countRowsInTable(omMetadataManager.getSnapshotInfoTable()));

    // Populate deletedTable and deletedDirectoryTable
    Set<String> dtSentinelKeys =
        addTestKeysToDeletedTable(volumeName, bucketName);
    Set<String> ddtSentinelKeys =
        addTestKeysToDeletedDirTable(volumeName, bucketName);

    // commit to table
    OMSnapshotCreateResponse omSnapshotCreateResponse =
        new OMSnapshotCreateResponse(OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateSnapshot)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCreateSnapshotResponse(
                CreateSnapshotResponse.newBuilder()
                .setSnapshotInfo(snapshotInfo.getProtobuf())
                .build()).build(), snapshotInfo);
    omSnapshotCreateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Confirm snapshot directory was created
    String snapshotDir = getSnapshotPath(ozoneConfiguration, snapshotInfo);
    Assert.assertTrue((new File(snapshotDir)).exists());

    // Confirm table has 1 entry
    Assert.assertEquals(1, omMetadataManager
        .countRowsInTable(omMetadataManager.getSnapshotInfoTable()));

    // Check contents of entry
    Table.KeyValue<String, SnapshotInfo> keyValue =
        omMetadataManager.getSnapshotInfoTable().iterator().next();
    SnapshotInfo storedInfo = keyValue.getValue();
    Assert.assertEquals(snapshotInfo.getTableKey(), keyValue.getKey());
    Assert.assertEquals(snapshotInfo, storedInfo);

    // Check deletedTable and deletedDirectoryTable clean up work as expected
    verifyEntriesLeftInDeletedTable(dtSentinelKeys);
    verifyEntriesLeftInDeletedDirTable(ddtSentinelKeys);
  }

  private Set<String> addTestKeysToDeletedTable(
      String volumeName, String bucketName) throws IOException {

    RepeatedOmKeyInfo dummyRepeatedKeyInfo = new RepeatedOmKeyInfo.Builder()
        .setOmKeyInfos(new ArrayList<>()).build();

    // Add deletedTable key entries that "surround" the snapshot scope
    Set<String> sentinelKeys = new HashSet<>();
    // Get a bucket name right before and after the bucketName
    // e.g. When bucketName is buck2, bucketNameBefore is buck1,
    // bucketNameAfter is buck3
    // This will not guarantee the bucket name is valid for Ozone but
    // this would be good enough for this unit test.
    char bucketNameLastChar = bucketName.charAt(bucketName.length() - 1);

    String bucketNameBefore = bucketName.substring(0, bucketName.length() - 1) +
        (char) (bucketNameLastChar - 1);
    for (int i = 0; i < 3; i++) {
      String dtKey = omMetadataManager.getOzoneKey(volumeName, bucketNameBefore,
          "dtkey" + i);
      omMetadataManager.getDeletedTable().put(dtKey, dummyRepeatedKeyInfo);
      sentinelKeys.add(dtKey);
    }

    String bucketNameAfter = bucketName.substring(0, bucketName.length() - 1) +
        (char) (bucketNameLastChar + 1);
    for (int i = 0; i < 3; i++) {
      String dtKey = omMetadataManager.getOzoneKey(volumeName, bucketNameAfter,
          "dtkey" + i);
      omMetadataManager.getDeletedTable().put(dtKey, dummyRepeatedKeyInfo);
      sentinelKeys.add(dtKey);
    }

    // Add deletedTable key entries in the snapshot (bucket) scope
    for (int i = 0; i < 10; i++) {
      String dtKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          "dtkey" + i);
      omMetadataManager.getDeletedTable().put(dtKey, dummyRepeatedKeyInfo);
      // These are the keys that should be deleted.
      // Thus not added to sentinelKeys list.
    }

    return sentinelKeys;
  }

  /**
   * Populates deletedDirectoryTable for the test.
   * @param volumeName volume name
   * @param bucketName bucket name
   * @return A set of DB keys
   */
  private Set<String> addTestKeysToDeletedDirTable(
      String volumeName, String bucketName) throws IOException {

    OMSnapshotResponseTestUtil.addVolumeBucketInfoToTable(
        omMetadataManager, volumeName, bucketName);

    final OmKeyInfo dummyOmKeyInfo = new OmKeyInfo.Builder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setKeyName("dummyKey")
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .build();
    // Add deletedDirectoryTable key entries that "surround" the snapshot scope
    Set<String> sentinelKeys = new HashSet<>();

    final String dbKeyPfx =
        OmSnapshotManager.getOzonePathKeyWithVolumeBucketNames(
            omMetadataManager, volumeName, bucketName);

    // Calculate offset to bucketId's last character in dbKeyPfx.
    // First -1 for offset, second -1 for second to last char (before '/')
    final int offset = dbKeyPfx.length() - 1 - 1;

    char bucketIdLastChar = dbKeyPfx.charAt(offset);

    String dbKeyPfxBefore =  dbKeyPfx.substring(0, offset) +
        (char) (bucketIdLastChar - 1) + dbKeyPfx.substring(offset);
    for (int i = 0; i < 3; i++) {
      String dtKey = dbKeyPfxBefore + "dir" + i;
      omMetadataManager.getDeletedDirTable().put(dtKey, dummyOmKeyInfo);
      sentinelKeys.add(dtKey);
    }

    String dbKeyPfxAfter =  dbKeyPfx.substring(0, offset) +
        (char) (bucketIdLastChar + 1) + dbKeyPfx.substring(offset);
    for (int i = 0; i < 3; i++) {
      String dtKey = dbKeyPfxAfter + "dir" + i;
      omMetadataManager.getDeletedDirTable().put(dtKey, dummyOmKeyInfo);
      sentinelKeys.add(dtKey);
    }

    // Add key entries in the snapshot (bucket) scope
    for (int i = 0; i < 10; i++) {
      String dtKey = dbKeyPfx + "dir" + i;
      omMetadataManager.getDeletedDirTable().put(dtKey, dummyOmKeyInfo);
      // These are the keys that should be deleted.
      // Thus not added to sentinelKeys list.
    }

    return sentinelKeys;
  }

  private void verifyEntriesLeftInDeletedTable(Set<String> expectedKeys)
      throws IOException {
    // Only keys inside the snapshot scope would be deleted from deletedTable.
    verifyEntriesLeftInTable(omMetadataManager.getDeletedTable(), expectedKeys);
  }

  private void verifyEntriesLeftInDeletedDirTable(Set<String> expectedKeys)
      throws IOException {
    verifyEntriesLeftInTable(omMetadataManager.getDeletedDirTable(),
        expectedKeys);
  }

  private void verifyEntriesLeftInTable(
      Table<String, ?> table, Set<String> expectedKeys) throws IOException {

    try (TableIterator<String, ? extends Table.KeyValue<String, ?>>
        keyIter = table.iterator()) {
      keyIter.seekToFirst();
      while (keyIter.hasNext()) {
        Table.KeyValue<String, ?> entry = keyIter.next();
        String dbKey = entry.getKey();
        Assert.assertTrue(table.getName() + " should contain key",
            expectedKeys.contains(dbKey));
        expectedKeys.remove(dbKey);
      }
    }

    Assert.assertTrue(table.getName() + " is missing keys that should be there",
        expectedKeys.isEmpty());
  }
}
