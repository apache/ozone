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

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
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
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
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
    String snapshotId = UUID.randomUUID().toString();
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(volumeName,
        bucketName,
        snapshotName,
        snapshotId);

    // confirm table is empty
    Assert.assertEquals(0,
        omMetadataManager
        .countRowsInTable(omMetadataManager.getSnapshotInfoTable()));

    // Prepare for deletedTable clean up logic verification
    Set<String> dtSentinelKeys = addTestKeysToDeletedTable(
        volumeName, bucketName);

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

    // Check deletedTable clean up works as expected
    verifyEntriesLeftInDeletedTable(dtSentinelKeys);
  }

  private Set<String> addTestKeysToDeletedTable(
      String volumeName, String bucketName) throws IOException {

    RepeatedOmKeyInfo dummyRepeatedKeyInfo = new RepeatedOmKeyInfo.Builder()
        .setOmKeyInfos(new ArrayList<>()).build();

    // Add deletedTable key entries that surround the snapshot scope
    Set<String> dtSentinelKeys = new HashSet<>();
    // Get a bucket name right before and after the bucketName
    // e.g. When bucketName is buck2, bucketNameBefore is buck1,
    // bucketNameAfter is buck3
    // This will not guarantee the bucket name is valid for Ozone but
    // this would be good enough for this unit test.
    char bucketNameLastChar = bucketName.charAt(bucketName.length() - 1);
    String bucketNameBefore = bucketName.substring(0, bucketName.length() - 1) +
        Character.toString((char)(bucketNameLastChar - 1));
    for (int i = 0; i < 3; i++) {
      String dtKey = omMetadataManager.getOzoneKey(volumeName, bucketNameBefore,
          "dtkey" + i);
      omMetadataManager.getDeletedTable().put(dtKey, dummyRepeatedKeyInfo);
      dtSentinelKeys.add(dtKey);
    }
    String bucketNameAfter = bucketName.substring(0, bucketName.length() - 1) +
        Character.toString((char)(bucketNameLastChar + 1));
    for (int i = 0; i < 3; i++) {
      String dtKey = omMetadataManager.getOzoneKey(volumeName, bucketNameAfter,
          "dtkey" + i);
      omMetadataManager.getDeletedTable().put(dtKey, dummyRepeatedKeyInfo);
      dtSentinelKeys.add(dtKey);
    }
    // Add deletedTable key entries in the snapshot (bucket) scope
    for (int i = 0; i < 10; i++) {
      String dtKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          "dtkey" + i);
      omMetadataManager.getDeletedTable().put(dtKey, dummyRepeatedKeyInfo);
    }

    return dtSentinelKeys;
  }

  private void verifyEntriesLeftInDeletedTable(Set<String> dtSentinelKeys)
      throws IOException {

    // Verify that only keys inside the snapshot scope are gone from
    // deletedTable.
    try (TableIterator<String,
        ? extends Table.KeyValue<String, RepeatedOmKeyInfo>>
        keyIter = omMetadataManager.getDeletedTable().iterator()) {
      keyIter.seekToFirst();
      while (keyIter.hasNext()) {
        Table.KeyValue<String, RepeatedOmKeyInfo> entry = keyIter.next();
        String dtKey = entry.getKey();
        // deletedTable should not have bucketName keys
        Assert.assertTrue("deletedTable should contain key",
            dtSentinelKeys.contains(dtKey));
        dtSentinelKeys.remove(dtKey);
      }
    }

    Assert.assertTrue("deletedTable is missing keys that should be there",
        dtSentinelKeys.isEmpty());
  }
}
