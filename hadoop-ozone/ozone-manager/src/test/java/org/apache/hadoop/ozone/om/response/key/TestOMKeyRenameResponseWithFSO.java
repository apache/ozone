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

package org.apache.hadoop.ozone.om.response.key;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.OMResponseTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests TestOMKeyRenameResponseWithFSO.
 */
public class TestOMKeyRenameResponseWithFSO extends TestOMKeyRenameResponse {
  @Override
  protected OmKeyInfo getOmKeyInfo(String keyName) {
    long bucketId = random.nextLong();
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
        .setObjectID(bucketId + 100)
        .setParentObjectID(bucketId + 101)
        .build();
  }

  /**
   * Renames a directory twice in one batch in a snapshotted bucket. snapshotRenamedTable must keep the name the
   * directory has in the snapshot.
   */
  @Test
  public void testSecondDirectoryRenameInSameBatchKeepsSnapshotName() throws Exception {
    OMRequestTestUtils.addSnapshotToTable(volumeName, bucketName, "snap", omMetadataManager);
    OmKeyInfo fromDirInfo = getOmKeyInfo(UUID.randomUUID().toString());
    OmKeyInfo midDirInfo = getOmKeyInfo(fromDirInfo, UUID.randomUUID().toString());
    OmKeyInfo toDirInfo = getOmKeyInfo(fromDirInfo, UUID.randomUUID().toString());
    String dbFromKey = OMRequestTestUtils.addDirKeyToDirTable(false, OMFileRequest.getDirectoryInfo(fromDirInfo),
        volumeName, bucketName, txnLogId, omMetadataManager);

    getDirectoryRenameResponse(fromDirInfo, midDirInfo).addToDBBatch(omMetadataManager, batchOperation);
    getDirectoryRenameResponse(midDirInfo, toDirInfo).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertThat(omMetadataManager.getDirectoryTable().get(getDBKeyName(toDirInfo))).isNotNull();
    String renameDbKey = omMetadataManager.getRenameKey(volumeName, bucketName, toDirInfo.getObjectID());
    assertThat(omMetadataManager.getSnapshotRenamedTable().get(renameDbKey)).isEqualTo(dbFromKey);
  }

  private OMKeyRenameResponseWithFSO getDirectoryRenameResponse(OmKeyInfo fromDirInfo, OmKeyInfo toDirInfo)
      throws IOException {
    return new OMKeyRenameResponseWithFSO(getOkRenameResponse(), getDBKeyName(fromDirInfo),
        getDBKeyName(toDirInfo), null, null, toDirInfo, null, true, getBucketLayout());
  }

  @Override
  protected OmKeyInfo getOmKeyInfo(OmKeyInfo toKeyInfo,
                                   String keyName) {
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
        .setObjectID(toKeyInfo.getObjectID())
        .setParentObjectID(toKeyInfo.getParentObjectID())
        .setUpdateID(0L)
        .setCreationTime(toKeyInfo.getCreationTime())
        .build();
  }

  @Override
  protected String addKeyToTable(OmKeyInfo keyInfo) throws Exception {
    OMRequestTestUtils.addFileToKeyTable(false, false,
        keyInfo.getFileName(), keyInfo, clientID, txnLogId, omMetadataManager);
    return getDBKeyName(keyInfo);
  }

  @Override
  protected String getDBKeyName(OmKeyInfo keyInfo) throws IOException {
    return omMetadataManager.getOzonePathKey(
        omMetadataManager.getVolumeId(volumeName),
        omMetadataManager.getBucketId(volumeName, bucketName),
        keyInfo.getParentObjectID(), keyInfo.getKeyName());
  }

  @Override
  protected OMKeyRenameResponse getOMKeyRenameResponse(OMResponse response,
      OmKeyInfo fromKeyInfo, OmKeyInfo toKeyInfo) throws IOException {
    createParent();
    return new OMKeyRenameResponseWithFSO(response, getDBKeyName(fromKeyInfo),
        getDBKeyName(toKeyInfo), fromKeyParent, toKeyParent, toKeyInfo,
        bucketInfo, false, getBucketLayout());
  }

  protected void createParent() {
    long bucketId = random.nextLong();
    String fromKeyParentName = UUID.randomUUID().toString();
    String toKeyParentName = UUID.randomUUID().toString();
    fromKeyParent = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, fromKeyParentName, replicationConfig)
        .setObjectID(bucketId + 100L)
        .setParentObjectID(bucketId)
        .build();
    toKeyParent = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, toKeyParentName, replicationConfig)
        .setObjectID(bucketId + 101L)
        .setParentObjectID(bucketId)
        .build();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    bucketInfo = OMResponseTestUtils.createBucket(volumeName, bucketName);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
