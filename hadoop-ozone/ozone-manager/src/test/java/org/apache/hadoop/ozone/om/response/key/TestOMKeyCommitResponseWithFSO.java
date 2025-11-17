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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Tests OMKeyCommitResponse - prefix layout.
 */
public class TestOMKeyCommitResponseWithFSO extends TestOMKeyCommitResponse {

  @Nonnull
  @Override
  protected OMKeyCommitResponse getOmKeyCommitResponse(OmKeyInfo omKeyInfo,
      OzoneManagerProtocolProtos.OMResponse omResponse, String openKey,
      String ozoneKey, RepeatedOmKeyInfo deleteKeys, Boolean isHSync, OmKeyInfo newOpenKeyInfo)
          throws IOException {
    assertNotNull(omBucketInfo);
    long volumeId = omMetadataManager.getVolumeId(omKeyInfo.getVolumeName());
    Map<String, RepeatedOmKeyInfo> deleteKeyMap = new HashMap<>();
    if (null != keysToDelete) {
      String deleteKey = omMetadataManager.getOzoneKey(volumeName,
          bucketName, keyName);
      deleteKeys.getOmKeyInfoList().stream().forEach(e -> deleteKeyMap.put(
          omMetadataManager.getOzoneDeletePathKey(e.getObjectID(), deleteKey),
          new RepeatedOmKeyInfo(e, omBucketInfo.getObjectID())));
    }
    return new OMKeyCommitResponseWithFSO(omResponse, omKeyInfo, ozoneKey,
        openKey, omBucketInfo, deleteKeyMap, volumeId, isHSync, newOpenKeyInfo, null, null);
  }

  @Nonnull
  @Override
  protected OmKeyInfo getOmKeyInfo() {
    assertNotNull(omBucketInfo);
    return OMRequestTestUtils.createOmKeyInfo(volumeName, omBucketInfo.getBucketName(), keyName, replicationConfig)
        .setObjectID(omBucketInfo.getObjectID() + 1)
        .setParentObjectID(omBucketInfo.getObjectID())
        .setUpdateID(100L)
        .build();
  }

  @Nonnull
  @Override
  protected void addKeyToOpenKeyTable() throws Exception {
    assertNotNull(omBucketInfo);
    long parentID = omBucketInfo.getObjectID();
    long objectId = parentID + 10;

    OmKeyInfo omKeyInfoFSO =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
            .setObjectID(objectId)
            .setParentObjectID(parentID)
            .setUpdateID(100L)
            .build();
    String fileName = OzoneFSUtils.getFileName(keyName);
    OMRequestTestUtils.addFileToKeyTable(true, false,
            fileName, omKeyInfoFSO, clientID, txnLogId, omMetadataManager);
  }

  @Nonnull
  @Override
  protected String getOpenKeyName() throws IOException  {
    assertNotNull(omBucketInfo);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return omMetadataManager.getOpenFileName(volumeId, bucketId,
            omBucketInfo.getObjectID(), keyName, clientID);
  }

  @Nonnull
  @Override
  protected String getOzoneKey()  throws IOException {
    assertNotNull(omBucketInfo);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return omMetadataManager.getOzonePathKey(volumeId, bucketId,
            omBucketInfo.getObjectID(), keyName);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
