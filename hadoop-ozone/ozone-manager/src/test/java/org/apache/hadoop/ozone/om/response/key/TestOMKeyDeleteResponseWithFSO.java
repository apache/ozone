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

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Tests OMKeyDeleteResponse - prefix layout.
 */
public class TestOMKeyDeleteResponseWithFSO extends TestOMKeyDeleteResponse {

  @Override
  protected OMKeyDeleteResponse getOmKeyDeleteResponse(OmKeyInfo omKeyInfo,
      OzoneManagerProtocolProtos.OMResponse omResponse) throws Exception {
    return new OMKeyDeleteResponseWithFSO(omResponse, omKeyInfo.getKeyName(),
        omKeyInfo, getOmBucketInfo(), false,
        omMetadataManager.getVolumeId(volumeName), null);
  }

  @Override
  protected String addKeyToTable() throws Exception {
    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);

    // Create parent dirs for the path
    long parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
            bucketName, "", omMetadataManager);

    OmKeyInfo omKeyInfo =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
            .setObjectID(parentId + 1)
            .setParentObjectID(parentId)
            .setUpdateID(100L)
            .build();
    OMRequestTestUtils.addFileToKeyTable(false, false,
            keyName, omKeyInfo, -1, 50, omMetadataManager);
    return omMetadataManager.getOzonePathKey(
            omMetadataManager.getVolumeId(volumeName),
            omMetadataManager.getBucketId(volumeName, bucketName),
           omKeyInfo.getParentObjectID(), keyName);
  }

  @Override
  protected OmKeyInfo getOmKeyInfo() {
    assertNotNull(getOmBucketInfo());
    return OMRequestTestUtils.createOmKeyInfo(volumeName, omBucketInfo.getBucketName(), keyName,
            replicationConfig)
        .setObjectID(getOmBucketInfo().getObjectID() + 1)
        .setParentObjectID(getOmBucketInfo().getObjectID())
        .setUpdateID(100L)
        .build();
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
