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

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Tests OMAllocateBlockResponse - prefix layout.
 */
public class TestOMAllocateBlockResponseWithFSO
        extends TestOMAllocateBlockResponse {

  // logical ID, which really doesn't exist in dirTable
  private long parentID = 10;
  private String fileName = "file1";

  @Override
  protected OmKeyInfo createOmKeyInfo() throws Exception {
    // need to initialize parentID
    String parentDir = keyName;
    keyName = parentDir + OzoneConsts.OM_KEY_PREFIX + fileName;

    long txnId = 50;
    long objectId = parentID + 1;

    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
        .setObjectID(objectId)
        .setParentObjectID(parentID)
        .setUpdateID(txnId)
        .build();
  }

  @Override
  protected String getOpenKey() throws Exception {
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return omMetadataManager.getOpenFileName(volumeId, bucketId,
            parentID, fileName, clientID);
  }

  @Nonnull
  @Override
  protected OMAllocateBlockResponse getOmAllocateBlockResponse(
      OmKeyInfo omKeyInfo, OmBucketInfo omBucketInfo,
      OMResponse omResponse) throws IOException {
    return new OMAllocateBlockResponseWithFSO(omResponse, omKeyInfo, clientID,
        getBucketLayout(), omMetadataManager.getVolumeId(volumeName),
        omBucketInfo.getObjectID());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
