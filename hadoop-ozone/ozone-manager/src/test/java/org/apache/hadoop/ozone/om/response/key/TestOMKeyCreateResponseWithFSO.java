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
import java.util.ArrayList;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Tests OMKeyCreateResponseWithFSO.
 */
public class TestOMKeyCreateResponseWithFSO extends TestOMKeyCreateResponse {

  @Nonnull
  @Override
  protected String getOpenKeyName() throws IOException {
    assertNotNull(omBucketInfo);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return omMetadataManager.getOpenFileName(volumeId, bucketId,
            omBucketInfo.getObjectID(), keyName, clientID);
  }

  @Nonnull
  @Override
  protected OmKeyInfo getOmKeyInfo() {
    assertNotNull(omBucketInfo);
    return OMRequestTestUtils.createOmKeyInfo(volumeName, omBucketInfo.getBucketName(), keyName,
            RatisReplicationConfig.getInstance(ONE))
        .setObjectID(omBucketInfo.getObjectID() + 1)
        .setParentObjectID(omBucketInfo.getObjectID())
        .setUpdateID(100L)
        .build();
  }

  @Nonnull
  @Override
  protected OMKeyCreateResponse getOmKeyCreateResponse(OmKeyInfo keyInfo,
      OmBucketInfo bucketInfo, OMResponse response) throws IOException {

    return new OMKeyCreateResponseWithFSO(response, keyInfo, new ArrayList<>(),
        clientID, bucketInfo, getVolumeId());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
