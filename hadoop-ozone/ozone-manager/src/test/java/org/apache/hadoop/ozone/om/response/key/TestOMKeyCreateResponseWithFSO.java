/**
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

package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Tests OMKeyCreateResponseWithFSO.
 */
public class TestOMKeyCreateResponseWithFSO extends TestOMKeyCreateResponse {

  @NotNull
  @Override
  protected String getOpenKeyName() throws IOException {
    Assertions.assertNotNull(omBucketInfo);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return omMetadataManager.getOpenFileName(volumeId, bucketId,
            omBucketInfo.getObjectID(), keyName, clientID);
  }

  @NotNull
  @Override
  protected OmKeyInfo getOmKeyInfo() {
    Assertions.assertNotNull(omBucketInfo);
    return OMRequestTestUtils.createOmKeyInfo(volumeName,
            omBucketInfo.getBucketName(), keyName, replicationType,
            replicationFactor,
            omBucketInfo.getObjectID() + 1,
            omBucketInfo.getObjectID(), 100, Time.now());
  }

  @NotNull
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
