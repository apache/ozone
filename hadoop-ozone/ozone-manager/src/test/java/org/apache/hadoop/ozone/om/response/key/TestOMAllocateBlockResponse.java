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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.Test;

/**
 * Tests OMAllocateBlockResponse.
 */
public class TestOMAllocateBlockResponse extends TestOMKeyResponse {

  @Test
  public void testAddToDBBatch() throws Exception {

    OmKeyInfo omKeyInfo = createOmKeyInfo();

    OMResponse omResponse = OMResponse.newBuilder()
        .setAllocateBlockResponse(
            AllocateBlockResponse.getDefaultInstance())
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
        .build();
    OMAllocateBlockResponse omAllocateBlockResponse =
            getOmAllocateBlockResponse(omKeyInfo, omBucketInfo, omResponse);

    String openKey = getOpenKey();

    // Not adding key entry before to test whether commit is successful or not.
    assertFalse(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));
    omAllocateBlockResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));
  }

  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {
    OmKeyInfo omKeyInfo = createOmKeyInfo();

    OMResponse omResponse = OMResponse.newBuilder()
        .setAllocateBlockResponse(
            AllocateBlockResponse.getDefaultInstance())
        .setStatus(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND)
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
        .build();
    OMAllocateBlockResponse omAllocateBlockResponse =
            getOmAllocateBlockResponse(omKeyInfo, omBucketInfo, omResponse);

    // Before calling addToDBBatch
    String openKey = getOpenKey();
    assertFalse(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));

    omAllocateBlockResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse is error it is a no-op.
    assertFalse(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));

  }

  protected OmKeyInfo createOmKeyInfo() throws Exception {
    return OMRequestTestUtils.createOmKeyInfo(volumeName,
        bucketName, keyName, replicationConfig).build();
  }

  protected String getOpenKey() throws Exception {
    return omMetadataManager.getOpenKey(volumeName, bucketName,
            keyName, clientID);
  }

  @Nonnull
  protected OMAllocateBlockResponse getOmAllocateBlockResponse(
          OmKeyInfo omKeyInfo, OmBucketInfo omBucketInfo,
          OMResponse omResponse) throws IOException {
    return new OMAllocateBlockResponse(omResponse, omKeyInfo, clientID,
        getBucketLayout());
  }
}
