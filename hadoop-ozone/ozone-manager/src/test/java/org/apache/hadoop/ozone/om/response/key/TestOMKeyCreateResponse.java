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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.Test;

/**
 * Tests MKeyCreateResponse.
 */
public class TestOMKeyCreateResponse extends TestOMKeyResponse {

  protected long getVolumeId() throws IOException {
    return omMetadataManager.getVolumeId(volumeName);
  }

  @Test
  public void testAddToDBBatch() throws Exception {

    OmKeyInfo omKeyInfo = getOmKeyInfo();

    OMResponse omResponse = OMResponse.newBuilder().setCreateKeyResponse(
                CreateKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateKey)
            .build();

    OMKeyCreateResponse omKeyCreateResponse =
            getOmKeyCreateResponse(omKeyInfo, omBucketInfo,
                    omResponse);

    String openKey = getOpenKeyName();

    assertFalse(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));
    omKeyCreateResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));
  }

  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {

    OmKeyInfo omKeyInfo = getOmKeyInfo();

    OMResponse omResponse = OMResponse.newBuilder().setCreateKeyResponse(
        CreateKeyResponse.getDefaultInstance())
        .setStatus(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND)
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateKey)
        .build();

    OMKeyCreateResponse omKeyCreateResponse =
            getOmKeyCreateResponse(omKeyInfo, omBucketInfo,
                    omResponse);

    // Before calling addToDBBatch
    String openKey = getOpenKeyName();
    assertFalse(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));

    omKeyCreateResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse is error it is a no-op.
    assertFalse(omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));

  }

  @Nonnull
  protected OMKeyCreateResponse getOmKeyCreateResponse(OmKeyInfo keyInfo,
      OmBucketInfo bucketInfo, OMResponse response) throws IOException {

    return new OMKeyCreateResponse(response, keyInfo, null, clientID,
            bucketInfo);
  }
}
