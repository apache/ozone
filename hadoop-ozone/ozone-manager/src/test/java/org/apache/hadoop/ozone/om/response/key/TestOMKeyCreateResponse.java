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

import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;

import java.io.IOException;


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

    Assert.assertFalse(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));
    omKeyCreateResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertTrue(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));
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
    Assert.assertFalse(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));

    omKeyCreateResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse is error it is a no-op.
    Assert.assertFalse(
        omMetadataManager.getOpenKeyTable(getBucketLayout()).isExist(openKey));

  }

  @NotNull
  protected OMKeyCreateResponse getOmKeyCreateResponse(OmKeyInfo keyInfo,
      OmBucketInfo bucketInfo, OMResponse response) throws IOException {

    return new OMKeyCreateResponse(response, keyInfo, null, clientID,
            bucketInfo);
  }
}
