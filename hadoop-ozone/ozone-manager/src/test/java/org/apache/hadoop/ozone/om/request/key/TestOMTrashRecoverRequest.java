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

package org.apache.hadoop.ozone.om.request.key;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RecoverTrashRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

/**
 * Tests OMTrashRecoverRequest request.
 */
public class TestOMTrashRecoverRequest extends TestOMKeyRequest {

  @Test
  public void testPreExecute() throws Exception {
    doPreExecute(createRecoverTrashRequest());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createRecoverTrashRequest());

    OMTrashRecoverRequest omTrashRecoverRequest =
        new OMTrashRecoverRequest(modifiedOmRequest);

    // Add volume, bucket and key entries to OM DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    // TransactionLogIndex is 0L here.
    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    String tableKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
    // let's set transactionLogIndex 10L here.
    long trxnLogIndex = 10L;
    TestOMRequestUtils.deleteKey(tableKey, omMetadataManager, trxnLogIndex);

    // We delete entry in keyTable when executing deleteKey.
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(tableKey);
    Assert.assertNull(omKeyInfo);

    trxnLogIndex = trxnLogIndex + 10;
    OMClientResponse omClientResponse =
        omTrashRecoverRequest.validateAndUpdateCache(ozoneManager,
        trxnLogIndex, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // Now after calling validateAndUpdateCache, it should be existed.
    omKeyInfo = omMetadataManager.getKeyTable().get(tableKey);
    Assert.assertNotNull(omKeyInfo);
  }

  @Test
  public void testReplayRequest() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createRecoverTrashRequest());

    OMTrashRecoverRequest omTrashRecoverRequest =
        new OMTrashRecoverRequest(modifiedOmRequest);

    // Add volume, bucket and key entries to OM DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    TestOMRequestUtils.addKeyToTableAndCache(volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, 1L, omMetadataManager);

    // let's set transactionLogIndex 10L here.
    long trxnLogIndex = 10L;
    String tableKey = omMetadataManager
        .getOzoneKey(volumeName, bucketName, keyName);
    TestOMRequestUtils.deleteKey(tableKey, omMetadataManager, trxnLogIndex);

    // Replay the original TrashRecoverRequest.
    OMClientResponse omClientResponse = omTrashRecoverRequest
        .validateAndUpdateCache(ozoneManager, trxnLogIndex,
            ozoneManagerDoubleBufferHelper);

    // Replay should result in Replay response
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.REPLAY,
        omClientResponse.getOMResponse().getStatus());
  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOmRequest - OMRequest not preExecute.
   * @return OMRequest - modified request returned from preExecute.
   */
  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {

    OMTrashRecoverRequest omTrashRecoverRequest =
        new OMTrashRecoverRequest(originalOmRequest);

    OMRequest modifiedOmRequest = omTrashRecoverRequest
        .preExecute(ozoneManager);

    // Not be equal as updating modificationTime.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  /**
   * Create OMRequest which encapsulates RecoverTrashRequest.
   * @return OMRequest - initial OMRequest.
   */
  private OMRequest createRecoverTrashRequest() {

    //TODO: HDDS-2425 recover to non-existing bucket.
    RecoverTrashRequest recoverTrashRequest =
        RecoverTrashRequest.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setDestinationBucket(bucketName)
            .setModificationTime(0L)
            .build();

    return OMRequest.newBuilder().setRecoverTrashRequest(recoverTrashRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.RecoverTrash)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}
