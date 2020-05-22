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

import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RecoverTrashResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

/**
 * Tests OMTrashRecoverResponse.
 */
public class TestOMTrashRecoverResponse extends TestOMKeyResponse {

  @Test
  public void testAddToDBBatch() throws Exception {

    OmKeyInfo omKeyInfo = TestOMRequestUtils.createOmKeyInfo(volumeName,
        bucketName, keyName, replicationType, replicationFactor);
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    String tableKey = omMetadataManager
        .getOzoneKey(volumeName, bucketName, keyName);
    long trxnLogIndex = 10L;
    TestOMRequestUtils.deleteKey(tableKey, omMetadataManager, trxnLogIndex);

    Assert.assertNull(omMetadataManager.getKeyTable().get(tableKey));

    RecoverTrashResponse recoverTrashResponse = RecoverTrashResponse
        .newBuilder().setResponse(true).build();
    OMResponse omResponse = OMResponse.newBuilder()
        .setRecoverTrashResponse(recoverTrashResponse)
        .setStatus(Status.OK).setCmdType(Type.RecoverTrash)
        .build();

    RepeatedOmKeyInfo trashRepeatedKeyInfo =
        omMetadataManager.getTrashTable().get(tableKey);
    OMTrashRecoverResponse omTrashRecoverResponse =
        new OMTrashRecoverResponse(trashRepeatedKeyInfo, omKeyInfo, omResponse);

    omTrashRecoverResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertNotNull(omMetadataManager.getKeyTable().get(tableKey));

    /* TODO: HDDS-2425 Complete tests about the table used in the flow.
     * Include trashTable and deletedTable.
     */
  }

}
