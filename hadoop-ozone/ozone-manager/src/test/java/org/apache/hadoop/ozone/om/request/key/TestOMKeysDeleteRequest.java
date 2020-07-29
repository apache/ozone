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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_DELETE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;

/**
 * Class tests OMKeysDeleteRequest.
 */
public class TestOMKeysDeleteRequest extends TestOMKeyRequest {


  private List<String> deleteKeyList;
  private OMRequest omRequest;

  @Test
  public void testKeysDeleteRequest() throws Exception {

    createPreRequisites();

    OMKeysDeleteRequest omKeysDeleteRequest =
        new OMKeysDeleteRequest(omRequest);

    OMClientResponse omClientResponse =
        omKeysDeleteRequest.validateAndUpdateCache(ozoneManager, 0L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getSuccess());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    Assert.assertTrue(omClientResponse.getOMResponse().getDeleteKeysResponse()
        .getStatus());
    DeleteKeyArgs unDeletedKeys =
        omClientResponse.getOMResponse().getDeleteKeysResponse()
            .getUnDeletedKeys();
    Assert.assertEquals(0,
        unDeletedKeys.getKeysCount());

    // Check all keys are deleted.
    for (String deleteKey : deleteKeyList) {
      Assert.assertNull(omMetadataManager.getKeyTable()
          .get(omMetadataManager.getOzoneKey(volumeName, bucketName,
              deleteKey)));
    }

  }

  @Test
  public void testKeysDeleteRequestFail() throws Exception {

    createPreRequisites();

    // Add a key which not exist, which causes batch delete to fail.

    omRequest = omRequest.toBuilder()
            .setDeleteKeysRequest(DeleteKeysRequest.newBuilder()
                .setDeleteKeys(DeleteKeyArgs.newBuilder()
                .setBucketName(bucketName).setVolumeName(volumeName)
                    .addAllKeys(deleteKeyList).addKeys("dummy"))).build();

    OMKeysDeleteRequest omKeysDeleteRequest =
        new OMKeysDeleteRequest(omRequest);

    OMClientResponse omClientResponse =
        omKeysDeleteRequest.validateAndUpdateCache(ozoneManager, 0L,
        ozoneManagerDoubleBufferHelper);

    Assert.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assert.assertEquals(PARTIAL_DELETE,
        omClientResponse.getOMResponse().getStatus());

    Assert.assertFalse(omClientResponse.getOMResponse().getDeleteKeysResponse()
        .getStatus());

    // Check keys are deleted and in response check unDeletedKey.
    for (String deleteKey : deleteKeyList) {
      Assert.assertNull(omMetadataManager.getKeyTable()
          .get(omMetadataManager.getOzoneKey(volumeName, bucketName,
              deleteKey)));
    }

    DeleteKeyArgs unDeletedKeys = omClientResponse.getOMResponse()
        .getDeleteKeysResponse().getUnDeletedKeys();
    Assert.assertEquals(1,
        unDeletedKeys.getKeysCount());
    Assert.assertEquals("dummy", unDeletedKeys.getKeys(0));

  }

  private void createPreRequisites() throws Exception {

    deleteKeyList = new ArrayList<>();
    // Add volume, bucket and key entries to OM DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    int count = 10;

    DeleteKeyArgs.Builder deleteKeyArgs = DeleteKeyArgs.newBuilder()
        .setBucketName(bucketName).setVolumeName(volumeName);

    // Create 10 keys
    String parentDir = "/user";
    String key = "";


    for (int i = 0; i < count; i++) {
      key = parentDir.concat("/key" + i);
      TestOMRequestUtils.addKeyToTableCache(volumeName, bucketName,
          parentDir.concat("/key" + i), HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE, omMetadataManager);
      deleteKeyArgs.addKeys(key);
      deleteKeyList.add(key);
    }

    omRequest =
        OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
            .setCmdType(DeleteKeys)
            .setDeleteKeysRequest(DeleteKeysRequest.newBuilder()
                .setDeleteKeys(deleteKeyArgs).build()).build();
  }

}
