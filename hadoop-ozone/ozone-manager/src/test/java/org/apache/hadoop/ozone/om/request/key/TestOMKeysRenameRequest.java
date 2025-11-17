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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysMap;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysRequest;
import org.junit.jupiter.api.Test;

/**
 * Tests RenameKey request.
 */
public class TestOMKeysRenameRequest extends TestOMKeyRequest {

  private int count = 10;
  private String parentDir = "/test";

  @Test
  public void testKeysRenameRequest() throws Exception {

    OMRequest modifiedOmRequest = createRenameKeyRequest(false);

    OMKeysRenameRequest omKeysRenameRequest =
        new OMKeysRenameRequest(modifiedOmRequest, getBucketLayout());

    OMClientResponse omKeysRenameResponse =
        omKeysRenameRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertTrue(omKeysRenameResponse.getOMResponse().getSuccess());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omKeysRenameResponse.getOMResponse().getStatus());

    for (int i = 0; i < count; i++) {
      // Original key should be deleted, toKey should exist.
      OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
          .get(omMetadataManager.getOzoneKey(volumeName, bucketName,
              parentDir.concat("/key" + i)));
      assertNull(omKeyInfo);

      omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(
          omMetadataManager.getOzoneKey(volumeName, bucketName,
              parentDir.concat("/newKey" + i)));
      assertNotNull(omKeyInfo);
    }

  }

  @Test
  public void testKeysRenameRequestFail() throws Exception {
    OMRequest modifiedOmRequest = createRenameKeyRequest(true);

    OMKeysRenameRequest omKeysRenameRequest =
        new OMKeysRenameRequest(modifiedOmRequest, getBucketLayout());

    OMClientResponse omKeysRenameResponse =
        omKeysRenameRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertFalse(omKeysRenameResponse.getOMResponse().getSuccess());
    assertEquals(OzoneManagerProtocolProtos.Status.PARTIAL_RENAME,
        omKeysRenameResponse.getOMResponse().getStatus());

    // The keys（key0 to key9）can be renamed success.
    for (int i = 0; i < count; i++) {
      // Original key should be deleted, toKey should exist.
      OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
          .get(omMetadataManager.getOzoneKey(volumeName, bucketName,
              parentDir.concat("/key" + i)));
      assertNull(omKeyInfo);

      omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(
          omMetadataManager.getOzoneKey(volumeName, bucketName,
              parentDir.concat("/newKey" + i)));
      assertNotNull(omKeyInfo);
    }

    // The key not rename should be in unRenamedKeys.
    RenameKeysMap unRenamedKeys = omKeysRenameResponse.getOMResponse()
        .getRenameKeysResponse().getUnRenamedKeys(0);
    assertEquals("testKey", unRenamedKeys.getFromKeyName());
  }

  /**
   * Create OMRequest which encapsulates RenameKeyRequest.
   *
   * @return OMRequest
   */
  private OMRequest createRenameKeyRequest(Boolean isIllegal) throws Exception {

    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    List<RenameKeysMap> renameKeyList  = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      String key = parentDir.concat("/key" + i);
      String toKey = parentDir.concat("/newKey" + i);
      OMRequestTestUtils.addKeyToTableCache(volumeName, bucketName,
          parentDir.concat("/key" + i), RatisReplicationConfig.getInstance(THREE), omMetadataManager);

      RenameKeysMap.Builder renameKey = RenameKeysMap.newBuilder()
          .setFromKeyName(key)
          .setToKeyName(toKey);
      renameKeyList.add(renameKey.build());
    }


    // Generating illegal data causes Rename Keys to fail.
    if (isIllegal) {
      RenameKeysMap.Builder renameKey = RenameKeysMap.newBuilder()
          .setFromKeyName("testKey")
          .setToKeyName("toKey");
      renameKeyList.add(renameKey.build());
    }

    RenameKeysArgs.Builder renameKeyArgs = RenameKeysArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .addAllRenameKeysMap(renameKeyList);

    RenameKeysRequest.Builder renameKeysReq = RenameKeysRequest.newBuilder()
        .setRenameKeysArgs(renameKeyArgs.build());

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setRenameKeysRequest(renameKeysReq.build())
        .setCmdType(OzoneManagerProtocolProtos.Type.RenameKeys).build();
  }

}
