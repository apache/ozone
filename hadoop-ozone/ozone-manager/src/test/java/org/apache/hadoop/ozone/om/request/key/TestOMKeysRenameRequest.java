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
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysMap;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests RenameKey request.
 */
public class TestOMKeysRenameRequest extends OMKeyRequestTests {

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

  @Test
  public void testKeysRenameRequestToExistingKey() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager);

    String fromKeyName = parentDir.concat("/fromKey");
    String toKeyName = parentDir.concat("/toKey");
    OmKeyInfo fromKeyInfo = OMRequestTestUtils.addKeyToTableCache(volumeName, bucketName, fromKeyName,
        RatisReplicationConfig.getInstance(THREE), omMetadataManager);
    OmKeyInfo toKeyInfo = OMRequestTestUtils.addKeyToTableCache(volumeName, bucketName, toKeyName,
        RatisReplicationConfig.getInstance(THREE), omMetadataManager);

    RenameKeysMap renameKey = RenameKeysMap.newBuilder().setFromKeyName(fromKeyName).setToKeyName(toKeyName).build();
    // fromKey does not exist either, the pair should still be reported only once.
    RenameKeysMap missingRenameKey =
        RenameKeysMap.newBuilder().setFromKeyName("testKey").setToKeyName(toKeyName).build();
    OMRequest omRequest = createRenameKeyRequest(Arrays.asList(renameKey, missingRenameKey));

    OMClientResponse omKeysRenameResponse =
        new OMKeysRenameRequest(omRequest, getBucketLayout()).validateAndUpdateCache(ozoneManager, 100L);

    assertThat(omKeysRenameResponse.getOMResponse().getStatus())
        .isEqualTo(OzoneManagerProtocolProtos.Status.PARTIAL_RENAME);
    assertThat(omKeysRenameResponse.getOMResponse().getRenameKeysResponse().getUnRenamedKeysList())
        .containsExactly(renameKey, missingRenameKey);

    // The existing toKey must not be overwritten, and fromKey must stay.
    assertThat(omMetadataManager.getKeyTable(getBucketLayout())
        .get(omMetadataManager.getOzoneKey(volumeName, bucketName, fromKeyName))).isEqualTo(fromKeyInfo);
    assertThat(omMetadataManager.getKeyTable(getBucketLayout())
        .get(omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName))).isEqualTo(toKeyInfo);
  }

  /**
   * Whatever preExecute and validateAndUpdateCache make of a transaction, the key table committed by its response
   * matches the cache. A transaction with a pair that shares a key with an earlier pair is rejected by preExecute.
   */
  @ParameterizedTest
  // Without the check the first three cases leave the DB different from the cache (the first two only for these key
  // names); the a to d pair replaces the response entry of the a to b pair. The next three commit correctly.
  @CsvSource(delimiter = '|', value = {
      "key>key_b key_b>key_c | key | true",
      "key>key_b other>key | key other | true",
      "a>b c>a a>d | a c | true",
      "a>b a>c | a | true",
      "a>c b>c | a b | true",
      "a>b c>d b>e | a c | true",
      "key>key_b other>other_b | key other | false"})
  public void testCommittedKeyTableMatchesCache(String pairs, String existingKeys, boolean expectRejected)
      throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager);
    for (String name : existingKeys.split(" ")) {
      OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, name, 0L,
          RatisReplicationConfig.getInstance(THREE), omMetadataManager);
    }
    List<RenameKeysMap> renameKeys = new ArrayList<>();
    List<String> names = new ArrayList<>();
    for (String pair : pairs.split(" ")) {
      String[] fromTo = pair.split(">");
      renameKeys.add(RenameKeysMap.newBuilder().setFromKeyName(fromTo[0]).setToKeyName(fromTo[1]).build());
      names.addAll(Arrays.asList(fromTo));
    }
    OMRequest omRequest = createRenameKeyRequest(renameKeys);

    OMException rejected = null;
    try {
      OMRequest checked = new OMKeysRenameRequest(omRequest, getBucketLayout()).preExecute(ozoneManager);
      try (BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation()) {
        new OMKeysRenameRequest(checked, getBucketLayout()).validateAndUpdateCache(ozoneManager, 100L)
            .checkAndUpdateDB(omMetadataManager, batchOperation);
        omMetadataManager.getStore().commitBatchOperation(batchOperation);
      }
    } catch (OMException e) {
      rejected = e;
    }

    Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable(getBucketLayout());
    for (String name : names) {
      String dbKey = omMetadataManager.getOzoneKey(volumeName, bucketName, name);
      assertThat(objectId(keyTable.getSkipCache(dbKey))).as(name).isEqualTo(objectId(keyTable.get(dbKey)));
    }
    assertThat(rejected == null ? null : rejected.getResult()).isEqualTo(expectRejected ? INVALID_REQUEST : null);
  }

  /**
   * The check rejects a transaction whose pairs share a key even when the pair sharing it would not be applied:
   * with only a present, b to c fails for its missing source and a to b succeeds, a partial rename before the check.
   * A transaction of that shape already in the Raft log is still applied as a partial rename.
   */
  @Test
  public void testPairsSharingKeyRejectedInsteadOfPartialRename() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, "a", 0L,
        RatisReplicationConfig.getInstance(THREE), omMetadataManager);
    OMRequest omRequest = createRenameKeyRequest(Arrays.asList(
        RenameKeysMap.newBuilder().setFromKeyName("b").setToKeyName("c").build(),
        RenameKeysMap.newBuilder().setFromKeyName("a").setToKeyName("b").build()));

    OMException e = assertThrows(OMException.class,
        () -> new OMKeysRenameRequest(omRequest, getBucketLayout()).preExecute(ozoneManager));
    assertThat(e.getResult()).isEqualTo(INVALID_REQUEST);

    OMClientResponse response = new OMKeysRenameRequest(omRequest, getBucketLayout())
        .validateAndUpdateCache(ozoneManager, 100L);
    assertThat(response.getOMResponse().getStatus()).isEqualTo(OzoneManagerProtocolProtos.Status.PARTIAL_RENAME);
    Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable(getBucketLayout());
    assertThat(keyTable.get(omMetadataManager.getOzoneKey(volumeName, bucketName, "a"))).isNull();
    assertThat(keyTable.get(omMetadataManager.getOzoneKey(volumeName, bucketName, "b"))).isNotNull();
  }

  private static Long objectId(OmKeyInfo keyInfo) {
    return keyInfo == null ? null : keyInfo.getObjectID();
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

    return createRenameKeyRequest(renameKeyList);
  }

  private OMRequest createRenameKeyRequest(List<RenameKeysMap> renameKeyList) {
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
