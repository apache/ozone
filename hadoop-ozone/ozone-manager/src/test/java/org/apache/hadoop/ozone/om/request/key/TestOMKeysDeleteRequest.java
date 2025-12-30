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
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyError;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RequestSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Class tests OMKeysDeleteRequest.
 */
public class TestOMKeysDeleteRequest extends TestOMKeyRequest {

  private List<String> deleteKeyList;
  private OMRequest omRequest;
  private static final int KEY_COUNT = 10;

  public static Collection<Object[]> requestSourceType() {
    return Arrays.asList(
        new Object[]{RequestSource.USER},
        new Object[]{RequestSource.LIFECYCLE},
        new Object[]{RequestSource.TRASH});
  }

  @ParameterizedTest
  @MethodSource("requestSourceType")
  public void testKeysDeleteRequest(RequestSource sourceType) throws Exception {

    createPreRequisites(sourceType);

    OMKeysDeleteRequest omKeysDeleteRequest =
        new OMKeysDeleteRequest(omRequest, getBucketLayout());
    checkDeleteKeysResponse(omKeysDeleteRequest, sourceType);
  }

  protected void checkDeleteKeysResponse(
      OMKeysDeleteRequest omKeysDeleteRequest) throws java.io.IOException {
    checkDeleteKeysResponse(omKeysDeleteRequest, RequestSource.USER);
  }

  protected void checkDeleteKeysResponse(
      OMKeysDeleteRequest omKeysDeleteRequest, RequestSource sourceType) throws java.io.IOException {
    OMClientResponse omClientResponse =
        omKeysDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertTrue(omClientResponse.getOMResponse().getSuccess());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    assertTrue(omClientResponse.getOMResponse()
        .getDeleteKeysResponse().getStatus());
    DeleteKeyArgs unDeletedKeys =
        omClientResponse.getOMResponse().getDeleteKeysResponse()
            .getUnDeletedKeys();
    assertEquals(0, unDeletedKeys.getKeysCount());
    List<DeleteKeyError> keyErrors =  omClientResponse.getOMResponse().getDeleteKeysResponse()
        .getErrorsList();
    assertEquals(0, keyErrors.size());

    // Check all keys are deleted.
    for (String deleteKey : deleteKeyList) {
      assertNull(omMetadataManager.getKeyTable(getBucketLayout())
          .get(omMetadataManager.getOzoneKey(volumeName, bucketName,
              deleteKey)));
    }
    switch (sourceType) {
    case USER:
      assertEquals(deleteKeyList.size(), ozoneManager.getMetrics().getNumKeyDeletes());
      break;
    case LIFECYCLE:
      assertEquals(deleteKeyList.size(), ozoneManager.getMetrics().getNumKeyLifecycleDeletes());
      break;
    case TRASH:
      assertEquals(deleteKeyList.size(), ozoneManager.getMetrics().getNumKeyTrashDeletes());
      break;
    default:
      break;
    }
  }

  @ParameterizedTest
  @MethodSource("requestSourceType")
  public void testKeysDeleteRequestFail(RequestSource sourceType) throws Exception {

    createPreRequisites(sourceType);

    // Add a key which not exist, which causes batch delete to fail.

    omRequest = omRequest.toBuilder()
            .setDeleteKeysRequest(DeleteKeysRequest.newBuilder()
                .setSourceType(sourceType)
                .setDeleteKeys(DeleteKeyArgs.newBuilder()
                .setBucketName(bucketName).setVolumeName(volumeName)
                    .addAllKeys(deleteKeyList).addKeys("dummy"))).build();

    OMKeysDeleteRequest omKeysDeleteRequest =
        new OMKeysDeleteRequest(omRequest, getBucketLayout());
    checkDeleteKeysResponseForFailure(omKeysDeleteRequest, Status.PARTIAL_DELETE, sourceType);
  }

  @ParameterizedTest
  @MethodSource("requestSourceType")
  public void testUpdateIDCountNoMatchKeyCount() throws Exception {

    createPreRequisites();

    // Add a key which not exist, which causes batch delete to fail.

    omRequest = omRequest.toBuilder()
        .setDeleteKeysRequest(DeleteKeysRequest.newBuilder()
            .setDeleteKeys(DeleteKeyArgs.newBuilder()
                .setBucketName(bucketName).setVolumeName(volumeName)
                .addAllKeys(deleteKeyList).addUpdateIDs(1000))).build();

    OMKeysDeleteRequest omKeysDeleteRequest =
        new OMKeysDeleteRequest(omRequest, getBucketLayout());
    checkDeleteKeysResponseForFailure(omKeysDeleteRequest, Status.INVALID_REQUEST);
  }

  @Test
  public void testUpdateIDCountMatchKeyCount() throws Exception {

    createPreRequisites();

    // Add a key which not exist, which causes batch delete to fail.
    // updateID of every deleteKeyList is same 1L.
    List<Long> updateIDList = new ArrayList<>(KEY_COUNT);
    updateIDList.forEach(id -> id = 1L);
    omRequest = omRequest.toBuilder()
        .setDeleteKeysRequest(DeleteKeysRequest.newBuilder()
            .setDeleteKeys(DeleteKeyArgs.newBuilder()
                .setBucketName(bucketName).setVolumeName(volumeName)
                .addAllKeys(deleteKeyList).addAllUpdateIDs(updateIDList))).build();

    OMKeysDeleteRequest omKeysDeleteRequest =
        new OMKeysDeleteRequest(omRequest, getBucketLayout());
    checkDeleteKeysResponse(omKeysDeleteRequest);
  }

  protected void checkDeleteKeysResponseForFailure(
      OMKeysDeleteRequest omKeysDeleteRequest, Status failureStatus) throws IOException {
    checkDeleteKeysResponseForFailure(omKeysDeleteRequest, failureStatus, RequestSource.USER);
  }

  protected void checkDeleteKeysResponseForFailure(
      OMKeysDeleteRequest omKeysDeleteRequest, Status failureStatus, RequestSource sourceType)
      throws java.io.IOException {
    OMClientResponse omClientResponse =
        omKeysDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertFalse(omClientResponse.getOMResponse().getSuccess());
    assertEquals(failureStatus, omClientResponse.getOMResponse().getStatus());

    assertFalse(omClientResponse.getOMResponse()
        .getDeleteKeysResponse().getStatus());

    // Check keys are deleted and in response check unDeletedKey.
    if (failureStatus != Status.INVALID_REQUEST) {
      for (String deleteKey : deleteKeyList) {
        assertNull(omMetadataManager.getKeyTable(getBucketLayout())
            .get(omMetadataManager.getOzoneKey(volumeName, bucketName,
                deleteKey)));
      }

      DeleteKeyArgs unDeletedKeys = omClientResponse.getOMResponse()
          .getDeleteKeysResponse().getUnDeletedKeys();
      assertEquals(1,
          unDeletedKeys.getKeysCount());
      List<DeleteKeyError> keyErrors = omClientResponse.getOMResponse().getDeleteKeysResponse()
          .getErrorsList();
      assertEquals(1, keyErrors.size());
      assertEquals("dummy", unDeletedKeys.getKeys(0));
      switch (sourceType) {
      case USER:
        assertEquals(1, ozoneManager.getMetrics().getNumKeyDeletesFails());
        break;
      case LIFECYCLE:
        assertEquals(1, ozoneManager.getMetrics().getNumKeyLifecycleDeleteFails());
        break;
      case TRASH:
        assertEquals(1, ozoneManager.getMetrics().getNumKeyTrashDeleteFails());
        break;
      default:
        break;
      }

    }
  }

  protected void createPreRequisites() throws Exception {
    createPreRequisites(RequestSource.USER);
  }

  protected void createPreRequisites(RequestSource sourceType) throws Exception {

    deleteKeyList = new ArrayList<>();
    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    DeleteKeyArgs.Builder deleteKeyArgs = DeleteKeyArgs.newBuilder()
        .setBucketName(bucketName).setVolumeName(volumeName);

    // Create 10 keys
    String parentDir = "/user";
    String key;


    for (int i = 0; i < KEY_COUNT; i++) {
      key = parentDir.concat("/key" + i);
      OMRequestTestUtils.addKeyToTableCache(volumeName, bucketName,
          parentDir.concat("/key" + i), RatisReplicationConfig.getInstance(THREE), omMetadataManager);
      deleteKeyArgs.addKeys(key);
      deleteKeyList.add(key);
    }

    omRequest =
        OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
            .setCmdType(DeleteKeys)
            .setDeleteKeysRequest(DeleteKeysRequest.newBuilder()
                .setDeleteKeys(deleteKeyArgs)
                .setSourceType(sourceType)
                .build()
            ).build();
  }

  public List<String> getDeleteKeyList() {
    return deleteKeyList;
  }

  public void setDeleteKeyList(List<String> deleteKeyList) {
    this.deleteKeyList = deleteKeyList;
  }

  public OMRequest getOmRequest() {
    return omRequest;
  }

  public void setOmRequest(OMRequest omRequest) {
    this.omRequest = omRequest;
  }
}
