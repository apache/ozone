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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DIRECTORY_NOT_EMPTY;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyError;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RequestSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Class tests OMKeysDeleteRequestWithFSO.
 */
public class TestOMKeysDeleteRequestWithFSO extends TestOMKeysDeleteRequest {

  private static final int KEY_COUNT = 3;

  @Override
  @ParameterizedTest
  @MethodSource("requestSourceType")
  public void testKeysDeleteRequest(RequestSource sourceType) throws Exception {

    createPreRequisites(sourceType);
    OmKeysDeleteRequestWithFSO omKeysDeleteRequest =
        new OmKeysDeleteRequestWithFSO(getOmRequest(),
            getBucketLayout());
    checkDeleteKeysResponse(omKeysDeleteRequest, sourceType);

  }

  @Override
  @ParameterizedTest
  @MethodSource("requestSourceType")
  public void testKeysDeleteRequestFail(RequestSource sourceType) throws Exception {
    createPreRequisites(sourceType);
    setOmRequest(getOmRequest().toBuilder().setDeleteKeysRequest(
        OzoneManagerProtocolProtos.DeleteKeysRequest.newBuilder()
            .setSourceType(sourceType)
            .setDeleteKeys(OzoneManagerProtocolProtos.DeleteKeyArgs
                .newBuilder()
                .setBucketName(bucketName)
                .setVolumeName(volumeName)
                .addAllKeys(getDeleteKeyList())
                .addKeys("dummy")))
        .build());

    OmKeysDeleteRequestWithFSO omKeysDeleteRequest =
        new OmKeysDeleteRequestWithFSO(getOmRequest(),
            getBucketLayout());
    checkDeleteKeysResponseForFailure(omKeysDeleteRequest, Status.PARTIAL_DELETE, sourceType);
  }

  @Test
  @Override
  public void testUpdateIDCountMatchKeyCount() throws Exception {

    createPreRequisites();

    // Add a key which not exist, which causes batch delete to fail.
    // updateID of every deleteKeyList is same 1L.
    List<Long> updateIDList = new ArrayList<>(KEY_COUNT);
    updateIDList.forEach(id -> id = 1L);
    OMRequest omRequest = getOmRequest().toBuilder()
        .setDeleteKeysRequest(DeleteKeysRequest.newBuilder()
            .setDeleteKeys(DeleteKeyArgs.newBuilder()
                .setBucketName(bucketName).setVolumeName(volumeName)
                .addAllKeys(getDeleteKeyList()).addAllUpdateIDs(updateIDList))).build();

    OmKeysDeleteRequestWithFSO omKeysDeleteRequest =
        new OmKeysDeleteRequestWithFSO(omRequest, getBucketLayout());
    checkDeleteKeysResponse(omKeysDeleteRequest);
  }

  @Test
  public void testDuplicatePathsReleaseQuotaOnce() throws Exception {
    createPreRequisites();
    DeleteKeyArgs keys = getOmRequest().getDeleteKeysRequest().getDeleteKeys();
    OMRequest request = getOmRequest().toBuilder()
        .setDeleteKeysRequest(getOmRequest().getDeleteKeysRequest().toBuilder()
            .setDeleteKeys(keys.toBuilder().clearKeys()
                .addKeys("dir0/file0").addKeys("dir0//file0").addKeys("/dir0/file0")
                .addKeys("dir1/").addKeys("/dir1"))).build();
    checkDuplicateKeysReleaseQuotaOnce(request, 2);
  }

  @Test
  public void testNonRecursiveKeepsNonEmptyDirectories() throws Exception {
    addFile("a/b", "file");

    OMResponse response = deleteNonRecursive("a/", "a/b/");

    assertEquals(Status.PARTIAL_DELETE, response.getStatus());
    DeleteKeysResponse deleteKeysResponse = response.getDeleteKeysResponse();
    assertThat(deleteKeysResponse.getUnDeletedKeys().getKeysList()).containsExactlyInAnyOrder("a/", "a/b/");
    assertThat(deleteKeysResponse.getErrorsList())
        .extracting(DeleteKeyError::getKey, DeleteKeyError::getErrorCode)
        .containsExactlyInAnyOrder(tuple("a/", DIRECTORY_NOT_EMPTY.name()), tuple("a/b/", DIRECTORY_NOT_EMPTY.name()));
    for (String key : Arrays.asList("a/", "a/b/", "a/b/file")) {
      assertNotNull(getFileStatus(key), key);
    }
  }

  @Test
  public void testNonRecursiveDeletesDirectoriesEmptiedByTheRequest() throws Exception {
    addFile("a/b", "file");

    // A listing returns each directory before its contents.
    OMResponse response = deleteNonRecursive("a/", "a/b/", "a/b/file");

    assertEquals(Status.OK, response.getStatus());
    for (String key : Arrays.asList("a/", "a/b/", "a/b/file")) {
      assertNull(getFileStatus(key), key);
    }
  }

  /**
   * Adds a file and its parent directories. Unlike {@link #createPreRequisites()}, which gives all its top-level
   * directories the same object ID, every entry here has its own, so a directory's children are only its own.
   */
  private void addFile(String parentDir, String fileName) throws Exception {
    when(ozoneManager.getDefaultReplicationConfig()).thenReturn(RatisReplicationConfig.getInstance(ONE));
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());
    long parentId = OMRequestTestUtils.addParentsToDirTable(volumeName, bucketName, parentDir, omMetadataManager);
    OmKeyInfo file = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, parentDir + "/" + fileName,
            RatisReplicationConfig.getInstance(ONE))
        .setObjectID(parentId + 1L)
        .setParentObjectID(parentId)
        .build();
    file.setKeyName(fileName);
    OMRequestTestUtils.addFileToKeyTable(false, false, fileName, file, -1, 50, omMetadataManager);
  }

  private OMResponse deleteNonRecursive(String... keys) throws Exception {
    OMRequest request = OMRequest.newBuilder().setClientId(UUID.randomUUID().toString()).setCmdType(DeleteKeys)
        .setDeleteKeysRequest(DeleteKeysRequest.newBuilder()
            .setRecursive(false)
            .setDeleteKeys(DeleteKeyArgs.newBuilder()
                .setVolumeName(volumeName).setBucketName(bucketName).addAllKeys(Arrays.asList(keys))))
        .build();
    OMClientResponse response = getOmKeysDeleteRequest(request).validateAndUpdateCache(ozoneManager, 100L);
    try (BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(omMetadataManager, batchOperation);
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }
    return response.getOMResponse();
  }

  private OzoneFileStatus getFileStatus(String keyName) throws IOException {
    return OMFileRequest.getOMKeyInfoIfExists(omMetadataManager, volumeName, bucketName, keyName, 0,
        RatisReplicationConfig.getInstance(ONE));
  }

  @Override
  protected void createPreRequisites() throws Exception {
    createPreRequisites(RequestSource.USER);
  }

  @Override
  protected OMKeysDeleteRequest getOmKeysDeleteRequest(OMRequest request) {
    return new OmKeysDeleteRequestWithFSO(request, getBucketLayout());
  }

  @Override
  protected long expectedNamespaceReleasedOnPartialDelete() {
    // Only the leaf files sit under the shared parent directory created by createPreRequisites.
    return 3L;
  }

  @Override
  protected void createPreRequisites(RequestSource sourceType) throws Exception {
    // Directories are read back through getOMKeyInfoIfExists, which stamps them with the OM
    // default; without it they cannot be written to the deleted directory table.
    when(ozoneManager.getDefaultReplicationConfig())
        .thenReturn(RatisReplicationConfig.getInstance(ONE));
    setDeleteKeyList(new ArrayList<>());
    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils
        .addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager,
            getBucketLayout());

    OzoneManagerProtocolProtos.DeleteKeyArgs.Builder deleteKeyArgs =
        OzoneManagerProtocolProtos.DeleteKeyArgs.newBuilder()
            .setBucketName(bucketName).setVolumeName(volumeName);

    // 3 dirs with files inside each dir

    for (int i = 0; i < KEY_COUNT; i++) {
      String dir = "dir" + i;
      String file = "file" + i;
      long parentId = OMRequestTestUtils
          .addParentsToDirTable(volumeName, bucketName, dir, omMetadataManager);

      OmKeyInfo omKeyInfo =
          OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, dir + "/" + file,
                  RatisReplicationConfig.getInstance(ONE))
              .setObjectID(parentId + 1L)
              .setParentObjectID(parentId)
              .setUpdateID(100L)
              .build();
      omKeyInfo.setKeyName(file);
      OMRequestTestUtils
          .addFileToKeyTable(false, false, file, omKeyInfo, -1, 50,
              omMetadataManager);

      // adding only top level dirs is enough.
      deleteKeyArgs.addKeys(dir + "/");
      getDeleteKeyList().add(dir + "/");
    }

    setOmRequest(OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString()).setCmdType(DeleteKeys)
        .setDeleteKeysRequest(
            OzoneManagerProtocolProtos.DeleteKeysRequest
                .newBuilder()
                .setSourceType(sourceType)
                .setDeleteKeys(deleteKeyArgs)
                .build())
        .build());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
