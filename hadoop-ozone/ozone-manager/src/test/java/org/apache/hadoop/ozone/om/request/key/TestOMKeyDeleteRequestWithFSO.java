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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.OzonePrefixPathImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.security.acl.OzonePrefixPath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests OmKeyDelete request with prefix layout.
 */
public class TestOMKeyDeleteRequestWithFSO extends TestOMKeyDeleteRequest {
  private static final String INTERMEDIATE_DIR = "c/d/";
  private static final String PARENT_DIR = "c/d/e";
  private static final String FILE_NAME = "file1";
  private static final String FILE_KEY = PARENT_DIR + "/" + FILE_NAME;

  @Override
  protected OMKeyDeleteRequest getOmKeyDeleteRequest(
      OMRequest modifiedOmRequest) {
    return new OMKeyDeleteRequestWithFSO(modifiedOmRequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @Override
  protected String addKeyToTable() throws Exception {
    keyName = FILE_KEY; // updated key name

    // Create parent dirs for the path
    long parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
            bucketName, PARENT_DIR, omMetadataManager);

    OmKeyInfo omKeyInfo =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, FILE_KEY, RatisReplicationConfig.getInstance(ONE))
            .setObjectID(parentId + 1L)
            .setParentObjectID(parentId)
            .setUpdateID(100L)
            .build();
    omKeyInfo.setKeyName(FILE_NAME);
    OMRequestTestUtils.addFileToKeyTable(false, false,
        FILE_NAME, omKeyInfo, -1, 50, omMetadataManager);
    final long volumeId = omMetadataManager.getVolumeId(
            omKeyInfo.getVolumeName());
    final long bucketId = omMetadataManager.getBucketId(
            omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());
    return omMetadataManager.getOzonePathKey(
            volumeId, bucketId, omKeyInfo.getParentObjectID(),
            omKeyInfo.getFileName());
  }

  protected String addKeyToDirTable(String volumeName, String bucketName,
                                    String key) throws Exception {
    // Create parent dirs for the path
    long parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
        bucketName, key, omMetadataManager);

    OmKeyInfo omKeyInfo =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, key, RatisReplicationConfig.getInstance(ONE))
            .setObjectID(parentId + 1L)
            .setParentObjectID(parentId)
            .setUpdateID(100L)
            .build();
    omKeyInfo.setKeyName(key);
    return omKeyInfo.getPath();
  }

  @ParameterizedTest
  @ValueSource(strings = {"keyName"})
  @Override
  public void testPreExecute(String testKeyName) throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());
    String ozoneKey = addKeyToTable();
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertNotNull(omKeyInfo);

    doPreExecute(createDeleteKeyRequest());
  }

  @Test
  public void testOzonePrefixPathViewer() throws Exception {
    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    String ozoneKey = addKeyToTable();

    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    // As we added manually to key table.
    assertNotNull(omKeyInfo);

    // OzonePrefixPathImpl on a directory
    OzonePrefixPathImpl ozonePrefixPath = new OzonePrefixPathImpl(volumeName,
        bucketName, "c", keyManager);
    OzoneFileStatus status = ozonePrefixPath.getOzoneFileStatus();
    assertNotNull(status);
    assertEquals("c", status.getTrimmedName());
    assertTrue(status.isDirectory());
    verifyPath(ozonePrefixPath, "c", "c/d");
    verifyPath(ozonePrefixPath, "c/d", "c/d/e");
    verifyPath(ozonePrefixPath, "c/d/e", "c/d/e/file1");

    OMException ome = assertThrows(OMException.class, () -> ozonePrefixPath.getChildren("c/d/e/file1"),
        "Should throw INVALID_KEY_NAME as the given path is a file.");
    assertEquals(OMException.ResultCodes.INVALID_KEY_NAME, ome.getResult());

    // OzonePrefixPathImpl on a file
    OzonePrefixPathImpl ozonePrefixPathFile1 = new OzonePrefixPathImpl(volumeName,
        bucketName, "c/d/e/file1", keyManager);
    status = ozonePrefixPathFile1.getOzoneFileStatus();
    assertNotNull(status);
    assertEquals("c/d/e/file1", status.getTrimmedName());
    assertEquals("c/d/e/file1", status.getKeyInfo().getKeyName());
    assertTrue(status.isFile());
  }

  private void verifyPath(OzonePrefixPath ozonePrefixPath, String pathName,
                          String expectedPath)
      throws IOException {
    Iterator<? extends OzoneFileStatus> pathItr = ozonePrefixPath.getChildren(
        pathName);
    assertTrue(pathItr.hasNext(), "Failed to list keyPaths");
    assertEquals(expectedPath, pathItr.next().getTrimmedName());
    assertThrows(NoSuchElementException.class, () -> pathItr.next(), "Reached end of the list!");
  }

  @Test
  public void testRecursiveAccessCheck() throws Exception {
    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    // Case 1:
    // We create an empty directory structure.
    String parentKey = "x/y/";
    String key = "x/y/z/";
    addKeyToDirTable(volumeName, bucketName, key);

    // Instantiate PrefixPath for complete key.
    OzonePrefixPathImpl pathViewer = new OzonePrefixPathImpl(volumeName,
        bucketName, key, ozoneManager.getKeyManager());

    // 'x/y/z' has no sub-directories or sub files - recursive access check
    // should not be enabled for this case.
    assertFalse(pathViewer.isCheckRecursiveAccess());

    // Instantiate PrefixPath for parent key.
    pathViewer = new OzonePrefixPathImpl(volumeName,
        bucketName, parentKey, ozoneManager.getKeyManager());

    // 'x/y/' has a sub-directory 'z', hence, we should be performing recursive
    // access check.
    assertTrue(pathViewer.isCheckRecursiveAccess());

    // Case 2:
    // We create a directory structure with a file as the leaf node.
    // 'c/d/e/file1'.
    String ozoneKey = addKeyToTable();

    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    // As we added manually to key table.
    assertNotNull(omKeyInfo);

    // Instantiate PrefixPath for parent key 'c/d/'.
    pathViewer = new OzonePrefixPathImpl(volumeName,
        bucketName, INTERMEDIATE_DIR, ozoneManager.getKeyManager());

    // 'c/d' has a sub-directory 'e', hence, we should be performing recursive
    // access check.
    assertTrue(pathViewer.isCheckRecursiveAccess());

    // Instantiate PrefixPath for complete directory structure (without file).
    pathViewer = new OzonePrefixPathImpl(volumeName,
        bucketName, PARENT_DIR, ozoneManager.getKeyManager());

    // 'c/d/e/' has a 'file1' under it, hence, we should be performing recursive
    // access check.
    assertTrue(pathViewer.isCheckRecursiveAccess());

    // Instantiate PrefixPath for complete file1.
    pathViewer = new OzonePrefixPathImpl(volumeName,
        bucketName, FILE_KEY, ozoneManager.getKeyManager());

    // Recursive access check is only enabled for directories, hence should be
    // false for file1.
    assertFalse(pathViewer.isCheckRecursiveAccess());
  }

  @Test
  public void testDeleteDirectoryWithColonInFSOBucket() throws Exception {
    when(ozoneManager.getEnableFileSystemPaths()).thenReturn(true);
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());

    String dirName = "foo:dir/";
    String dirKeyPath = addKeyToDirTable(volumeName, bucketName, dirName);

    long parentObjectID = 0L;
    long dirObjectID = 12345L;
    OmDirectoryInfo omDirectoryInfo = OMRequestTestUtils.createOmDirectoryInfo(dirName, dirObjectID, parentObjectID);
    omMetadataManager.getDirectoryTable().put(dirKeyPath, omDirectoryInfo);

    OmDirectoryInfo storedDirInfo = omMetadataManager.getDirectoryTable().get(dirKeyPath);
    assertNotNull(storedDirInfo);
    assertEquals(dirName, storedDirInfo.getName());
    assertEquals(dirObjectID, storedDirInfo.getObjectID());
    assertEquals(parentObjectID, storedDirInfo.getParentObjectID());

    OMRequest deleteRequest = doPreExecute(createDeleteKeyRequest(dirName));
    OMKeyDeleteRequest omKeyDeleteRequest = getOmKeyDeleteRequest(deleteRequest);
    OMClientResponse response = omKeyDeleteRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK, response.getOMResponse().getStatus());
    assertNull(omMetadataManager.getDirectoryTable().get(dirName));
  }

  private OMRequest createDeleteKeyRequest(String keyPath, boolean recursive) {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setKeyName(keyPath)
        .setRecursive(recursive)
        .build();

    DeleteKeyRequest deleteKeyRequest =
        DeleteKeyRequest.newBuilder().setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder()
        .setDeleteKeyRequest(deleteKeyRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  /**
   * Minimal test to reproduce "Directory Not Empty" bug with Ratis.
   * Tests both checkSubDirectoryExists() and checkSubFileExists() paths.
   * Creates child directory and file, deletes them, then tries to delete parent.
   * This test exposes a Ratis transaction visibility issue where deleted
   * entries are in cache but not yet flushed to DB via double buffer.
   */
  @Test
  public void testDeleteParentAfterChildDeleted() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());

    String parentDir = "parent";
    long parentId = OMRequestTestUtils.addParentsToDirTable(volumeName, bucketName, parentDir, omMetadataManager);

    // Create a child directory (tests checkSubDirectoryExists path)
    OMRequestTestUtils.addParentsToDirTable(volumeName, bucketName, parentDir + "/childDir", omMetadataManager);

    // Create a child file (tests checkSubFileExists path)
    String fileName = "childFile";
    OmKeyInfo fileInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
        bucketName, parentDir + "/" + fileName, RatisReplicationConfig.getInstance(ONE))
        .setObjectID(parentId + 2)
        .setParentObjectID(parentId)
        .setUpdateID(50L)
        .build();
    fileInfo.setKeyName(fileName);
    OMRequestTestUtils.addFileToKeyTable(false, false, fileName, fileInfo, -1, 50, omMetadataManager);

    // Delete the child directory
    long txnId = 1000L;
    OMRequest deleteChildDirRequest = doPreExecute(createDeleteKeyRequest(parentDir + "/childDir", true));
    OMKeyDeleteRequest deleteChildDirKeyRequest = getOmKeyDeleteRequest(deleteChildDirRequest);
    OMClientResponse deleteChildDirResponse = deleteChildDirKeyRequest.validateAndUpdateCache(ozoneManager, txnId++);
    assertEquals(OzoneManagerProtocolProtos.Status.OK, deleteChildDirResponse.getOMResponse().getStatus(),
        "Child directory delete should succeed");

    // Delete the child file
    OMRequest deleteChildFileRequest = doPreExecute(createDeleteKeyRequest(parentDir + "/" + fileName, false));
    OMKeyDeleteRequest deleteChildFileKeyRequest = getOmKeyDeleteRequest(deleteChildFileRequest);
    OMClientResponse deleteChildFileResponse = deleteChildFileKeyRequest.validateAndUpdateCache(ozoneManager, txnId++);
    assertEquals(OzoneManagerProtocolProtos.Status.OK, deleteChildFileResponse.getOMResponse().getStatus(),
        "Child file delete should succeed");

    // Try to delete parent (should succeed but fails without fix)
    OMRequest deleteParentRequest = doPreExecute(createDeleteKeyRequest(parentDir, false));
    OMKeyDeleteRequest deleteParentKeyRequest = getOmKeyDeleteRequest(deleteParentRequest);
    OMClientResponse response = deleteParentKeyRequest.validateAndUpdateCache(ozoneManager, txnId);

    // This should succeed after the fix
    assertEquals(OzoneManagerProtocolProtos.Status.OK, response.getOMResponse().getStatus(),
        "Parent delete should succeed after children deleted");
  }
}
