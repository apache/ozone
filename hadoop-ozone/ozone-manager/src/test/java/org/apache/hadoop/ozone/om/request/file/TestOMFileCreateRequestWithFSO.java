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

package org.apache.hadoop.ozone.om.request.file;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests OMFileCreateRequest - prefix layout.
 */
public class TestOMFileCreateRequestWithFSO extends TestOMFileCreateRequest {

  @Override
  @Test
  public void testValidateAndUpdateCacheWithNonRecursive() throws Exception {
    testNonRecursivePath(UUID.randomUUID().toString(), false, false, false);
    testNonRecursivePath("a/b", false, false, true);
    assertEquals(0, omMetrics.getNumKeys(), "Invalid metrics value");

    // Create parent dirs for the path
    OMRequestTestUtils.addParentsToDirTable(volumeName, bucketName,
            "a/b/c", omMetadataManager);
    String fileNameD = "d";
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/b/c/" + fileNameD, 0L, RatisReplicationConfig.getInstance(ONE), omMetadataManager);

    // cannot create file if directory of same name exists
    testNonRecursivePath("a/b/c", false, false, true);

    // Delete child key but retain path "a/b/ in the key table
    OmDirectoryInfo dirPathC = getDirInfo("a/b/c");
    assertNotNull(dirPathC, "Failed to find dir path: a/b/c");
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    String dbFileD = omMetadataManager.getOzonePathKey(volumeId, bucketId,
            dirPathC.getObjectID(), fileNameD);
    omMetadataManager.getKeyTable(getBucketLayout()).delete(dbFileD);
    omMetadataManager.getKeyTable(getBucketLayout()).delete(dirPathC.getPath());

    // can create non-recursive because parents already exist.
    testNonRecursivePath("a/b/e", false, false, false);
  }

  @Override
  @Test
  public void testValidateAndUpdateCacheWithNamespaceQuotaExceeded()
      throws Exception {
    OMRequest omRequest = createFileRequest(volumeName, bucketName,
        "/test/a1/a2", ONE,
        HddsProtos.ReplicationType.RATIS, false, true);

    // create bucket with quota limit 1
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(getBucketLayout())
            .setQuotaInNamespace(1));
    
    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);
    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    omFileCreateRequest = getOMFileCreateRequest(modifiedOmRequest);
    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertSame(omFileCreateResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED);
  }

  @Override
  @Test
  public void testValidateAndUpdateCacheWithRecursiveAndOverWrite()
          throws Exception {
    String key = "c/d/e/f";
    // Should be able to create file even if parent directories does not exist
    testNonRecursivePath(key, false, true, false);
    assertEquals(3, omMetrics.getNumKeys(), "Invalid metrics value");
    assertEquals(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName))
        .getUsedNamespace(), omMetrics.getNumKeys());

    // Add the key to key table
    OmDirectoryInfo omDirInfo = getDirInfo("c/d/e");
    OmKeyInfo omKeyInfo =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, key, RatisReplicationConfig.getInstance(ONE))
            .setObjectID(omDirInfo.getObjectID() + 10)
            .setParentObjectID(omDirInfo.getObjectID())
            .setUpdateID(100)
            .build();
    OMRequestTestUtils.addFileToKeyTable(false, false,
            "f", omKeyInfo, -1,
            omDirInfo.getObjectID() + 10, omMetadataManager);

    // Even if key exists, should be able to create file as overwrite is set
    // to true
    testNonRecursivePath(key, true, true, false);
    testNonRecursivePath(key, false, true, true);
  }

  @Override
  @Test
  public void testValidateAndUpdateCacheWithNonRecursiveAndOverWrite()
          throws Exception {
    String parentDir = "c/d/e";
    String fileName = "f";
    String key = parentDir + "/" + fileName;
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    // Create parent dirs for the path
    long parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
        bucketName, parentDir, omMetadataManager);

    // Need to add the path which starts with "c/d/e" to OpenKeyTable as this is
    // non-recursive parent should exist.
    testNonRecursivePath(key, false, false, false);

    OmKeyInfo omKeyInfo =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, key, RatisReplicationConfig.getInstance(ONE))
            .setObjectID(parentId + 1L)
            .setParentObjectID(parentId)
            .setUpdateID(100L)
            .build();
    OMRequestTestUtils.addFileToKeyTable(false, false, fileName, omKeyInfo, -1, 50, omMetadataManager);

    // Even if key exists in KeyTable, should be able to create file as
    // overwrite is set to true
    testNonRecursivePath(key, true, false, false);
    testNonRecursivePath(key, false, false, true);
  }

  @Override
  @Test
  public void testCreateFileInheritParentDefaultAcls()
      throws Exception {
    super.testCreateFileInheritParentDefaultAcls();
  }

  @Test
  public void testValidateAndUpdateCacheWithSnapshotReservedWord()
      throws Exception {
    String[] validPaths = {
        "a/b/" + OM_SNAPSHOT_INDICATOR + "c/d",
        "a/b/c/" + OM_SNAPSHOT_INDICATOR + "/d"
    };
    for (String validPath : validPaths) {
      testNonRecursivePath(validPath, false, true, false);
    }
  }

  @Override
  protected OmKeyInfo verifyPathInOpenKeyTable(String key, long id,
                                             boolean doAssert)
          throws Exception {
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    String[] pathComponents = StringUtils.split(key, '/');
    long parentId = bucketId;
    for (int indx = 0; indx < pathComponents.length; indx++) {
      String pathElement = pathComponents[indx];
      // Reached last component, which is file name
      if (indx == pathComponents.length - 1) {
        String dbOpenFileName = omMetadataManager.getOpenFileName(
                volumeId, bucketId, parentId, pathElement, id);
        OmKeyInfo omKeyInfo =
            omMetadataManager.getOpenKeyTable(getBucketLayout())
                .get(dbOpenFileName);
        if (doAssert) {
          assertNotNull(omKeyInfo, "Invalid key!");
        }
        return omKeyInfo;
      } else {
        // directory
        String dbKey = omMetadataManager.getOzonePathKey(volumeId,
                bucketId, parentId, pathElement);
        OmDirectoryInfo dirInfo =
                omMetadataManager.getDirectoryTable().get(dbKey);
        parentId = dirInfo.getObjectID();
      }
    }
    if (doAssert) {
      fail("Invalid key!");
    }
    return null;
  }

  private OmDirectoryInfo getDirInfo(String key)
          throws Exception {
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    String[] pathComponents = StringUtils.split(key, '/');
    long parentId = bucketId;
    OmDirectoryInfo dirInfo = null;
    for (String pathElement : pathComponents) {
      // Reached last component, which is file name
      // directory
      String dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId, parentId, pathElement);
      dirInfo = omMetadataManager.getDirectoryTable().get(dbKey);
      parentId = dirInfo.getObjectID();
    }
    return dirInfo;
  }

  @Override
  protected OMFileCreateRequest getOMFileCreateRequest(OMRequest omRequest) throws IOException {
    OMFileCreateRequest request = new OMFileCreateRequestWithFSO(omRequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    request.setUGI(UserGroupInformation.getCurrentUser());
    return request;
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
