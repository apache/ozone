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

package org.apache.hadoop.ozone.om.request.file;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Tests OMFileCreateRequest - prefix layout.
 */
public class TestOMFileCreateRequestWithFSO extends TestOMFileCreateRequest {

  @Test
  public void testValidateAndUpdateCacheWithNonRecursive() throws Exception {
    testNonRecursivePath(UUID.randomUUID().toString(), false, false, false);
    testNonRecursivePath("a/b", false, false, true);
    Assert.assertEquals("Invalid metrics value", 0, omMetrics.getNumKeys());

    // Create parent dirs for the path
    TestOMRequestUtils.addParentsToDirTable(volumeName, bucketName,
            "a/b/c", omMetadataManager);
    String fileNameD = "d";
    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName,
            "a/b/c/" + fileNameD, 0L, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, omMetadataManager);

    // cannot create file if directory of same name exists
    testNonRecursivePath("a/b/c", false, false, true);

    // Delete child key but retain path "a/b/ in the key table
    OmDirectoryInfo dirPathC = getDirInfo("a/b/c");
    Assert.assertNotNull("Failed to find dir path: a/b/c", dirPathC);
    String dbFileD = omMetadataManager.getOzonePathKey(
            dirPathC.getObjectID(), fileNameD);
    omMetadataManager.getKeyTable().delete(dbFileD);
    omMetadataManager.getKeyTable().delete(dirPathC.getPath());

    // can create non-recursive because parents already exist.
    testNonRecursivePath("a/b/e", false, false, false);
  }

  @Test
  public void testValidateAndUpdateCacheWithRecursiveAndOverWrite()
          throws Exception {
    String key = "c/d/e/f";
    // Should be able to create file even if parent directories does not exist
    testNonRecursivePath(key, false, true, false);
    Assert.assertEquals("Invalid metrics value", 3, omMetrics.getNumKeys());

    // Add the key to key table
    OmDirectoryInfo omDirInfo = getDirInfo("c/d/e");
    OmKeyInfo omKeyInfo =
            TestOMRequestUtils.createOmKeyInfo(volumeName, bucketName, key,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE,
                    omDirInfo.getObjectID() + 10,
                    omDirInfo.getObjectID(), 100, Time.now());
    TestOMRequestUtils.addFileToKeyTable(false, false,
            "f", omKeyInfo, -1,
            omDirInfo.getObjectID() + 10, omMetadataManager);

    // Even if key exists, should be able to create file as overwrite is set
    // to true
    testNonRecursivePath(key, true, true, false);
    testNonRecursivePath(key, false, true, true);
  }

  @Test
  public void testValidateAndUpdateCacheWithNonRecursiveAndOverWrite()
          throws Exception {
    String parentDir = "c/d/e";
    String fileName = "f";
    String key = parentDir + "/" + fileName;
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);
    // Create parent dirs for the path
    long parentId = TestOMRequestUtils.addParentsToDirTable(volumeName,
            bucketName, parentDir, omMetadataManager);

    // Need to add the path which starts with "c/d/e" to OpenKeyTable as this is
    // non-recursive parent should exist.
    testNonRecursivePath(key, false, false, false);

    OmKeyInfo omKeyInfo =
            TestOMRequestUtils.createOmKeyInfo(volumeName, bucketName, key,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE,
                    parentId + 1,
                    parentId, 100, Time.now());
    TestOMRequestUtils.addFileToKeyTable(false, false,
            fileName, omKeyInfo, -1, 50, omMetadataManager);

    // Even if key exists in KeyTable, should be able to create file as
    // overwrite is set to true
    testNonRecursivePath(key, true, false, false);
    testNonRecursivePath(key, false, false, true);
  }

  @Override
  protected OmKeyInfo verifyPathInOpenKeyTable(String key, long id,
                                             boolean doAssert)
          throws Exception {
    long bucketId = TestOMRequestUtils.getBucketId(volumeName, bucketName,
            omMetadataManager);
    String[] pathComponents = StringUtils.split(key, '/');
    long parentId = bucketId;
    for (int indx = 0; indx < pathComponents.length; indx++) {
      String pathElement = pathComponents[indx];
      // Reached last component, which is file name
      if (indx == pathComponents.length - 1) {
        String dbOpenFileName = omMetadataManager.getOpenFileName(
                parentId, pathElement, id);
        OmKeyInfo omKeyInfo =
            omMetadataManager.getOpenKeyTable(getBucketLayout())
                .get(dbOpenFileName);
        if (doAssert) {
          Assert.assertNotNull("Invalid key!", omKeyInfo);
        }
        return omKeyInfo;
      } else {
        // directory
        String dbKey = omMetadataManager.getOzonePathKey(parentId,
                pathElement);
        OmDirectoryInfo dirInfo =
                omMetadataManager.getDirectoryTable().get(dbKey);
        parentId = dirInfo.getObjectID();
      }
    }
    if (doAssert) {
      Assert.fail("Invalid key!");
    }
    return null;
  }

  private OmDirectoryInfo getDirInfo(String key)
          throws Exception {
    long bucketId = TestOMRequestUtils.getBucketId(volumeName, bucketName,
            omMetadataManager);
    String[] pathComponents = StringUtils.split(key, '/');
    long parentId = bucketId;
    OmDirectoryInfo dirInfo = null;
    for (int indx = 0; indx < pathComponents.length; indx++) {
      String pathElement = pathComponents[indx];
      // Reached last component, which is file name
      // directory
      String dbKey = omMetadataManager.getOzonePathKey(parentId,
              pathElement);
      dirInfo =
              omMetadataManager.getDirectoryTable().get(dbKey);
      parentId = dirInfo.getObjectID();
    }
    return dirInfo;
  }

  @NotNull
  @Override
  protected OzoneConfiguration getOzoneConfiguration() {
    OzoneConfiguration config = super.getOzoneConfiguration();
    // Metadata Layout prefix will be set while invoking OzoneManager#start()
    // and its not invoked in this test. Hence it is explicitly setting
    // this configuration to populate prefix tables.
    OzoneManagerRatisUtils.setBucketFSOptimized(true);
    return config;
  }

  @Override
  protected OMFileCreateRequest getOMFileCreateRequest(OMRequest omRequest) {
    return new OMFileCreateRequestWithFSO(omRequest);
  }

  @Override
  protected BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
