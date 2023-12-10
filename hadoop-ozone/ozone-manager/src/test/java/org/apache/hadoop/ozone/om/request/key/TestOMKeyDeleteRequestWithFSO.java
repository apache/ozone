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
import org.apache.hadoop.ozone.om.OzonePrefixPathImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.security.acl.OzonePrefixPath;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
            OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, FILE_KEY,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE,
                    parentId + 1,
                    parentId, 100, Time.now());
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
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, key,
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE,
            parentId + 1,
            parentId, 100, Time.now());
    omKeyInfo.setKeyName(key);
    return omKeyInfo.getPath();
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
    Assertions.assertNotNull(omKeyInfo);

    // OzonePrefixPathImpl on a directory
    OzonePrefixPathImpl ozonePrefixPath = new OzonePrefixPathImpl(volumeName,
        bucketName, "c", keyManager);
    OzoneFileStatus status = ozonePrefixPath.getOzoneFileStatus();
    Assertions.assertNotNull(status);
    Assertions.assertEquals("c", status.getTrimmedName());
    Assertions.assertTrue(status.isDirectory());
    verifyPath(ozonePrefixPath, "c", "c/d");
    verifyPath(ozonePrefixPath, "c/d", "c/d/e");
    verifyPath(ozonePrefixPath, "c/d/e", "c/d/e/file1");

    try {
      ozonePrefixPath.getChildren("c/d/e/file1");
      Assertions.fail("Should throw INVALID_KEY_NAME as the given " +
          "path is a file.");
    } catch (OMException ome) {
      Assertions.assertEquals(OMException.ResultCodes.INVALID_KEY_NAME,
          ome.getResult());
    }

    // OzonePrefixPathImpl on a file
    ozonePrefixPath = new OzonePrefixPathImpl(volumeName,
        bucketName, "c/d/e/file1", keyManager);
    status = ozonePrefixPath.getOzoneFileStatus();
    Assertions.assertNotNull(status);
    Assertions.assertEquals("c/d/e/file1", status.getTrimmedName());
    Assertions.assertEquals("c/d/e/file1", status.getKeyInfo().getKeyName());
    Assertions.assertTrue(status.isFile());
  }

  private void verifyPath(OzonePrefixPath ozonePrefixPath, String pathName,
                          String expectedPath)
      throws IOException {
    Iterator<? extends OzoneFileStatus> pathItr = ozonePrefixPath.getChildren(
        pathName);
    Assertions.assertTrue(pathItr.hasNext(), "Failed to list keyPaths");
    Assertions.assertEquals(expectedPath, pathItr.next().getTrimmedName());
    try {
      pathItr.next();
      Assertions.fail("Reached end of the list!");
    } catch (NoSuchElementException nse) {
      // expected
    }
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
    Assertions.assertFalse(pathViewer.isCheckRecursiveAccess());

    // Instantiate PrefixPath for parent key.
    pathViewer = new OzonePrefixPathImpl(volumeName,
        bucketName, parentKey, ozoneManager.getKeyManager());

    // 'x/y/' has a sub-directory 'z', hence, we should be performing recursive
    // access check.
    Assertions.assertTrue(pathViewer.isCheckRecursiveAccess());

    // Case 2:
    // We create a directory structure with a file as the leaf node.
    // 'c/d/e/file1'.
    String ozoneKey = addKeyToTable();

    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    // As we added manually to key table.
    Assertions.assertNotNull(omKeyInfo);

    // Instantiate PrefixPath for parent key 'c/d/'.
    pathViewer = new OzonePrefixPathImpl(volumeName,
        bucketName, INTERMEDIATE_DIR, ozoneManager.getKeyManager());

    // 'c/d' has a sub-directory 'e', hence, we should be performing recursive
    // access check.
    Assertions.assertTrue(pathViewer.isCheckRecursiveAccess());

    // Instantiate PrefixPath for complete directory structure (without file).
    pathViewer = new OzonePrefixPathImpl(volumeName,
        bucketName, PARENT_DIR, ozoneManager.getKeyManager());

    // 'c/d/e/' has a 'file1' under it, hence, we should be performing recursive
    // access check.
    Assertions.assertTrue(pathViewer.isCheckRecursiveAccess());

    // Instantiate PrefixPath for complete file1.
    pathViewer = new OzonePrefixPathImpl(volumeName,
        bucketName, FILE_KEY, ozoneManager.getKeyManager());

    // Recursive access check is only enabled for directories, hence should be
    // false for file1.
    Assertions.assertFalse(pathViewer.isCheckRecursiveAccess());
  }
}
