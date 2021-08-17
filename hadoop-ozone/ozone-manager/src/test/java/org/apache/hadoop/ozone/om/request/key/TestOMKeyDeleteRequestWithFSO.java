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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.OzonePrefixPathImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.security.acl.OzonePrefixPath;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tests OmKeyDelete request with prefix layout.
 */
public class TestOMKeyDeleteRequestWithFSO extends TestOMKeyDeleteRequest {

  @Override
  protected OMKeyDeleteRequest getOmKeyDeleteRequest(
      OMRequest modifiedOmRequest) {
    return new OMKeyDeleteRequestWithFSO(modifiedOmRequest);
  }

  @Override
  protected String addKeyToTable() throws Exception {
    String parentDir = "c/d/e";
    String fileName = "file1";
    String key = parentDir + "/" + fileName;
    keyName = key; // updated key name

    // Create parent dirs for the path
    long parentId = TestOMRequestUtils.addParentsToDirTable(volumeName,
            bucketName, parentDir, omMetadataManager);

    OmKeyInfo omKeyInfo =
            TestOMRequestUtils.createOmKeyInfo(volumeName, bucketName, key,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE,
                    parentId + 1,
                    parentId, 100, Time.now());
    omKeyInfo.setKeyName(fileName);
    TestOMRequestUtils.addFileToKeyTable(false, false,
            fileName, omKeyInfo, -1, 50, omMetadataManager);
    return omKeyInfo.getPath();
  }

  @Override
  protected OzoneConfiguration getOzoneConfiguration() {
    OzoneConfiguration config = super.getOzoneConfiguration();
    // Setting explicitly to FSO
    super.setBucketlayout(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    return config;
  }

  @Test
  public void testOzonePrefixPathViewer() throws Exception {
    // Add volume, bucket and key entries to OM DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    String ozoneKey = addKeyToTable();

    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    // As we added manually to key table.
    Assert.assertNotNull(omKeyInfo);

    // OzonePrefixPathImpl on a directory
    OzonePrefixPathImpl ozonePrefixPath = new OzonePrefixPathImpl(volumeName,
        bucketName, "c", keyManager);
    OzoneFileStatus status = ozonePrefixPath.getOzoneFileStatus();
    Assert.assertNotNull(status);
    Assert.assertEquals("c", status.getTrimmedName());
    Assert.assertTrue(status.isDirectory());
    verifyPath(ozonePrefixPath, "c", "c/d");
    verifyPath(ozonePrefixPath, "c/d", "c/d/e");
    verifyPath(ozonePrefixPath, "c/d/e", "c/d/e/file1");

    try {
      ozonePrefixPath.getChildren("c/d/e/file1");
      Assert.fail("Should throw INVALID_KEY_NAME as the given path is a file.");
    } catch (OMException ome) {
      Assert.assertEquals(OMException.ResultCodes.INVALID_KEY_NAME,
          ome.getResult());
    }

    // OzonePrefixPathImpl on a file
    ozonePrefixPath = new OzonePrefixPathImpl(volumeName,
        bucketName, "c/d/e/file1", keyManager);
    status = ozonePrefixPath.getOzoneFileStatus();
    Assert.assertNotNull(status);
    Assert.assertEquals("c/d/e/file1", status.getTrimmedName());
    Assert.assertEquals("c/d/e/file1", status.getKeyInfo().getKeyName());
    Assert.assertTrue(status.isFile());
  }

  private void verifyPath(OzonePrefixPath ozonePrefixPath, String pathName,
                          String expectedPath)
      throws IOException {
    Iterator<? extends OzoneFileStatus> pathItr = ozonePrefixPath.getChildren(
        pathName);
    Assert.assertTrue("Failed to list keyPaths", pathItr.hasNext());
    Assert.assertEquals(expectedPath, pathItr.next().getTrimmedName());
    try{
      pathItr.next();
      Assert.fail("Reached end of the list!");
    } catch (NoSuchElementException nse){
      // expected
    }
  }
}
