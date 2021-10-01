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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

/**
 * Tests OMAllocateBlockRequest class prefix layout.
 */
public class TestOMAllocateBlockRequestWithFSO
    extends TestOMAllocateBlockRequest {

  @NotNull
  @Override
  protected OzoneConfiguration getOzoneConfiguration() {
    OzoneConfiguration config = super.getOzoneConfiguration();
    // metadata layout prefix will be set while invoking OzoneManager#start()
    // and its not invoked in this test. Hence it is explicitly setting
    // this configuration to populate prefix tables.
    OzoneManagerRatisUtils.setBucketFSOptimized(true);
    return config;
  }

  @Override
  protected String addKeyToOpenKeyTable(String volumeName, String bucketName)
          throws Exception {
    // need to initialize parentID
    String parentDir = keyName;
    String fileName = "file1";
    keyName = parentDir + OzoneConsts.OM_KEY_PREFIX + fileName;

    // add parentDir to dirTable
    long parentID = TestOMRequestUtils.addParentsToDirTable(volumeName,
            bucketName, parentDir, omMetadataManager);
    long txnId = 50;
    long objectId = parentID + 1;

    OmKeyInfo omKeyInfoFSO =
            TestOMRequestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE, objectId, parentID, txnId,
                    Time.now());

    // add key to openFileTable
    TestOMRequestUtils.addFileToKeyTable(true, false,
            fileName, omKeyInfoFSO, clientID, txnLogId, omMetadataManager);

    return omMetadataManager.getOzonePathKey(parentID, fileName);
  }

  @NotNull
  @Override
  protected OMAllocateBlockRequest getOmAllocateBlockRequest(
          OzoneManagerProtocolProtos.OMRequest modifiedOmRequest) {
    return new OMAllocateBlockRequestWithFSO(modifiedOmRequest);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @Override
  protected OmKeyInfo verifyPathInOpenKeyTable(String key, long id,
      boolean doAssert) throws Exception {
    long bucketId = TestOMRequestUtils.getBucketId(volumeName, bucketName,
            omMetadataManager);
    String[] pathComponents = StringUtils.split(key, '/');
    long parentId = bucketId;
    for (int indx = 0; indx < pathComponents.length; indx++) {
      String pathElement = pathComponents[indx];
      // Reached last component, which is file name
      if (indx == pathComponents.length - 1) {
        String dbOpenFileName =
            omMetadataManager.getOpenFileName(parentId, pathElement, id);
        OmKeyInfo omKeyInfo =
            omMetadataManager.getOpenKeyTable(getBucketLayout())
                .get(dbOpenFileName);
        if (doAssert) {
          Assert.assertNotNull("Invalid key!", omKeyInfo);
        }
        return omKeyInfo;
      } else {
        // directory
        String dbKey = omMetadataManager.getOzonePathKey(parentId, pathElement);
        OmDirectoryInfo dirInfo =
            omMetadataManager.getDirectoryTable().get(dbKey);
        parentId = dirInfo.getObjectID();
      }
    }
    if (doAssert) {
      Assert.fail("Invalid key!");
    }
    return  null;
  }
}
