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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import jakarta.annotation.Nonnull;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.StringUtils;

/**
 * Tests OMAllocateBlockRequest class prefix layout.
 */
public class TestOMAllocateBlockRequestWithFSO
    extends TestOMAllocateBlockRequest {

  @Nonnull
  @Override
  protected OzoneConfiguration getOzoneConfiguration() {
    OzoneConfiguration config = super.getOzoneConfiguration();
    // metadata layout prefix will be set while invoking OzoneManager#start()
    // and its not invoked in this test. Hence it is explicitly setting
    // this configuration to populate prefix tables.
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
    long parentID = OMRequestTestUtils.addParentsToDirTable(volumeName,
            bucketName, parentDir, omMetadataManager);
    long txnId = 50;
    long objectId = parentID + 1;

    OmKeyInfo omKeyInfoFSO =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
            .setObjectID(objectId)
            .setParentObjectID(parentID)
            .setUpdateID(txnId)
            .build();

    // add key to openFileTable
    OMRequestTestUtils.addFileToKeyTable(true, false,
            fileName, omKeyInfoFSO, clientID, txnLogId, omMetadataManager);

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return omMetadataManager.getOzonePathKey(volumeId, bucketId,
            parentID, fileName);
  }

  @Nonnull
  @Override
  protected OMAllocateBlockRequest getOmAllocateBlockRequest(
      OzoneManagerProtocolProtos.OMRequest modifiedOmRequest) {
    return new OMAllocateBlockRequestWithFSO(modifiedOmRequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @Override
  protected OmKeyInfo verifyPathInOpenKeyTable(String key, long id,
      boolean doAssert) throws Exception {
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    String[] pathComponents = StringUtils.split(key, '/');
    long parentId = bucketId;
    for (int indx = 0; indx < pathComponents.length; indx++) {
      String pathElement = pathComponents[indx];
      // Reached last component, which is file name
      if (indx == pathComponents.length - 1) {
        String dbOpenFileName =
            omMetadataManager.getOpenFileName(volumeId, bucketId,
                    parentId, pathElement, id);
        OmKeyInfo omKeyInfo =
            omMetadataManager.getOpenKeyTable(getBucketLayout())
                .get(dbOpenFileName);
        if (doAssert) {
          assertNotNull(omKeyInfo, "Invalid key!");
        }
        return omKeyInfo;
      } else {
        // directory
        String dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
                parentId, pathElement);
        OmDirectoryInfo dirInfo =
            omMetadataManager.getDirectoryTable().get(dbKey);
        parentId = dirInfo.getObjectID();
      }
    }
    if (doAssert) {
      fail("Invalid key!");
    }
    return  null;
  }
}
