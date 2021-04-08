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
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Tests OMCreateKeyRequestV1 class.
 */
public class TestOMKeyCreateRequestV1 extends TestOMKeyCreateRequest {

  @NotNull
  @Override
  protected OzoneConfiguration getOzoneConfiguration() {
    OzoneConfiguration config = super.getOzoneConfiguration();
    // omLayoutVersionV1 flag will be set while invoking OzoneManager#start()
    // and its not invoked in this test. Hence it is explicitly setting
    // this configuration to populate prefix tables.
    OzoneManagerRatisUtils.setBucketFSOptimized(true);
    return config;
  }

  protected void addToKeyTable(String keyName) throws Exception {
    Path keyPath = Paths.get(keyName);
    long parentId = checkIntermediatePaths(keyPath);
    String fileName = OzoneFSUtils.getFileName(keyName);
    OmKeyInfo omKeyInfo =
            TestOMRequestUtils.createOmKeyInfo(volumeName, bucketName, fileName,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE,
                    parentId + 1,
                    parentId, 100, Time.now());
    TestOMRequestUtils.addFileToKeyTable(false, false,
            fileName, omKeyInfo, -1, 50, omMetadataManager);
  }

  protected void checkCreatedPaths(OMKeyCreateRequest omKeyCreateRequest,
      OMRequest omRequest, String keyName) throws Exception {
    keyName = omKeyCreateRequest.validateAndNormalizeKey(true, keyName);
    // Check intermediate directories created or not.
    Path keyPath = Paths.get(keyName);
    long parentID = checkIntermediatePaths(keyPath);

    // Check open key entry
    String fileName = keyPath.getFileName().toString();
    String openKey = omMetadataManager.getOpenFileName(parentID, fileName,
            omRequest.getCreateKeyRequest().getClientID());
    OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);
    Assert.assertNotNull(omKeyInfo);
  }

  protected long checkIntermediatePaths(Path keyPath) throws Exception {
    // Check intermediate paths are created
    keyPath = keyPath.getParent(); // skip the file name
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    long lastKnownParentId = omBucketInfo.getObjectID();

    Iterator<Path> elements = keyPath.iterator();
    StringBuilder fullKeyPath = new StringBuilder(bucketKey);
    while (elements.hasNext()) {
      String fileName = elements.next().toString();
      fullKeyPath.append(OzoneConsts.OM_KEY_PREFIX);
      fullKeyPath.append(fileName);
      String dbNodeName = omMetadataManager.getOzonePathKey(
              lastKnownParentId, fileName);
      OmDirectoryInfo omDirInfo = omMetadataManager.getDirectoryTable().
              get(dbNodeName);

      Assert.assertNotNull("Parent key path:" + fullKeyPath +
              " doesn't exist", omDirInfo);
      lastKnownParentId = omDirInfo.getObjectID();
    }

    return lastKnownParentId;
  }

  protected String getOpenKey(long id) throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    if (omBucketInfo != null) {
      return omMetadataManager.getOpenFileName(omBucketInfo.getObjectID(),
              keyName, id);
    } else {
      return omMetadataManager.getOpenFileName(1000, keyName, id);
    }
  }

  protected OMKeyCreateRequest getOMKeyCreateRequest(OMRequest omRequest) {
    return new OMKeyCreateRequestV1(omRequest);
  }
}
