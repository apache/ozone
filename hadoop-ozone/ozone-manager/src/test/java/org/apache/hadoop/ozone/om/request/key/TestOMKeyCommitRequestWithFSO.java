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
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;

/**
 * Class tests OMKeyCommitRequest with prefix layout.
 */
public class TestOMKeyCommitRequestWithFSO extends TestOMKeyCommitRequest {

  private long parentID = Long.MIN_VALUE;

  private long getBucketID() throws java.io.IOException {
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    if(omBucketInfo!= null){
      return omBucketInfo.getObjectID();
    }
    // bucket doesn't exists in DB
    return Long.MIN_VALUE;
  }

  @Override
  protected String getOzonePathKey() throws IOException {
    long bucketID = getBucketID();
    String fileName = OzoneFSUtils.getFileName(keyName);
    return omMetadataManager.getOzonePathKey(bucketID, fileName);
  }

  @Override
  protected String addKeyToOpenKeyTable(List<OmKeyLocationInfo> locationList)
      throws Exception {
    // need to initialize parentID
    if (getParentDir() == null) {
      parentID = getBucketID();
    } else {
      parentID = TestOMRequestUtils.addParentsToDirTable(volumeName,
              bucketName, getParentDir(), omMetadataManager);
    }
    long objectId = 100;

    OmKeyInfo omKeyInfoFSO =
            TestOMRequestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE, objectId, parentID, 100,
                    Time.now());
    omKeyInfoFSO.appendNewBlocks(locationList, false);

    String fileName = OzoneFSUtils.getFileName(keyName);
    TestOMRequestUtils.addFileToKeyTable(true, false,
            fileName, omKeyInfoFSO, clientID, txnLogId, omMetadataManager);

    return omMetadataManager.getOzonePathKey(parentID, fileName);
  }

  @Override
  protected OzoneConfiguration getOzoneConfiguration() {
    OzoneConfiguration config = super.getOzoneConfiguration();
    // Metadata layout prefix will be set while invoking OzoneManager#start()
    // and its not invoked in this test. Hence it is explicitly setting
    // this configuration to populate prefix tables.
    OzoneManagerRatisUtils.setBucketFSOptimized(true);
    return config;
  }

  @NotNull
  protected OMKeyCommitRequest getOmKeyCommitRequest(OMRequest omRequest) {
    return new OMKeyCommitRequestWithFSO(omRequest);
  }

  @Override
  protected void verifyKeyName(OmKeyInfo omKeyInfo) {
    // prefix layout format - stores fileName in the keyName DB field.
    String fileName = OzoneFSUtils.getFileName(keyName);
    Assert.assertEquals("Incorrect FileName", fileName,
            omKeyInfo.getFileName());
    Assert.assertEquals("Incorrect KeyName", fileName,
            omKeyInfo.getKeyName());
  }
}
