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
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.IOException;

/**
 * Class tests OMKeyCommitRequestV1 class layout version V1.
 */
public class TestOMKeyCommitRequestV1 extends TestOMKeyCommitRequest {

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
  protected String addKeyToOpenKeyTable() throws Exception {
    // need to initialize parentID
    if (getParentDir() == null) {
      parentID = getBucketID();
    } else {
      parentID = TestOMRequestUtils.addParentsToDirTable(volumeName,
              bucketName, getParentDir(), omMetadataManager);
    }
    long objectId = 100;

    OmKeyInfo omKeyInfoV1 =
            TestOMRequestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE, objectId, parentID, 100,
                    Time.now());

    String fileName = OzoneFSUtils.getFileName(keyName);
    TestOMRequestUtils.addFileToKeyTable(true, false,
            fileName, omKeyInfoV1, clientID, txnLogId, omMetadataManager);

    return omMetadataManager.getOzonePathKey(parentID, fileName);
  }

  @NotNull
  @Override
  protected OzoneConfiguration getOzoneConfiguration() {
    OzoneConfiguration config = super.getOzoneConfiguration();
    config.set(OMConfigKeys.OZONE_OM_LAYOUT_VERSION, "V1");
    // omLayoutVersionV1 flag will be set while invoking OzoneManager#start()
    // and its not invoked in this test. Hence it is explicitly setting
    // this configuration to populate prefix tables.
    OzoneManagerRatisUtils.setOmLayoutVersionV1(true);
    return config;
  }

  @NotNull
  protected OMKeyCommitRequest getOmKeyCommitRequest(OMRequest omRequest) {
    return new OMKeyCommitRequestV1(omRequest);
  }

  protected void verifyKeyName(OmKeyInfo omKeyInfo) {
    // V1 format - stores fileName in the keyName DB field.
    String fileName = OzoneFSUtils.getFileName(keyName);
    Assert.assertEquals("Incorrect FileName", fileName,
            omKeyInfo.getFileName());
    Assert.assertEquals("Incorrect KeyName", fileName,
            omKeyInfo.getKeyName());
  }
}
