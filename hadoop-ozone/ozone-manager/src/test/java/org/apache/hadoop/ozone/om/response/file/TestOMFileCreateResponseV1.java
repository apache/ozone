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

package org.apache.hadoop.ozone.om.response.file;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.om.response.key.TestOMKeyCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

/**
 * Tests MKeyCreateResponse layout version V1.
 */
public class TestOMFileCreateResponseV1 extends TestOMKeyCreateResponse {

  @NotNull
  @Override
  protected OmKeyInfo getOmKeyInfo() {
    Assert.assertNotNull(omBucketInfo);
    return TestOMRequestUtils.createOmKeyInfo(volumeName,
            omBucketInfo.getBucketName(), keyName, replicationType,
            replicationFactor,
            omBucketInfo.getObjectID() + 1,
            omBucketInfo.getObjectID(), 100, Time.now());
  }

  @NotNull
  @Override
  protected String getOpenKeyName() {
    Assert.assertNotNull(omBucketInfo);
    return omMetadataManager.getOpenFileName(
            omBucketInfo.getObjectID(), keyName, clientID);
  }

  @NotNull
  @Override
  protected OMKeyCreateResponse getOmKeyCreateResponse(OmKeyInfo keyInfo,
      OmBucketInfo bucketInfo, OMResponse response) {

    return new OMFileCreateResponseV1(response, keyInfo, null, clientID,
            bucketInfo);
  }

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
}
