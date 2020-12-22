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

package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;

/**
 * Tests OMAllocateBlockResponse layout version V1.
 */
public class TestOMAllocateBlockResponseV1
        extends TestOMAllocateBlockResponse {

  // logical ID, which really doesn't exist in dirTable
  private long parentID = 10;
  private String fileName = "file1";

  protected OmKeyInfo createOmKeyInfo() throws Exception {
    // need to initialize parentID
    String parentDir = keyName;
    keyName = parentDir + OzoneConsts.OM_KEY_PREFIX + fileName;

    long txnId = 50;
    long objectId = parentID + 1;

    OmKeyInfo omKeyInfoV1 =
            TestOMRequestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE, objectId, parentID, txnId,
                    Time.now());
    return omKeyInfoV1;
  }

  protected String getOpenKey() throws Exception {
    return omMetadataManager.getOpenFileName(
            parentID, fileName, clientID);
  }

  @NotNull
  protected OMAllocateBlockResponse getOmAllocateBlockResponse(
          OmKeyInfo omKeyInfo, OmVolumeArgs omVolumeArgs,
          OmBucketInfo omBucketInfo, OMResponse omResponse) {
    return new OMAllocateBlockResponseV1(omResponse, omKeyInfo, clientID,
            omBucketInfo);
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

}
