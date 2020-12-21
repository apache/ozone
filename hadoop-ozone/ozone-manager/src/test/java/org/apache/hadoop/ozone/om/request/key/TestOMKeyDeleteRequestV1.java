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
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;

/**
 * Tests OmKeyDelete request layout version V1.
 */
public class TestOMKeyDeleteRequestV1 extends TestOMKeyDeleteRequest {

  protected OMKeyDeleteRequest getOmKeyDeleteRequest(
      OMRequest modifiedOmRequest) {
    return new OMKeyDeleteRequestV1(modifiedOmRequest);
  }

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
    TestOMRequestUtils.addFileToKeyTable(false, false,
            fileName, omKeyInfo, -1, 50, omMetadataManager);
    return omKeyInfo.getPath();
  }

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
