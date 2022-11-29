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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.DeleteTablePrefix;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;
import org.junit.Assert;

/**
 * Tests OMKeyDeleteResponse - prefix layout.
 */
public class TestOMKeyDeleteResponseWithFSO extends TestOMKeyDeleteResponse {

  @Override
  protected OMKeyDeleteResponse getOmKeyDeleteResponse(OmKeyInfo omKeyInfo,
          OzoneManagerProtocolProtos.OMResponse omResponse,
          DeleteTablePrefix deleteTablePrefix) throws Exception {
    return new OMKeyDeleteResponseWithFSO(omResponse, omKeyInfo.getKeyName(),
            deleteTablePrefix, omKeyInfo, getOmBucketInfo(), false,
        omMetadataManager.getVolumeId(volumeName));
  }

  @Override
  protected String addKeyToTable() throws Exception {
    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);

    // Create parent dirs for the path
    long parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
            bucketName, "", omMetadataManager);

    OmKeyInfo omKeyInfo =
            OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.ONE,
                    parentId + 1,
                    parentId, 100, Time.now());
    OMRequestTestUtils.addFileToKeyTable(false, false,
            keyName, omKeyInfo, -1, 50, omMetadataManager);
    return omMetadataManager.getOzonePathKey(
            omMetadataManager.getVolumeId(volumeName),
            omMetadataManager.getBucketId(volumeName, bucketName),
           omKeyInfo.getParentObjectID(), keyName);
  }

  @Override
  protected OmKeyInfo getOmKeyInfo() {
    Assert.assertNotNull(getOmBucketInfo());
    return OMRequestTestUtils.createOmKeyInfo(volumeName,
            getOmBucketInfo().getBucketName(), keyName, replicationType,
            replicationFactor,
            getOmBucketInfo().getObjectID() + 1,
            getOmBucketInfo().getObjectID(), 100, Time.now());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
