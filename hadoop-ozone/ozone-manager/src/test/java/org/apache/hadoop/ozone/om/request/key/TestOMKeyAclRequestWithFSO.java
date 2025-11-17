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

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAddAclRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyRemoveAclRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeySetAclRequestWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Test Key ACL requests for prefix layout.
 */
public class TestOMKeyAclRequestWithFSO extends TestOMKeyAclRequest {

  @Override
  protected String addKeyToTable() throws Exception {
    String parentDir = "c/d/e";
    String fileName = "file1";
    keyName = parentDir + "/" + fileName; // updated key name

    // Create parent dirs for the path
    long parentId = OMRequestTestUtils
        .addParentsToDirTable(volumeName, bucketName, parentDir,
            omMetadataManager);

    OmKeyInfo omKeyInfo =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, fileName, RatisReplicationConfig.getInstance(ONE))
            .setObjectID(parentId + 1L)
            .setParentObjectID(parentId)
            .setUpdateID(100L)
            .build();
    OMRequestTestUtils
        .addFileToKeyTable(false, false, fileName, omKeyInfo, -1, 50,
            omMetadataManager);
    final long volumeId = omMetadataManager.getVolumeId(
        omKeyInfo.getVolumeName());
    final long bucketId = omMetadataManager.getBucketId(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());
    return omMetadataManager.getOzonePathKey(
        volumeId, bucketId, omKeyInfo.getParentObjectID(),
        fileName);
  }

  @Override
  protected OMKeyAclRequest getOmKeyAddAclRequest(
      OzoneManagerProtocolProtos.OMRequest originalRequest) {
    return new OMKeyAddAclRequestWithFSO(originalRequest, getBucketLayout());
  }

  @Override
  protected OMKeyAclRequest getOmKeyRemoveAclRequest(
      OzoneManagerProtocolProtos.OMRequest removeAclRequest) {
    return new OMKeyRemoveAclRequestWithFSO(removeAclRequest,
        getBucketLayout());
  }

  @Override
  protected OMKeyAclRequest getOmKeySetAclRequest(
      OzoneManagerProtocolProtos.OMRequest setAclRequest) {
    return new OMKeySetAclRequestWithFSO(setAclRequest, getBucketLayout());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
