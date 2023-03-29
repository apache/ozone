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
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.TestOMResponseUtils;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Tests TestOMKeyRenameResponseWithFSO.
 */
public class TestOMKeyRenameResponseWithFSO extends TestOMKeyRenameResponse {
  @Override
  protected OmKeyInfo getOmKeyInfo(String keyName) {
    long bucketId = random.nextLong();
    return OMRequestTestUtils.createOmKeyInfo(
        volumeName, bucketName, keyName,
        HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.ONE,
        bucketId + 100L, bucketId + 101L, 0L, Time.now());
  }

  @Override
  protected OmKeyInfo getOmKeyInfo(OmKeyInfo toKeyInfo,
                                   String keyName) {
    return OMRequestTestUtils.createOmKeyInfo(toKeyInfo.getVolumeName(),
        toKeyInfo.getBucketName(), keyName, replicationType,
        replicationFactor, toKeyInfo.getObjectID(),
        toKeyInfo.getParentObjectID(), 0L, toKeyInfo.getCreationTime());
  }

  @Override
  protected String addKeyToTable(OmKeyInfo keyInfo) throws Exception {
    OMRequestTestUtils.addFileToKeyTable(false, false,
        keyInfo.getFileName(), keyInfo, clientID, txnLogId, omMetadataManager);
    return getDBKeyName(keyInfo);
  }

  @Override
  protected String getDBKeyName(OmKeyInfo keyInfo) throws IOException {
    return omMetadataManager.getOzonePathKey(
        omMetadataManager.getVolumeId(volumeName),
        omMetadataManager.getBucketId(volumeName, bucketName),
        keyInfo.getParentObjectID(), keyInfo.getKeyName());
  }

  @Override
  protected OMKeyRenameResponse getOMKeyRenameResponse(OMResponse response,
      OmKeyInfo fromKeyInfo, OmKeyInfo toKeyInfo) throws IOException {
    createParent();
    return new OMKeyRenameResponseWithFSO(response, getDBKeyName(fromKeyInfo),
        getDBKeyName(toKeyInfo), fromKeyParent, toKeyParent, toKeyInfo,
        bucketInfo, false, getBucketLayout());
  }
  protected void createParent() {
    long bucketId = random.nextLong();
    String fromKeyParentName = UUID.randomUUID().toString();
    String toKeyParentName = UUID.randomUUID().toString();
    fromKeyParent = OMRequestTestUtils.createOmKeyInfo(volumeName,
        bucketName, fromKeyParentName, replicationType, replicationFactor,
        bucketId + 100L);
    toKeyParent = OMRequestTestUtils.createOmKeyInfo(volumeName,
        bucketName, toKeyParentName, replicationType, replicationFactor,
        bucketId + 101L);
    fromKeyParent.setParentObjectID(bucketId);
    toKeyParent.setParentObjectID(bucketId);
    fromKeyParent.setFileName(OzoneFSUtils.getFileName(
        fromKeyParent.getKeyName()));
    toKeyParent.setFileName(OzoneFSUtils.getFileName(
        toKeyParent.getKeyName()));
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    bucketInfo = TestOMResponseUtils.createBucket(volumeName, bucketName);
  }
  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
