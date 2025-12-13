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
import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

/**
 * Class tests OMKeyCommitRequest with prefix layout.
 */
public class TestOMKeyCommitRequestWithFSO extends TestOMKeyCommitRequest {

  private long parentID = Long.MIN_VALUE;

  private long getVolumeID() throws IOException {
    return omMetadataManager.getVolumeId(volumeName);
  }

  private long getBucketID() throws java.io.IOException {
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    if (omBucketInfo != null) {
      return omBucketInfo.getObjectID();
    }
    // bucket doesn't exists in DB
    return Long.MIN_VALUE;
  }

  @Override
  protected String getOzonePathKey() throws IOException {
    final long volumeID = getVolumeID();
    final long bucketID = getBucketID();
    String fileName = OzoneFSUtils.getFileName(keyName);

    return omMetadataManager.getOzonePathKey(volumeID, bucketID,
            parentID, fileName);
  }

  @Override
  protected String addKeyToOpenKeyTable(List<OmKeyLocationInfo> locationList, OmKeyInfo.Builder keyInfoBuilder)
      throws Exception {
    // need to initialize parentID
    if (getParentDir() == null) {
      parentID = getBucketID();
    } else {
      parentID = OMRequestTestUtils.addParentsToDirTable(volumeName,
          bucketName, getParentDir(), omMetadataManager);
    }
    keyInfoBuilder.setParentObjectID(parentID);

    OmKeyInfo keyInfo = keyInfoBuilder.build();
    keyInfo.appendNewBlocks(locationList, false);

    String fileName = OzoneFSUtils.getFileName(keyName);
    return OMRequestTestUtils.addFileToKeyTable(true, false,
        fileName, keyInfo, clientID, txnLogId, omMetadataManager);

  }

  @Override
  protected String addKeyToOpenKeyTable(List<OmKeyLocationInfo> locationList)
      throws Exception {
    long objectId = 100;

    OmKeyInfo.Builder omKeyInfoFSO =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
                RatisReplicationConfig.getInstance(ONE), new OmKeyLocationInfoGroup(version, new ArrayList<>(), false))
            .setObjectID(objectId)
            .setUpdateID(100L);
    return addKeyToOpenKeyTable(locationList, omKeyInfoFSO);
  }

  @Override
  @Nonnull
  protected OMKeyCommitRequest getOmKeyCommitRequest(OMRequest omRequest) {
    return new OMKeyCommitRequestWithFSO(omRequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @Override
  protected void verifyKeyName(OmKeyInfo omKeyInfo) {
    // prefix layout format - stores fileName in the keyName DB field.
    String fileName = OzoneFSUtils.getFileName(keyName);
    assertEquals(fileName, omKeyInfo.getFileName(), "Incorrect FileName");
    assertEquals(fileName, omKeyInfo.getKeyName(), "Incorrect KeyName");
  }
}
