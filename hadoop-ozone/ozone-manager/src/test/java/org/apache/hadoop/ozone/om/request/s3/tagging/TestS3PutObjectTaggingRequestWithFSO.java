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

package org.apache.hadoop.ozone.om.request.s3.tagging;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.jupiter.api.Test;

/**
 * Test put object tagging request for FSO bucket.
 */
public class TestS3PutObjectTaggingRequestWithFSO extends TestS3PutObjectTaggingRequest {

  private static final String PARENT_DIR = "c/d/e";
  private static final String FILE_NAME = "file1";
  private static final String FILE_KEY = PARENT_DIR + "/" + FILE_NAME;

  @Test
  public void testValidateAndUpdateCachePutObjectTaggingToDir() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());

    addKeyToTable();

    OMRequest modifiedOmRequest =
        doPreExecute(volumeName, bucketName, PARENT_DIR, getTags(2));

    S3PutObjectTaggingRequest request = getPutObjectTaggingRequest(modifiedOmRequest);

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 2L);

    assertEquals(OzoneManagerProtocolProtos.Status.NOT_SUPPORTED_OPERATION,
        omClientResponse.getOMResponse().getStatus());
  }

  @Override
  protected String addKeyToTable() throws Exception {
    keyName = FILE_KEY; // updated key name

    // Create parent dirs for the path
    long parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
        bucketName, PARENT_DIR, omMetadataManager);

    OmKeyInfo omKeyInfo =
        OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, FILE_NAME, RatisReplicationConfig.getInstance(ONE))
            .setObjectID(parentId + 1L)
            .setParentObjectID(parentId)
            .setUpdateID(1L)
            .build();
    OMRequestTestUtils.addFileToKeyTable(false, false,
        FILE_NAME, omKeyInfo, -1, 50, omMetadataManager);
    final long volumeId = omMetadataManager.getVolumeId(
        omKeyInfo.getVolumeName());
    final long bucketId = omMetadataManager.getBucketId(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());
    return omMetadataManager.getOzonePathKey(
        volumeId, bucketId, omKeyInfo.getParentObjectID(),
        omKeyInfo.getFileName());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @Override
  protected S3PutObjectTaggingRequest getPutObjectTaggingRequest(OMRequest originalRequest) {
    return new S3PutObjectTaggingRequestWithFSO(originalRequest, getBucketLayout());
  }
}
