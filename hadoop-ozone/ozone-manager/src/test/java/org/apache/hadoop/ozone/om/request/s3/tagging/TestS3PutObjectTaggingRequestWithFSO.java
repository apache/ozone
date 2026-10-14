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

import com.google.protobuf.ByteString;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
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

  @Test
  public void testPutObjectTaggingRejectsActiveNfsFileLease() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());
    String tableKey = addKeyToTable();
    OmKeyInfo keyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(tableKey);
    long volumeId = omMetadataManager.getVolumeId(volumeName);
    long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    long now = Time.now();
    String exportUuid = UUID.randomUUID().toString();
    OmNfsLeaseInfo exportLease = OmNfsLeaseInfo.createExportLease(exportUuid, volumeId, bucketId,
        1, now + 60_000, "gateway", 1);
    OmNfsLeaseInfo fileLease = OmNfsLeaseInfo.acquireFileLease(null, exportLease, keyInfo.getObjectID(),
        "transfer", "nfs-user@EXAMPLE.COM", ByteString.copyFromUtf8("owner-token"), now, 10_000);
    omMetadataManager.getNfsExportLeaseTable().put(omMetadataManager.getNfsExportLeaseKey(exportUuid), exportLease);
    omMetadataManager.getNfsFileLeaseTable().put(
        omMetadataManager.getNfsFileLeaseKey(volumeId, bucketId, keyInfo.getObjectID()), fileLease);

    OMRequest modifiedOmRequest = doPreExecute(volumeName, bucketName, FILE_KEY, getTags(2));
    OMClientResponse response = getPutObjectTaggingRequest(modifiedOmRequest)
        .validateAndUpdateCache(ozoneManager, 2L);

    assertEquals(OzoneManagerProtocolProtos.Status.NFS_LEASE_CONFLICT, response.getOMResponse().getStatus());
    assertEquals(keyInfo.getTags(), omMetadataManager.getKeyTable(getBucketLayout()).get(tableKey).getTags());
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
