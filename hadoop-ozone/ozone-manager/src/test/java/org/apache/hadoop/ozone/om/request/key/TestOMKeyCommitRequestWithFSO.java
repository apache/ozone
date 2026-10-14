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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCommitRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

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

  @Test
  public void testNfsFileLeaseFencesCommitWithoutNfsIdentity() throws Exception {
    OMRequest modifiedOmRequest = doPreExecute(createCommitKeyRequest());
    OMKeyCommitRequest omKeyCommitRequest = getOmKeyCommitRequest(modifiedOmRequest);
    List<OmKeyLocationInfo> allocatedLocationList = modifiedOmRequest.getCommitKeyRequest().getKeyArgs()
        .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(java.util.stream.Collectors.toList());
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, omKeyCommitRequest.getBucketLayout());
    addKeyToOpenKeyTable(allocatedLocationList);

    long now = Time.now();
    long volumeId = getVolumeID();
    long bucketId = getBucketID();
    String exportUuid = UUID.randomUUID().toString();
    OmNfsLeaseInfo exportLease = OmNfsLeaseInfo.createExportLease(exportUuid, volumeId, bucketId,
        1, now + 60_000, "gateway", 1);
    OmNfsLeaseInfo fileLease = OmNfsLeaseInfo.acquireFileLease(null, exportLease, 100,
        "transfer", "nfs-user@EXAMPLE.COM", ByteString.copyFromUtf8("owner-token"), now, 10_000);
    String fileLeaseKey = omMetadataManager.getNfsFileLeaseKey(volumeId, bucketId, 100);
    omMetadataManager.getNfsFileLeaseTable().put(fileLeaseKey, fileLease);
    omMetadataManager.getNfsExportLeaseTable().put(
        omMetadataManager.getNfsExportLeaseKey(exportUuid), exportLease);

    OMClientResponse response = omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 101L);

    assertEquals(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NFS_LEASE_CONFLICT,
        response.getOMResponse().getStatus());
  }

  @Test
  public void testNfsCommitPersistsPublishedContentMarker() throws Exception {
    OMRequest modifiedOmRequest = doPreExecute(createCommitKeyRequest());
    List<OmKeyLocationInfo> allocatedLocationList = modifiedOmRequest.getCommitKeyRequest().getKeyArgs()
        .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(java.util.stream.Collectors.toList());
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    addKeyToOpenKeyTable(allocatedLocationList);

    long now = Time.now();
    long volumeId = getVolumeID();
    long bucketId = getBucketID();
    String exportUuid = UUID.randomUUID().toString();
    ByteString ownerToken = ByteString.copyFromUtf8("owner-token");
    OmNfsLeaseInfo exportLease = OmNfsLeaseInfo.createExportLease(exportUuid, volumeId, bucketId,
        1, now + 60_000, "gateway", 1);
    OmNfsLeaseInfo fileLease = OmNfsLeaseInfo.acquireFileLease(null, exportLease, 100,
        "transfer", "nfs-user@EXAMPLE.COM", ownerToken, now, 10_000);
    String fileLeaseKey = omMetadataManager.getNfsFileLeaseKey(volumeId, bucketId, 100);
    omMetadataManager.getNfsFileLeaseTable().put(fileLeaseKey, fileLease);
    omMetadataManager.getNfsExportLeaseTable().put(
        omMetadataManager.getNfsExportLeaseKey(exportUuid), exportLease);
    NfsFileCommitRequest fence = NfsFileCommitRequest.newBuilder().setExportUUID(exportUuid).setGatewayID("gateway")
        .setGatewayGeneration(1).setTransferID("transfer").setFileGeneration(1).setOwnerToken(ownerToken).build();
    modifiedOmRequest = modifiedOmRequest.toBuilder().setCommitKeyRequest(
        modifiedOmRequest.getCommitKeyRequest().toBuilder().setNfsFileCommitRequest(fence)).build();

    OMClientResponse response = getOmKeyCommitRequest(modifiedOmRequest).validateAndUpdateCache(ozoneManager, 101L);

    assertEquals(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    assertTrue(omMetadataManager.getNfsFileLeaseTable().get(fileLeaseKey).getProtobuf().getContentPublished());
    try (BatchOperation batch = omMetadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(omMetadataManager, batch);
      omMetadataManager.getStore().commitBatchOperation(batch);
    }
    assertTrue(omMetadataManager.getNfsFileLeaseTable().get(fileLeaseKey).getProtobuf().getContentPublished());
  }
}
