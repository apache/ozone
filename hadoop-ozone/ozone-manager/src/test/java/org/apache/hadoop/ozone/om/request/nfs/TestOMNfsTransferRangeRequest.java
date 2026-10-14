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

package org.apache.hadoop.ozone.om.request.nfs;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_DB_DIRS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsTransferRangeInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestOMNfsTransferRangeRequest {
  private static final String EXPORT_UUID = "a7ad8fd1-c88d-4f1b-bf58-38406e931adc";
  private static final String VOLUME = "volume";
  private static final String BUCKET = "bucket";
  private static final long OBJECT_ID = 100;
  private static final String GATEWAY = "gateway";
  private static final String TRANSFER = "transfer";
  private static final ByteString OWNER_TOKEN = ByteString.copyFromUtf8("owner-token");
  private static final ByteString SHA256 = ByteString.copyFrom(new byte[32]);

  @TempDir
  private java.io.File folder;
  private OMMetadataManager metadataManager;
  private OzoneManager ozoneManager;
  private long volumeId;
  private long bucketId;

  @BeforeEach
  void setup() throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_OM_DB_DIRS, folder.getAbsolutePath());
    metadataManager = spy(new OmMetadataManagerImpl(configuration, null));
    doReturn(true).when(metadataManager).isNfsDurableWriteRangesEnabled();
    doReturn(true).when(metadataManager).isNfsObjectIndexReady();
    OMRequestTestUtils.addVolumeToDB(VOLUME, metadataManager);
    OMRequestTestUtils.addBucketToDB(VOLUME, BUCKET, metadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    volumeId = metadataManager.getVolumeId(VOLUME);
    bucketId = metadataManager.getBucketId(VOLUME, BUCKET);
    OmNfsLeaseInfo exportLease = OmNfsLeaseInfo.createExportLease(
        EXPORT_UUID, volumeId, bucketId, 1, 5000, GATEWAY, 1);
    OmNfsLeaseInfo fileLease = OmNfsLeaseInfo.acquireFileLease(null, exportLease, OBJECT_ID, TRANSFER,
        "nfs-user@EXAMPLE.COM", OWNER_TOKEN, 1000, 1000);
    metadataManager.getNfsExportLeaseTable().put(EXPORT_UUID, exportLease);
    metadataManager.getNfsFileLeaseTable().put(
        metadataManager.getNfsFileLeaseKey(volumeId, bucketId, OBJECT_ID), fileLease);
    ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);
    when(ozoneManager.getAclsEnabled()).thenReturn(false);
    when(ozoneManager.getAuditLogger()).thenReturn(mock(AuditLogger.class));
  }

  @AfterEach
  void teardown() throws Exception {
    metadataManager.stop();
  }

  @Test
  void recordsIdempotentRangesAndAllowsOverlappingWritesInRatisOrder() throws Exception {
    OMClientResponse first = apply(request(NfsTransferRangeRequest.Operation.UPSERT, 0, 8, "range-0", SHA256), 1);
    assertEquals(Status.OK, first.getOMResponse().getStatus());
    assertEquals(1, first.getOMResponse().getNfsTransferRangeResponse().getTransactionIndex());
    assertEquals(range(0, 8, "range-0", SHA256), getRange(0));
    commit(first);

    OMClientResponse retry = apply(request(NfsTransferRangeRequest.Operation.UPSERT, 0, 8, "range-0", SHA256), 2);
    assertEquals(Status.OK, retry.getOMResponse().getStatus());
    assertEquals(1, retry.getOMResponse().getNfsTransferRangeResponse().getTransactionIndex());

    OMClientResponse overlap = apply(request(NfsTransferRangeRequest.Operation.UPSERT, 4, 8, "range-4", SHA256), 3);
    assertEquals(Status.OK, overlap.getOMResponse().getStatus());
    assertEquals(3, overlap.getOMResponse().getNfsTransferRangeResponse().getTransactionIndex());
    assertEquals(range(4, 8, "range-4", SHA256), getRange(4));
    byte[] shorterDigestBytes = new byte[32];
    shorterDigestBytes[0] = 2;
    ByteString shorterDigest = ByteString.copyFrom(shorterDigestBytes);
    NfsTransferRangeRequest shorterRewrite = request(NfsTransferRangeRequest.Operation.UPSERT, 0, 4,
        "range-0-short", shorterDigest);
    OMClientResponse rewrite = apply(shorterRewrite, 4);
    assertEquals(Status.OK, rewrite.getOMResponse().getStatus());
    assertEquals(4, rewrite.getOMResponse().getNfsTransferRangeResponse().getTransactionIndex());
    assertEquals(range(0, 8, "range-0", SHA256), getRange(0));
    assertEquals("range-0-short", getRange(0, "range-0-short").getStagingKey());
    byte[] differentDigest = new byte[32];
    differentDigest[0] = 1;
    OMClientResponse differentRetry = apply(request(NfsTransferRangeRequest.Operation.UPSERT, 0, 8,
        "range-0", ByteString.copyFrom(differentDigest)), 4);
    assertEquals(Status.NFS_LEASE_CONFLICT, differentRetry.getOMResponse().getStatus());
  }

  @Test
  void deletesOnlyTheMatchingRange() throws Exception {
    OMClientResponse added = apply(request(NfsTransferRangeRequest.Operation.UPSERT, 16, 8, "range-16", SHA256), 1);
    commit(added);
    OMClientResponse mismatchedDelete = apply(request(NfsTransferRangeRequest.Operation.DELETE,
        16, 8, "other-stage-key", SHA256), 2);
    assertEquals(Status.OK, mismatchedDelete.getOMResponse().getStatus());
    assertEquals("range-16", getRange(16).getStagingKey());

    OMClientResponse deleted = apply(request(NfsTransferRangeRequest.Operation.DELETE, 16, 8, "range-16", SHA256), 3);
    assertEquals(Status.OK, deleted.getOMResponse().getStatus());
    assertNull(getRange(16));
    commit(deleted);
    assertNull(getRange(16));
  }

  @Test
  void newRangesClearPublishedMarkerButIdempotentRetriesDoNot() throws Exception {
    String leaseKey = metadataManager.getNfsFileLeaseKey(volumeId, bucketId, OBJECT_ID);
    metadataManager.getNfsFileLeaseTable().put(leaseKey,
        metadataManager.getNfsFileLeaseTable().get(leaseKey).withContentPublished(true));

    NfsTransferRangeRequest rangeRequest = request(NfsTransferRangeRequest.Operation.UPSERT, 0, 8,
        "range-0", SHA256);
    OMClientResponse first = apply(rangeRequest, 1);
    assertEquals(Status.OK, first.getOMResponse().getStatus());
    assertFalse(metadataManager.getNfsFileLeaseTable().get(leaseKey).getProtobuf().getContentPublished());
    commit(first);

    metadataManager.getNfsFileLeaseTable().addCacheEntry(leaseKey,
        metadataManager.getNfsFileLeaseTable().get(leaseKey).withContentPublished(true), 2);
    OMClientResponse retry = apply(rangeRequest, 3);
    assertEquals(Status.OK, retry.getOMResponse().getStatus());
    assertTrue(metadataManager.getNfsFileLeaseTable().get(leaseKey).getProtobuf().getContentPublished());
  }

  @Test
  void rejectsStaleLeaseAndInvalidStagingKey() throws Exception {
    OMClientResponse stale = apply(request(NfsTransferRangeRequest.Operation.UPSERT, 0, 8, "range-0", SHA256)
        .toBuilder().setOwnerToken(ByteString.copyFromUtf8("stale-owner")).build(), 1);
    assertEquals(Status.NFS_LEASE_CONFLICT, stale.getOMResponse().getStatus());

    OMClientResponse invalid = apply(request(NfsTransferRangeRequest.Operation.UPSERT, 0, 8, "../range-0", SHA256), 2);
    assertEquals(Status.INVALID_REQUEST, invalid.getOMResponse().getStatus());
  }

  private NfsTransferRangeRequest request(NfsTransferRangeRequest.Operation operation, long offset, long length,
      String stagingKey, ByteString sha256) {
    return NfsTransferRangeRequest.newBuilder().setOperation(operation).setVolumeName(VOLUME)
        .setBucketName(BUCKET).setExportUUID(EXPORT_UUID).setObjectID(OBJECT_ID).setGatewayID(GATEWAY)
        .setGatewayGeneration(1).setTransferID(TRANSFER).setLeaseGeneration(1).setOwnerToken(OWNER_TOKEN)
        .setOffset(offset).setLength(length).setStagingKey(stagingKey).setSha256(sha256)
        .setLeaderTimeMillis(1500).build();
  }

  private OmNfsTransferRangeInfo range(long offset, long length, String stagingKey, ByteString sha256) {
    return new OmNfsTransferRangeInfo(EXPORT_UUID, volumeId, bucketId, OBJECT_ID, 1,
        offset, length, TRANSFER, stagingKey, sha256, offset == 0 ? 1 : 3);
  }

  private OmNfsTransferRangeInfo getRange(long offset) throws Exception {
    return getRange(offset, "range-" + offset);
  }

  private OmNfsTransferRangeInfo getRange(long offset, String stagingKey) throws Exception {
    String key = metadataManager.getNfsTransferRangeKey(EXPORT_UUID, volumeId, bucketId, OBJECT_ID, 1, offset,
        stagingKey);
    return metadataManager.getNfsTransferRangeTable().get(key);
  }

  private OMClientResponse apply(NfsTransferRangeRequest rangeRequest, long index) throws Exception {
    OMRequest request = OMRequest.newBuilder().setCmdType(Type.NfsTransferRange)
        .setClientId(UUID.randomUUID().toString()).setNfsTransferRangeRequest(rangeRequest).build();
    return ((OMNfsTransferRangeRequest) OzoneManagerRatisUtils.createClientRequest(request, ozoneManager))
        .validateAndUpdateCache(ozoneManager, index);
  }

  private void commit(OMClientResponse response) throws Exception {
    try (BatchOperation batch = metadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(metadataManager, batch);
      metadataManager.getStore().commitBatchOperation(batch);
    }
  }
}
