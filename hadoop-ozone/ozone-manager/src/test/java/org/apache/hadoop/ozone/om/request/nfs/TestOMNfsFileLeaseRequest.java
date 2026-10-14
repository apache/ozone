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
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestOMNfsFileLeaseRequest {
  private static final String EXPORT_UUID = "a7ad8fd1-c88d-4f1b-bf58-38406e931adc";
  private static final String VOLUME = "volume";
  private static final String BUCKET = "bucket";
  private static final long OBJECT_ID = 100;
  private static final String GATEWAY = "gateway";
  private static final String TRANSFER = "transfer";
  private static final ByteString OWNER_TOKEN = ByteString.copyFromUtf8("owner-token");

  @TempDir
  private java.io.File folder;
  private OMMetadataManager metadataManager;
  private OzoneManager ozoneManager;
  private long volumeId;
  private long bucketId;
  private OmNfsLeaseInfo exportLease;

  @BeforeEach
  void setup() throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_OM_DB_DIRS, folder.getAbsolutePath());
    metadataManager = spy(new OmMetadataManagerImpl(configuration, null));
    doReturn(true).when(metadataManager).isNfsWriterLeaseEnabled();
    doReturn(true).when(metadataManager).isNfsObjectIndexReady();
    OMRequestTestUtils.addVolumeToDB(VOLUME, metadataManager);
    OMRequestTestUtils.addBucketToDB(VOLUME, BUCKET, metadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    volumeId = metadataManager.getVolumeId(VOLUME);
    bucketId = metadataManager.getBucketId(VOLUME, BUCKET);
    exportLease = OmNfsLeaseInfo.createExportLease(EXPORT_UUID, volumeId, bucketId, 1, 5000, GATEWAY, 1);
    OmNfsLeaseInfo fileLease = OmNfsLeaseInfo.acquireFileLease(null, exportLease, OBJECT_ID, TRANSFER,
        "nfs-user@EXAMPLE.COM", OWNER_TOKEN, 1000, 1000);
    metadataManager.getNfsExportLeaseTable().put(EXPORT_UUID, exportLease);
    metadataManager.getNfsFileLeaseTable().put(metadataManager.getNfsFileLeaseKey(volumeId, bucketId, OBJECT_ID),
        fileLease);
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
  void renewAndReleaseAreReplicatedAndFenceFormerOwner() throws Exception {
    OMClientResponse renewed = apply(request(NfsFileLeaseRequest.Operation.RENEW, 1, 1500, 1000), 1);
    assertEquals(Status.OK, renewed.getOMResponse().getStatus());
    assertEquals(2500, fileLease().getProtobuf().getExpiresAtMillis());
    commit(renewed);

    OMClientResponse released = apply(request(NfsFileLeaseRequest.Operation.RELEASE, 1, 2000, 0), 2);
    assertEquals(Status.OK, released.getOMResponse().getStatus());
    assertEquals(2, fileLease().getProtobuf().getGeneration());
    commit(released);

    OMClientResponse staleRenew = apply(request(NfsFileLeaseRequest.Operation.RENEW, 1, 2600, 1000), 3);
    assertEquals(Status.NFS_LEASE_CONFLICT, staleRenew.getOMResponse().getStatus());
  }

  @Test
  void rejectsWrongOwnerAndExpiredFileLease() throws Exception {
    OMClientResponse wrongOwner = apply(request(NfsFileLeaseRequest.Operation.RENEW, 1, 2000, 1000)
        .toBuilder().setOwnerToken(ByteString.copyFromUtf8("wrong-token")).build(), 1);
    assertEquals(Status.NFS_LEASE_CONFLICT, wrongOwner.getOMResponse().getStatus());
    OMClientResponse expired = apply(request(NfsFileLeaseRequest.Operation.RENEW, 1, 2100, 1000), 2);
    assertEquals(Status.NFS_LEASE_CONFLICT, expired.getOMResponse().getStatus());
  }

  @Test
  void rejectsAnExportLeaseFromAFormerGatewayGeneration() throws Exception {
    OMClientResponse stale = apply(request(NfsFileLeaseRequest.Operation.RENEW, 1, 2000, 1000)
        .toBuilder().setGatewayGeneration(2).build(), 1);
    assertEquals(Status.NFS_LEASE_CONFLICT, stale.getOMResponse().getStatus());
  }

  @Test
  void recoveryMovesLeaseToNewGatewayAndRotatesItsToken() throws Exception {
    OmNfsLeaseInfo newExport = replaceExportLease(5000);
    ByteString newToken = ByteString.copyFrom(new byte[16]);
    NfsFileLeaseRequest recovery = recoveryRequest(newExport, "nfs-user@EXAMPLE.COM", newToken, 5001);

    OMClientResponse response = apply(recovery, 1);

    assertEquals(Status.OK, response.getOMResponse().getStatus());
    assertEquals(2, fileLease().getProtobuf().getGeneration());
    assertEquals("gateway-b", fileLease().getProtobuf().getGatewayID());
    assertEquals("transfer", fileLease().getProtobuf().getTransferID());
    assertEquals("nfs-user@EXAMPLE.COM", fileLease().getOwnerPrincipal());
    assertEquals(newToken, fileLease().getProtobuf().getOwnerToken());
    assertEquals("transfer", response.getOMResponse().getNfsFileLeaseResponse().getTransferID());
    assertEquals(newToken, response.getOMResponse().getNfsFileLeaseResponse().getOwnerToken());
    commit(response);

    OMClientResponse formerOwner = apply(request(NfsFileLeaseRequest.Operation.RENEW, 1, 5002, 1000), 2);
    assertEquals(Status.NFS_LEASE_CONFLICT, formerOwner.getOMResponse().getStatus());
  }

  @Test
  void recoveryRejectsAnotherPrincipal() throws Exception {
    OmNfsLeaseInfo newExport = replaceExportLease(5000);
    NfsFileLeaseRequest recovery = recoveryRequest(newExport, "other-user@EXAMPLE.COM",
        ByteString.copyFrom(new byte[16]), 5001);

    OMClientResponse response = apply(recovery, 1);

    assertEquals(Status.NFS_LEASE_CONFLICT, response.getOMResponse().getStatus());
    assertEquals(1, fileLease().getProtobuf().getGeneration());
  }

  @Test
  void recoveryReportsWhenNoWriterLeaseExists() throws Exception {
    OmNfsLeaseInfo newExport = replaceExportLease(5000);
    metadataManager.getNfsFileLeaseTable().delete(
        metadataManager.getNfsFileLeaseKey(volumeId, bucketId, OBJECT_ID));

    OMClientResponse response = apply(recoveryRequest(newExport, "nfs-user@EXAMPLE.COM",
        ByteString.copyFrom(new byte[16]), 5001), 1);

    assertEquals(Status.FILE_NOT_FOUND, response.getOMResponse().getStatus());
  }

  private OmNfsLeaseInfo replaceExportLease(long now) throws Exception {
    OmNfsLeaseInfo newExport = OmNfsLeaseInfo.acquireExportLease(
        exportLease, EXPORT_UUID, volumeId, bucketId, "gateway-b", now, 5000);
    metadataManager.getNfsExportLeaseTable().put(EXPORT_UUID, newExport);
    return newExport;
  }

  private NfsFileLeaseRequest recoveryRequest(OmNfsLeaseInfo newExport, String principal, ByteString newToken,
      long now) {
    return NfsFileLeaseRequest.newBuilder().setOperation(NfsFileLeaseRequest.Operation.RECOVER)
        .setExportUUID(EXPORT_UUID).setVolumeName(VOLUME).setBucketName(BUCKET).setObjectID(OBJECT_ID)
        .setGatewayID("gateway-b").setGatewayGeneration(newExport.getProtobuf().getGatewayGeneration())
        .setTransferID("").setGeneration(0).setOwnerToken(newToken).setRequesterPrincipal(principal)
        .setLeaseDurationMillis(1000).setLeaderTimeMillis(now).build();
  }

  private NfsFileLeaseRequest request(NfsFileLeaseRequest.Operation operation, long generation,
      long now, long duration) {
    NfsFileLeaseRequest.Builder builder = NfsFileLeaseRequest.newBuilder().setOperation(operation)
        .setExportUUID(EXPORT_UUID).setVolumeName(VOLUME).setBucketName(BUCKET).setObjectID(OBJECT_ID)
        .setGatewayID(GATEWAY).setGatewayGeneration(1).setTransferID(TRANSFER).setGeneration(generation)
        .setOwnerToken(OWNER_TOKEN).setLeaderTimeMillis(now);
    if (operation == NfsFileLeaseRequest.Operation.RENEW) {
      builder.setLeaseDurationMillis(duration);
    }
    return builder.build();
  }

  private OMClientResponse apply(NfsFileLeaseRequest leaseRequest, long index) throws Exception {
    OMRequest request = OMRequest.newBuilder().setCmdType(Type.NfsFileLease)
        .setClientId(UUID.randomUUID().toString()).setNfsFileLeaseRequest(leaseRequest).build();
    return ((OMNfsFileLeaseRequest) OzoneManagerRatisUtils.createClientRequest(request, ozoneManager))
        .validateAndUpdateCache(ozoneManager, index);
  }

  private OmNfsLeaseInfo fileLease() throws Exception {
    String leaseKey = metadataManager.getNfsFileLeaseKey(volumeId, bucketId, OBJECT_ID);
    return metadataManager.getNfsFileLeaseTable().get(leaseKey);
  }

  private void commit(OMClientResponse response) throws Exception {
    try (BatchOperation batch = metadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(metadataManager, batch);
      metadataManager.getStore().commitBatchOperation(batch);
    }
  }
}
