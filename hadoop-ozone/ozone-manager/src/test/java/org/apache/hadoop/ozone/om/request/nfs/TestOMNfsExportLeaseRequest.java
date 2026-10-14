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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestOMNfsExportLeaseRequest {
  private static final String EXPORT_UUID = "a7ad8fd1-c88d-4f1b-bf58-38406e931adc";
  private static final String VOLUME = "volume";
  private static final String BUCKET = "bucket";

  @TempDir
  private java.io.File folder;
  private OMMetadataManager metadataManager;
  private OzoneManager ozoneManager;

  @BeforeEach
  void setup() throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_OM_DB_DIRS, folder.getAbsolutePath());
    metadataManager = spy(new OmMetadataManagerImpl(configuration, null));
    doReturn(true).when(metadataManager).isNfsWriterLeaseEnabled();
    doReturn(true).when(metadataManager).isNfsObjectIndexReady();
    OMRequestTestUtils.addVolumeToDB(VOLUME, metadataManager);
    OMRequestTestUtils.addBucketToDB(VOLUME, BUCKET, metadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);

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
  void acquireRenewReleaseAndTakeoverAreReplicatedInTheTable() throws Exception {
    OMClientResponse acquired = apply(request(NfsExportLeaseRequest.Operation.ACQUIRE, "gateway-a", 0, 0,
        1000, 1000), 1);
    assertEquals(Status.OK, acquired.getOMResponse().getStatus());
    OmNfsLeaseInfo first = metadataManager.getNfsExportLeaseTable().get(EXPORT_UUID);
    assertEquals(1, first.getProtobuf().getGeneration());
    assertEquals(1, first.getProtobuf().getGatewayGeneration());
    commit(acquired);

    OMClientResponse conflict = apply(request(NfsExportLeaseRequest.Operation.ACQUIRE, "gateway-b", 0, 0,
        1999, 1000), 2);
    assertEquals(Status.NFS_LEASE_CONFLICT, conflict.getOMResponse().getStatus());
    assertEquals("gateway-a", metadataManager.getNfsExportLeaseTable().get(EXPORT_UUID).getProtobuf().getGatewayID());

    OMClientResponse takeover = apply(request(NfsExportLeaseRequest.Operation.ACQUIRE, "gateway-b", 0, 0,
        2000, 1000), 3);
    assertEquals(Status.OK, takeover.getOMResponse().getStatus());
    OmNfsLeaseInfo second = metadataManager.getNfsExportLeaseTable().get(EXPORT_UUID);
    assertEquals(2, second.getProtobuf().getGeneration());
    assertEquals(2, second.getProtobuf().getGatewayGeneration());
    commit(takeover);

    OMClientResponse renewed = apply(request(NfsExportLeaseRequest.Operation.RENEW, "gateway-b", 2, 2,
        2100, 1000), 4);
    assertEquals(Status.OK, renewed.getOMResponse().getStatus());
    assertEquals(3100, metadataManager.getNfsExportLeaseTable().get(EXPORT_UUID).getProtobuf().getExpiresAtMillis());
    commit(renewed);

    OMClientResponse released = apply(request(NfsExportLeaseRequest.Operation.RELEASE, "gateway-b", 2, 2,
        2200, 0), 5);
    assertEquals(Status.OK, released.getOMResponse().getStatus());
    assertEquals(3, metadataManager.getNfsExportLeaseTable().get(EXPORT_UUID).getProtobuf().getGeneration());
    commit(released);
  }

  @Test
  void rejectsInvalidDurationAndNonFsoBucketWithoutWritingLease() throws Exception {
    OMClientResponse invalid = apply(request(NfsExportLeaseRequest.Operation.ACQUIRE, "gateway-a", 0, 0,
        1000, 60_001), 1);
    assertEquals(Status.INVALID_REQUEST, invalid.getOMResponse().getStatus());

    OMRequestTestUtils.addBucketToDB(VOLUME, "object-store", metadataManager, BucketLayout.OBJECT_STORE);
    NfsExportLeaseRequest nonFsoRequest = NfsExportLeaseRequest.newBuilder()
        .setOperation(NfsExportLeaseRequest.Operation.ACQUIRE).setExportUUID(EXPORT_UUID)
        .setVolumeName(VOLUME).setBucketName("object-store").setGatewayID("gateway-a")
        .setLeaderTimeMillis(1000).setLeaseDurationMillis(1000).build();
    OMRequest request = OMRequest.newBuilder().setCmdType(Type.NfsExportLease).setClientId(UUID.randomUUID().toString())
        .setNfsExportLeaseRequest(nonFsoRequest).build();
    OMClientResponse response = createRequest(request).validateAndUpdateCache(ozoneManager, 2);
    assertEquals(Status.NOT_SUPPORTED_OPERATION, response.getOMResponse().getStatus());
    assertNull(metadataManager.getNfsExportLeaseTable().get(EXPORT_UUID));
  }

  @Test
  void rejectsRequestBeforeWriterLeaseFeatureIsFinalized() throws Exception {
    doReturn(false).when(metadataManager).isNfsWriterLeaseEnabled();
    OMClientResponse response = apply(request(NfsExportLeaseRequest.Operation.ACQUIRE, "gateway-a", 0, 0,
        1000, 1000), 1);
    assertEquals(Status.NOT_SUPPORTED_OPERATION, response.getOMResponse().getStatus());
    assertNull(metadataManager.getNfsExportLeaseTable().get(EXPORT_UUID));
  }

  private NfsExportLeaseRequest request(NfsExportLeaseRequest.Operation operation, String gatewayId,
      long gatewayGeneration, long generation, long now, long duration) {
    NfsExportLeaseRequest.Builder builder = NfsExportLeaseRequest.newBuilder().setOperation(operation)
        .setExportUUID(EXPORT_UUID).setVolumeName(VOLUME).setBucketName(BUCKET).setGatewayID(gatewayId)
        .setLeaderTimeMillis(now);
    if (operation != NfsExportLeaseRequest.Operation.ACQUIRE) {
      builder.setGatewayGeneration(gatewayGeneration).setGeneration(generation);
    }
    if (operation != NfsExportLeaseRequest.Operation.RELEASE) {
      builder.setLeaseDurationMillis(duration);
    }
    return builder.build();
  }

  private OMClientResponse apply(NfsExportLeaseRequest leaseRequest, long index) throws Exception {
    OMRequest request = OMRequest.newBuilder().setCmdType(Type.NfsExportLease).setClientId(UUID.randomUUID().toString())
        .setNfsExportLeaseRequest(leaseRequest).build();
    return createRequest(request).validateAndUpdateCache(ozoneManager, index);
  }

  private OMNfsExportLeaseRequest createRequest(OMRequest request) throws Exception {
    return (OMNfsExportLeaseRequest) OzoneManagerRatisUtils.createClientRequest(request, ozoneManager);
  }

  private void commit(OMClientResponse response) throws Exception {
    try (BatchOperation batch = metadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(metadataManager, batch);
      metadataManager.getStore().commitBatchOperation(batch);
    }
  }
}
