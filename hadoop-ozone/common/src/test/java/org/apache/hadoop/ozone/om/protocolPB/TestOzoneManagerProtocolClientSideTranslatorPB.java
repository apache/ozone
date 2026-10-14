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

package org.apache.hadoop.ozone.om.protocolPB;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.nfs.NfsChildrenPage;
import org.apache.hadoop.ozone.nfs.NfsFileHandle;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.OmDeleteKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyError;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsChildrenRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsChildrenResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsObjectRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsObjectResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCommitRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCreateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneFileStatusProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.StartQuotaRepairRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.StartQuotaRepairResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

class TestOzoneManagerProtocolClientSideTranslatorPB {

  private final OmTransport omTransport = mock(OmTransport.class);
  private final OzoneManagerProtocolClientSideTranslatorPB pb = new OzoneManagerProtocolClientSideTranslatorPB(
      omTransport, "test-client-id");
  private static final OmDeleteKeys DELETE_KEYS = new OmDeleteKeys("vol", "bucket", Arrays.asList("key1", "key2"));

  @Test
  void testNfsFileCommitWireIdentity() throws IOException {
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(OMResponse.newBuilder()
        .setCmdType(Type.CommitKey).setStatus(Status.OK).setSuccess(true).build());
    NfsFileCommitRequest nfsRequest = NfsFileCommitRequest.newBuilder().setExportUUID("export")
        .setGatewayID("gateway").setGatewayGeneration(3).setTransferID("transfer").setFileGeneration(4)
        .setOwnerToken(ByteString.copyFromUtf8("owner-token")).build();
    OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName("vol").setBucketName("bucket").setKeyName("file")
        .setLocationInfoList(Collections.emptyList()).build();

    pb.commitKey(args, 5, nfsRequest);

    ArgumentCaptor<OMRequest> captor = ArgumentCaptor.forClass(OMRequest.class);
    verify(omTransport).submitRequest(captor.capture());
    CommitKeyRequest request = captor.getValue().getCommitKeyRequest();
    assertThat(request.hasNfsFileCommitRequest()).isTrue();
    assertThat(request.getNfsFileCommitRequest()).isEqualTo(nfsRequest);
    assertThat(request.getClientID()).isEqualTo(5);
  }

  @Test
  void testNfsCreateReturnsCommitFence() throws IOException {
    OmKeyInfo keyInfo = new OmKeyInfo.Builder().setVolumeName("vol").setBucketName("bucket")
        .setKeyName("file").setOwnerName("owner")
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.ONE)).build();
    NfsFileCreateResponse lease = NfsFileCreateResponse.newBuilder().setObjectID(7).setGatewayGeneration(3)
        .setGeneration(4).setExpiresAtMillis(1000).setOwnerToken(ByteString.copyFromUtf8("owner-token")).build();
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(OMResponse.newBuilder()
        .setCmdType(Type.CreateFile).setStatus(Status.OK).setSuccess(true)
        .setCreateFileResponse(CreateFileResponse.newBuilder().setID(8).setOpenVersion(1)
            .setKeyInfo(keyInfo.getProtobuf(false, ClientVersion.CURRENT)).setNfsFileCreateResponse(lease)).build());
    NfsFileCreateRequest nfsRequest = NfsFileCreateRequest.newBuilder().setExportUUID("export")
        .setGatewayID("gateway").setGatewayGeneration(3).setTransferID("transfer").setLeaseDurationMillis(1000)
        .build();
    OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName("vol").setBucketName("bucket").setKeyName("file")
        .setOwnerName("owner").build();

    OpenKeySession session = pb.createNfsFile(args, nfsRequest);

    assertThat(session.getNfsFileCreateResponse()).isEqualTo(lease);
    assertThat(session.getNfsFileCommitRequest().getExportUUID()).isEqualTo("export");
    assertThat(session.getNfsFileCommitRequest().getGatewayGeneration()).isEqualTo(3);
    assertThat(session.getNfsFileCommitRequest().getFileGeneration()).isEqualTo(4);
    assertThat(session.getNfsFileCommitRequest().getOwnerToken()).isEqualTo(lease.getOwnerToken());
    ArgumentCaptor<OMRequest> captor = ArgumentCaptor.forClass(OMRequest.class);
    verify(omTransport).submitRequest(captor.capture());
    assertThat(captor.getValue().getCreateFileRequest().getNfsFileCreateRequest()).isEqualTo(nfsRequest);
  }

  @Test
  void testNfsFileLeaseWireIdentity() throws IOException {
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(OMResponse.newBuilder()
        .setCmdType(Type.NfsFileLease).setStatus(Status.OK).setSuccess(true)
        .setNfsFileLeaseResponse(NfsFileLeaseResponse.newBuilder().setGeneration(4).setExpiresAtMillis(5000))
        .build());
    NfsFileLeaseRequest request = NfsFileLeaseRequest.newBuilder()
        .setOperation(NfsFileLeaseRequest.Operation.RENEW).setExportUUID("export").setVolumeName("vol")
        .setBucketName("bucket").setObjectID(7).setGatewayID("gateway").setGatewayGeneration(3)
        .setTransferID("transfer").setGeneration(4).setOwnerToken(ByteString.copyFromUtf8("owner-token"))
        .setLeaseDurationMillis(1000).build();

    NfsFileLeaseResponse response = pb.updateNfsFileLease(request);

    assertThat(response.getGeneration()).isEqualTo(4);
    ArgumentCaptor<OMRequest> captor = ArgumentCaptor.forClass(OMRequest.class);
    verify(omTransport).submitRequest(captor.capture());
    assertThat(captor.getValue().getCmdType()).isEqualTo(Type.NfsFileLease);
    assertThat(captor.getValue().getNfsFileLeaseRequest()).isEqualTo(request);
  }

  @ParameterizedTest
  @EnumSource(NfsFileHandle.Kind.class)
  void testNfsLookupWireIdentity(NfsFileHandle.Kind kind) throws IOException {
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(OMResponse.newBuilder()
        .setCmdType(Type.LookupNfsObject).setStatus(Status.OK).setSuccess(true)
        .setLookupNfsObjectResponse(LookupNfsObjectResponse.newBuilder()
            .setStatus(OzoneFileStatusProto.newBuilder().setIsDirectory(true))).build());
    OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName("vol").setBucketName("bucket").setKeyName("")
        .setHeadOp(true).setSortDatanodesInPipeline(true).setLatestVersionLocation(true).build();
    assertThat(pb.lookupNfsObject(args, Long.MIN_VALUE, -1, Long.MAX_VALUE, kind).isDirectory()).isTrue();
    ArgumentCaptor<OMRequest> captor = ArgumentCaptor.forClass(OMRequest.class);
    verify(omTransport).submitRequest(captor.capture());
    assertThat(captor.getValue().getCmdType()).isEqualTo(Type.LookupNfsObject);
    LookupNfsObjectRequest request = captor.getValue().getLookupNfsObjectRequest();
    assertThat(request.getVolumeName()).isEqualTo("vol");
    assertThat(request.getBucketName()).isEqualTo("bucket");
    assertThat(request.getVolumeID()).isEqualTo(Long.MIN_VALUE);
    assertThat(request.getBucketID()).isEqualTo(-1);
    assertThat(request.getObjectID()).isEqualTo(Long.MAX_VALUE);
    assertThat(request.getKind().name()).isEqualTo(kind.name());
    assertThat(request.getHeadOp()).isTrue();
    assertThat(request.getSortDatanodes()).isTrue();
    assertThat(request.getLatestVersionLocation()).isTrue();
  }

  @Test
  void testNfsLookupRejectsUnsupportedResponse() throws IOException {
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(OMResponse.newBuilder()
        .setCmdType(Type.LookupNfsObject).setStatus(Status.OK).setSuccess(true).build());
    OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName("vol").setBucketName("bucket").setKeyName("").build();
    assertThat(assertThrows(OMException.class,
        () -> pb.lookupNfsObject(args, 1, 2, 3, NfsFileHandle.Kind.FILE)).getResult())
        .isEqualTo(ResultCodes.NOT_SUPPORTED_OPERATION);
  }

  @Test
  void testNfsLookupRejectsPathWithoutRpc() {
    OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName("vol").setBucketName("bucket").setKeyName("file").build();
    assertThat(assertThrows(OMException.class,
        () -> pb.lookupNfsObject(args, 1, 2, 3, NfsFileHandle.Kind.FILE)).getResult())
        .isEqualTo(ResultCodes.INVALID_REQUEST);
    verifyNoInteractions(omTransport);
  }

  @Test
  void testNfsChildrenPageWireIdentity() throws IOException {
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(OMResponse.newBuilder()
        .setCmdType(Type.LookupNfsChildren).setStatus(Status.OK).setSuccess(true)
        .setLookupNfsChildrenResponse(LookupNfsChildrenResponse.newBuilder()
            .addChildren(OzoneFileStatusProto.newBuilder().setIsDirectory(true))
            .setVerifier(-1).setEof(false)).build());
    OmKeyArgs args = new OmKeyArgs.Builder().setVolumeName("vol").setBucketName("bucket").setKeyName("").build();
    NfsChildrenPage page = pb.lookupNfsChildren(args, 1, 2, 3, NfsFileHandle.Kind.DIRECTORY,
        null, 32, 9L, -1L);
    assertThat(page.getChildren()).hasSize(1);
    assertThat(page.getVerifier()).isEqualTo(-1);
    assertThat(page.isEof()).isFalse();
    ArgumentCaptor<OMRequest> captor = ArgumentCaptor.forClass(OMRequest.class);
    verify(omTransport).submitRequest(captor.capture());
    assertThat(captor.getValue().getCmdType()).isEqualTo(Type.LookupNfsChildren);
    LookupNfsChildrenRequest request = captor.getValue().getLookupNfsChildrenRequest();
    assertThat(request.getVolumeID()).isEqualTo(1);
    assertThat(request.getBucketID()).isEqualTo(2);
    assertThat(request.getParentObjectID()).isEqualTo(3);
    assertThat(request.getParentKind()).isEqualTo(LookupNfsChildrenRequest.ParentKind.DIRECTORY);
    assertThat(request.getMaxEntries()).isEqualTo(32);
    assertThat(request.getStartAfterObjectID()).isEqualTo(9L);
    assertThat(request.getVerifier()).isEqualTo(-1);
  }

  @Test
  void testNfsExportLeaseWireRequest() throws IOException {
    String exportUuid = "a7ad8fd1-c88d-4f1b-bf58-38406e931adc";
    NfsExportLeaseResponse lease = NfsExportLeaseResponse.newBuilder().setExportUUID(exportUuid)
        .setVolumeID(1).setBucketID(2).setGatewayID("gateway-a").setGatewayGeneration(3)
        .setGeneration(4).setExpiresAtMillis(5000).build();
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(OMResponse.newBuilder()
        .setCmdType(Type.NfsExportLease).setStatus(Status.OK).setSuccess(true)
        .setNfsExportLeaseResponse(lease).build());
    NfsExportLeaseRequest request = NfsExportLeaseRequest.newBuilder()
        .setOperation(NfsExportLeaseRequest.Operation.RENEW).setExportUUID(exportUuid)
        .setVolumeName("vol").setBucketName("bucket").setGatewayID("gateway-a")
        .setGatewayGeneration(3).setGeneration(4).setLeaseDurationMillis(1000).setLeaderTimeMillis(1).build();

    assertThat(pb.updateNfsExportLease(request)).isEqualTo(lease);
    ArgumentCaptor<OMRequest> captor = ArgumentCaptor.forClass(OMRequest.class);
    verify(omTransport).submitRequest(captor.capture());
    assertThat(captor.getValue().getCmdType()).isEqualTo(Type.NfsExportLease);
    assertThat(captor.getValue().getNfsExportLeaseRequest().hasLeaderTimeMillis()).isFalse();
    assertThat(captor.getValue().getNfsExportLeaseRequest().getGeneration()).isEqualTo(4);
  }

  @Test
  void testStartQuotaRepair() throws IOException {
    StartQuotaRepairResponse response = StartQuotaRepairResponse.newBuilder().build();
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(
        OMResponse.newBuilder()
            .setCmdType(Type.StartQuotaRepair)
            .setStatus(Status.OK)
            .setStartQuotaRepairResponse(response)
            .build());

    ArgumentCaptor<OMRequest> captor = ArgumentCaptor.forClass(OMRequest.class);

    pb.startQuotaRepair(Collections.emptyList());

    verify(omTransport).submitRequest(captor.capture());

    OMRequest request = captor.getValue();
    assertThat(request.getCmdType()).isEqualTo(Type.StartQuotaRepair);

    StartQuotaRepairRequest startQuotaRepairRequest = request.getStartQuotaRepairRequest();
    assertThat(startQuotaRepairRequest.getBucketsList()).isEmpty();
  }

  @Test
  void testStartQuotaRepairWithSpecifiedBuckets() throws IOException {
    StartQuotaRepairResponse response = StartQuotaRepairResponse.newBuilder().build();
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(
        OMResponse.newBuilder()
            .setCmdType(Type.StartQuotaRepair)
            .setStatus(Status.OK)
            .setStartQuotaRepairResponse(response)
            .build());

    ArgumentCaptor<OMRequest> captor = ArgumentCaptor.forClass(OMRequest.class);

    List<String> buckets = Collections.singletonList("Bucket1");

    pb.startQuotaRepair(buckets);

    verify(omTransport).submitRequest(captor.capture());

    OMRequest request = captor.getValue();
    assertThat(request.getCmdType()).isEqualTo(Type.StartQuotaRepair);

    StartQuotaRepairRequest startQuotaRepairRequest = request.getStartQuotaRepairRequest();
    assertThat(startQuotaRepairRequest.getBucketsList()).isEqualTo(buckets);
  }

  @Test
  void testStartQuotaRepairWithNullBuckets() {
    assertThatThrownBy(() -> pb.startQuotaRepair(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("buckets == null");
    verifyNoInteractions(omTransport);
  }

  @Test
  void submitRequestAddsThreadLocalReadConsistencyHint() throws Exception {
    CapturingTransport transport = new CapturingTransport();
    OzoneManagerProtocolClientSideTranslatorPB client =
        new OzoneManagerProtocolClientSideTranslatorPB(transport, "client-id");

    client.setThreadLocalReadConsistency(ReadConsistency.LOCAL_LEASE);

    client.getServiceList();

    assertThat(transport.getLastRequest().hasReadConsistencyHint()).isTrue();
    assertThat(transport.getLastRequest()
        .getReadConsistencyHint()
        .getReadConsistency())
        .isEqualTo(ReadConsistency.LOCAL_LEASE.toProto());
  }

  @Test
  void submitRequestAddsThreadLocalLocalLeaseContext() throws Exception {
    CapturingTransport transport = new CapturingTransport();
    OzoneManagerProtocolClientSideTranslatorPB client =
        new OzoneManagerProtocolClientSideTranslatorPB(transport, "client-id");

    client.setThreadLocalReadConsistency(ReadConsistency.LOCAL_LEASE, 10L, 100L);

    client.getServiceList();

    assertThat(transport.getLastRequest().hasReadConsistencyHint()).isTrue();
    assertThat(transport.getLastRequest()
        .getReadConsistencyHint()
        .getReadConsistency())
        .isEqualTo(ReadConsistency.LOCAL_LEASE.toProto());
    assertThat(transport.getLastRequest()
        .getReadConsistencyHint()
        .hasLocalLeaseContext())
        .isTrue();
    assertThat(transport.getLastRequest()
        .getReadConsistencyHint()
        .getLocalLeaseContext()
        .getLogLimit())
        .isEqualTo(10L);
    assertThat(transport.getLastRequest()
        .getReadConsistencyHint()
        .getLocalLeaseContext()
        .getLeaseTimeMs())
        .isEqualTo(100L);
  }

  @Test
  void submitRequestOmitsReadConsistencyHintByDefault() throws Exception {
    CapturingTransport transport = new CapturingTransport();
    OzoneManagerProtocolClientSideTranslatorPB client =
        new OzoneManagerProtocolClientSideTranslatorPB(transport, "client-id");

    client.getServiceList();

    assertThat(transport.getLastRequest().hasReadConsistencyHint()).isFalse();
  }

  @Test
  void getS3VolumeContextUsesLeaderOnlyWithS3Authentication()
      throws Exception {
    CapturingTransport transport = new CapturingTransport();
    OzoneManagerProtocolClientSideTranslatorPB client =
        new OzoneManagerProtocolClientSideTranslatorPB(transport, "client-id");
    client.setThreadLocalS3Auth(new S3Auth("string-to-sign", "signature",
        "access-id", "user-principal"));
    client.setThreadLocalReadConsistency(ReadConsistency.LOCAL_LEASE);

    client.getS3VolumeContext();

    assertThat(transport.getLastRequest().getReadConsistencyHint()
        .getReadConsistency())
        .isEqualTo(ReadConsistency.LINEARIZABLE_LEADER_ONLY.toProto());
  }

  @Test
  void testQuietDeleteKeysThrowsWhenWholeBatchFails() throws IOException {
    // Whole-batch failure at OM: non-OK status, DeleteKeysResponse has status=false and no per-key errors.
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(
        OMResponse.newBuilder()
            .setCmdType(Type.DeleteKeys)
            .setStatus(Status.BUCKET_NOT_FOUND)
            .setSuccess(false)
            .setMessage("Bucket not found")
            .setDeleteKeysResponse(DeleteKeysResponse.newBuilder().setStatus(false))
            .build());

    OMException e = assertThrows(OMException.class, () -> pb.deleteKeys(DELETE_KEYS, true));

    assertThat(e.getResult()).isEqualTo(ResultCodes.BUCKET_NOT_FOUND);
    assertThat(e).hasMessage("Bucket not found");
  }

  @Test
  void testQuietDeleteKeysReturnsPerKeyErrorsOnPartialDelete() throws IOException {
    DeleteKeyError keyError = DeleteKeyError.newBuilder()
        .setKey("key2")
        .setErrorCode(ResultCodes.ACCESS_DENIED.name())
        .setErrorMsg("ACL check failed")
        .build();
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(
        OMResponse.newBuilder()
            .setCmdType(Type.DeleteKeys)
            .setStatus(Status.PARTIAL_DELETE)
            .setSuccess(false)
            .setDeleteKeysResponse(DeleteKeysResponse.newBuilder().setStatus(false).addErrors(keyError))
            .build());

    Map<String, ErrorInfo> errors = pb.deleteKeys(DELETE_KEYS, true);

    assertThat(errors).hasSize(1).containsKey("key2");
    assertThat(errors.get("key2").getCode()).isEqualTo(ResultCodes.ACCESS_DENIED.name());
    assertThat(errors.get("key2").getMessage()).isEqualTo("ACL check failed");
  }

  @Test
  void testQuietDeleteKeysReturnsEmptyMapOnSuccess() throws IOException {
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(
        OMResponse.newBuilder()
            .setCmdType(Type.DeleteKeys)
            .setStatus(Status.OK)
            .setSuccess(true)
            .setDeleteKeysResponse(DeleteKeysResponse.newBuilder().setStatus(true))
            .build());

    assertThat(pb.deleteKeys(DELETE_KEYS, true)).isEmpty();
  }

  private static final class CapturingTransport implements OmTransport {
    private OMRequest lastRequest;

    @Override
    public OMResponse submitRequest(OMRequest payload) {
      lastRequest = payload;
      return OMResponse.newBuilder()
          .setCmdType(payload.getCmdType())
          .setStatus(Status.OK)
          .setSuccess(true)
          .setServiceListResponse(ServiceListResponse.newBuilder())
          .build();
    }

    @Override
    public Text getDelegationTokenService() {
      return new Text();
    }

    @Override
    public void close() throws IOException {
    }

    private OMRequest getLastRequest() {
      return lastRequest;
    }
  }

}
