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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketDeletedBytes;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.OmDeleteKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyError;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetBucketDeletedBytesResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetObjectTaggingResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.StartQuotaRepairRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.StartQuotaRepairResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestOzoneManagerProtocolClientSideTranslatorPB {

  private final OmTransport omTransport = mock(OmTransport.class);
  private final OzoneManagerProtocolClientSideTranslatorPB pb = new OzoneManagerProtocolClientSideTranslatorPB(
      omTransport, "test-client-id");
  private static final OmDeleteKeys DELETE_KEYS = new OmDeleteKeys("vol", "bucket", Arrays.asList("key1", "key2"));

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
  void getObjectTaggingSendsS3ContextFlag() throws IOException {
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(
        OMResponse.newBuilder()
            .setCmdType(Type.GetObjectTagging)
            .setStatus(Status.OK)
            .setGetObjectTaggingResponse(GetObjectTaggingResponse.newBuilder())
            .build());

    ArgumentCaptor<OMRequest> captor = ArgumentCaptor.forClass(OMRequest.class);
    pb.getObjectTagging(new OmKeyArgs.Builder()
        .setVolumeName(null)
        .setBucketName("bucket")
        .setKeyName("key")
        .setAssumeS3Context(true)
        .build());

    verify(omTransport).submitRequest(captor.capture());
    assertThat(captor.getValue().getAssumeS3Context()).isTrue();
  }

  @Test
  void abortMultipartUploadSetsS3Context() throws IOException {
    CapturingTransport transport = new CapturingTransport();
    OzoneManagerProtocolClientSideTranslatorPB client =
        new OzoneManagerProtocolClientSideTranslatorPB(transport, "client-id");
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName("s3v")
        .setBucketName("bucket")
        .setKeyName("key")
        .setMultipartUploadID("upload-id")
        .build();

    client.abortMultipartUpload(keyArgs, true);

    assertThat(transport.getLastRequest().getCmdType()).isEqualTo(Type.AbortMultiPartUpload);
    assertThat(transport.getLastRequest().getAssumeS3Context()).isTrue();
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

  @Test
  void testGetBucketDeletedBytesFromOlderOmWithoutPurgeableBytesSplit() throws IOException {
    // Older OM does not set purgeableKeyBytes / purgeableDirBytes in the response.
    GetBucketDeletedBytesResponse response = GetBucketDeletedBytesResponse.newBuilder()
        .setSnapshotTrappedBytes(100L)
        .setPurgeableBytes(200L)
        .setSnapshotTrappedKeys(1L)
        .setPurgeableKeys(2L)
        .setSnapshotTrappedDirs(3L)
        .setPurgeableDirs(4L)
        .build();
    when(omTransport.submitRequest(any(OMRequest.class))).thenReturn(
        OMResponse.newBuilder()
            .setCmdType(Type.GetBucketDeletedBytes)
            .setStatus(Status.OK)
            .setGetBucketDeletedBytesResponse(response)
            .build());

    BucketDeletedBytes bytes = pb.getBucketDeletedBytes("/vol/bucket");

    assertThat(bytes.getSnapshotTrappedBytes()).isEqualTo(100L);
    assertThat(bytes.getPurgeableBytes()).isEqualTo(200L);
    assertThat(bytes.getSnapshotTrappedKeys()).isEqualTo(1L);
    assertThat(bytes.getPurgeableKeys()).isEqualTo(2L);
    assertThat(bytes.getSnapshotTrappedDirs()).isEqualTo(3L);
    assertThat(bytes.getPurgeableDirs()).isEqualTo(4L);
    assertThat(bytes.getPurgeableKeyBytes()).isEqualTo(-1L);
    assertThat(bytes.getPurgeableDirBytes()).isEqualTo(-1L);
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
