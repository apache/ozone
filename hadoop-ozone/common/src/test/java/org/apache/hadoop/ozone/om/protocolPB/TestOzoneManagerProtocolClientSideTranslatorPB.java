package org.apache.hadoop.ozone.om.protocolPB;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.StartQuotaRepairRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.StartQuotaRepairResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestOzoneManagerProtocolClientSideTranslatorPB {

  private final OmTransport omTransport = mock(OmTransport.class);
  private final OzoneManagerProtocolClientSideTranslatorPB pb = new OzoneManagerProtocolClientSideTranslatorPB(
      omTransport, "test-client-id");

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

}
