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

package org.apache.hadoop.ozone.protocolPB;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.blocksWith;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationInternalInterface;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.om.execution.OMExecutionFlow;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMLockDetailsProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverLeaseResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that the leader adds pipelines to the RecoverLease reply after the request has been applied.
 * */
class TestRecoverLeaseBlockInfo {
  private final OzoneManager om = mock(OzoneManager.class, CALLS_REAL_METHODS);
  private final OMExecutionFlow execution = mock(OMExecutionFlow.class);
  private final OzoneManagerRatisServer ratis = mock(OzoneManagerRatisServer.class);
  private final StorageContainerLocationInternalInterface containerClient =
      mock(StorageContainerLocationInternalInterface.class);
  private OzoneManagerProtocolServerSideTranslatorPB translator;

  @BeforeEach
  void setup() throws Exception {
    final ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.getContainerClient()).thenReturn(containerClient);
    final Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    final ContainerWithPipeline containerWithPipeline = mock(ContainerWithPipeline.class);
    when(containerWithPipeline.getPipeline()).thenReturn(pipeline);
    when(containerClient.getContainerWithPipeline(anyLong())).thenReturn(containerWithPipeline);

    when(om.getConfiguration()).thenReturn(new OzoneConfiguration());
    doNothing().when(om).checkLeaderStatus();
    when(om.getOmExecutionFlow()).thenReturn(execution);
    when(om.getMetadataManager()).thenReturn(mock(OMMetadataManager.class));
    when(om.getScmClient()).thenReturn(scmClient);
    when(om.isGrpcBlockTokenEnabled()).thenReturn(false);
    translator = new OzoneManagerProtocolServerSideTranslatorPB(om, ratis, mock(ProtocolMessageMetrics.class));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testAddsPipelinesToSuccessfulResponse(boolean cacheHit) throws Exception {
    final OMResponse applied = appliedResponse();
    when(ratis.checkRetryCache()).thenReturn(cacheHit ? applied : null);
    when(execution.submit(any(), anyBoolean())).thenReturn(applied);

    final OMResponse result = translator.processRequest(request());

    assertThat(result.getStatus()).isEqualTo(Status.OK);
    assertThat(blocksWith(result.getRecoverLeaseResponse().getKeyInfo(), KeyLocation::hasPipeline))
        .containsExactly(true);
    assertThat(blocksWith(result.getRecoverLeaseResponse().getOpenKeyInfo(), KeyLocation::hasPipeline))
        .containsExactly(true);
    verify(containerClient, times(2)).getContainerWithPipeline(anyLong());
    if (cacheHit) {
      // A retried request gets pipelines too: the reply cached by Ratis does not contain them.
      verify(execution, never()).submit(any(), anyBoolean());
    }
  }

  @Test
  void testDoesNotAddPipelinesToFailedResponse() throws Exception {
    final OMResponse failed = OMResponse.newBuilder().setCmdType(Type.RecoverLease)
        .setStatus(Status.KEY_NOT_FOUND).setSuccess(false)
        .setRecoverLeaseResponse(RecoverLeaseResponse.newBuilder()).build();
    when(execution.submit(any(), anyBoolean())).thenReturn(failed);

    final OMResponse result = translator.processRequest(request());

    assertThat(result).isEqualTo(failed);
    verify(containerClient, never()).getContainerWithPipeline(anyLong());
  }

  @Test
  void testReturnsErrorWhenScmFails() throws Exception {
    final OMLockDetailsProto lockDetails = OMLockDetailsProto.newBuilder().setWriteLockNanos(10).build();
    when(execution.submit(any(), anyBoolean())).thenReturn(
        appliedResponse().toBuilder().setOmLockDetails(lockDetails).build());
    when(containerClient.getContainerWithPipeline(anyLong())).thenThrow(new IOException("SCM is not reachable"));

    final OMResponse result = translator.processRequest(request());

    assertThat(result.getSuccess()).isFalse();
    assertThat(result.getStatus()).isEqualTo(Status.SCM_GET_PIPELINE_EXCEPTION);
    assertThat(result.getMessage()).isEqualTo("SCM is not reachable");
    assertThat(result.getOmLockDetails()).isEqualTo(lockDetails);
  }

  private static OMRequest request() {
    return OMRequest.newBuilder().setCmdType(Type.RecoverLease).setClientId("client")
        .setRecoverLeaseRequest(RecoverLeaseRequest.newBuilder()
            .setVolumeName("volume").setBucketName("bucket").setKeyName("key"))
        .build();
  }

  /** The reply of a successfully applied request: one block per key info, without a pipeline. */
  private static OMResponse appliedResponse() {
    return OMResponse.newBuilder().setCmdType(Type.RecoverLease).setStatus(Status.OK).setSuccess(true)
        .setRecoverLeaseResponse(RecoverLeaseResponse.newBuilder()
            .setKeyInfo(keyInfoWithOneBlock(1000L))
            .setOpenKeyInfo(keyInfoWithOneBlock(1001L)))
        .build();
  }

  private static KeyInfo keyInfoWithOneBlock(long containerID) {
    final KeyLocation block = KeyLocation.newBuilder()
        .setBlockID(HddsProtos.BlockID.newBuilder()
            .setContainerBlockID(HddsProtos.ContainerBlockID.newBuilder()
                .setContainerID(containerID).setLocalID(100L)))
        .setOffset(0).setLength(200).setCreateVersion(0L).build();
    return OMRequestTestUtils.createOmKeyInfo("volume", "bucket", "key",
        RatisReplicationConfig.getInstance(ReplicationFactor.ONE),
        new OmKeyLocationInfoGroup(0L, List.of(OmKeyLocationInfo.getFromProtobuf(block)), false)).build()
        .getNetworkProtobuf(ClientVersion.CURRENT, true);
  }
}
