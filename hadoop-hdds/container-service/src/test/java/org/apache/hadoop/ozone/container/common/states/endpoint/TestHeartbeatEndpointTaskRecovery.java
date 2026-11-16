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

package org.apache.hadoop.ozone.container.common.states.endpoint;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_ADDRESS_REFRESH_MISSED_COUNT_THRESHOLD;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_RE_REGISTER_MISSED_COUNT_THRESHOLD;
import static org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates.GETVERSION;
import static org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates.HEARTBEAT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Message;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.net.HostAndPort;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.upgrade.DatanodeVersionManager;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.junit.jupiter.api.Test;

/**
 * Tests heartbeat failure recovery that resets only the affected endpoint.
 */
public class TestHeartbeatEndpointTaskRecovery {

  private static final HostAndPort SCM = new HostAndPort("test-scm-1", 9861);

  @Test
  public void thresholdFailureResetsEndpointAndClearsCommandStatusQueue() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(HDDS_HEARTBEAT_RE_REGISTER_MISSED_COUNT_THRESHOLD, 1);
    conf.setInt(HDDS_HEARTBEAT_ADDRESS_REFRESH_MISSED_COUNT_THRESHOLD, 50);
    DatanodeStateMachine dsm = mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, dsm, "");
    StorageContainerDatanodeProtocolClientSideTranslatorPB proxy = failingProxy();
    try (EndpointStateMachine endpoint = new EndpointStateMachine(SCM, proxy, conf, "")) {
      endpoint.setState(HEARTBEAT);
      context.addEndpoint(SCM);
      context.addIncrementalReport(newCommandStatusReport(11L));
      context.addIncrementalReport(IncrementalContainerReportProto.getDefaultInstance());

      newHeartbeatTask(endpoint, conf, context).call();

      assertEquals(GETVERSION, endpoint.getState());
      verify(dsm).setNextHB(anyLong());
      List<Message> pendingReports = context.getAllAvailableReports(SCM);
      assertEquals(1, pendingReports.size());
      assertTrue(pendingReports.get(0) instanceof IncrementalContainerReportProto);
    }
  }

  @Test
  public void belowThresholdKeepsHeartbeatAndCommandStatusQueue() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(HDDS_HEARTBEAT_RE_REGISTER_MISSED_COUNT_THRESHOLD, 2);
    conf.setInt(HDDS_HEARTBEAT_ADDRESS_REFRESH_MISSED_COUNT_THRESHOLD, 50);
    DatanodeStateMachine dsm = mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, dsm, "");
    StorageContainerDatanodeProtocolClientSideTranslatorPB proxy = failingProxy();
    try (EndpointStateMachine endpoint = new EndpointStateMachine(SCM, proxy, conf, "")) {
      endpoint.setState(HEARTBEAT);
      context.addEndpoint(SCM);
      context.addIncrementalReport(newCommandStatusReport(12L));
      context.addIncrementalReport(IncrementalContainerReportProto.getDefaultInstance());

      newHeartbeatTask(endpoint, conf, context).call();

      assertEquals(HEARTBEAT, endpoint.getState());
      verify(dsm, never()).setNextHB(anyLong());
      List<Message> pendingReports = context.getAllAvailableReports(SCM);
      assertEquals(2, pendingReports.size());
      long commandStatusCount = pendingReports.stream()
          .filter(CommandStatusReportsProto.class::isInstance)
          .count();
      assertEquals(1L, commandStatusCount);
    }
  }

  @Test
  public void passiveEndpointDoesNotResetOrClearCommandStatusQueue() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(HDDS_HEARTBEAT_RE_REGISTER_MISSED_COUNT_THRESHOLD, 1);
    conf.setInt(HDDS_HEARTBEAT_ADDRESS_REFRESH_MISSED_COUNT_THRESHOLD, 50);
    DatanodeStateMachine dsm = mockDatanodeStateMachine();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, dsm, "");
    StorageContainerDatanodeProtocolClientSideTranslatorPB proxy = failingProxy();
    try (EndpointStateMachine endpoint = new EndpointStateMachine(SCM, proxy, conf, "")) {
      endpoint.setPassive(true);
      endpoint.setState(HEARTBEAT);
      context.addEndpoint(SCM);
      context.addIncrementalReport(newCommandStatusReport(13L));

      newHeartbeatTask(endpoint, conf, context).call();

      assertEquals(HEARTBEAT, endpoint.getState());
      verify(dsm, never()).setNextHB(anyLong());
      List<Message> pendingReports = context.getAllAvailableReports(SCM);
      assertEquals(1, pendingReports.size());
      assertTrue(pendingReports.get(0) instanceof CommandStatusReportsProto);
    }
  }

  private static HeartbeatEndpointTask newHeartbeatTask(EndpointStateMachine endpoint,
      OzoneConfiguration conf, StateContext context) {
    return HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setContext(context)
        .setEndpointStateMachine(endpoint)
        .build();
  }

  private static StorageContainerDatanodeProtocolClientSideTranslatorPB failingProxy() throws IOException {
    StorageContainerDatanodeProtocolClientSideTranslatorPB proxy =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    when(proxy.sendHeartbeat(any())).thenThrow(new IOException("scm unavailable"));
    return proxy;
  }

  private static CommandStatusReportsProto newCommandStatusReport(long commandId) {
    return CommandStatusReportsProto.newBuilder()
        .addCmdStatus(CommandStatus.newBuilder()
            .setCmdId(commandId)
            .setType(SCMCommandProto.Type.deleteBlocksCommand)
            .setStatus(CommandStatus.Status.EXECUTED)
            .build())
        .build();
  }

  private static DatanodeStateMachine mockDatanodeStateMachine() {
    DatanodeDetails dn = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .setCurrentVersion(HDDSVersion.SOFTWARE_VERSION)
        .build();
    DatanodeStateMachine dsm = mock(DatanodeStateMachine.class);
    when(dsm.getDatanodeDetails()).thenReturn(dn);
    when(dsm.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    DatanodeVersionManager versionManager = ContainerTestUtils.mockDnVersionManager();
    when(dsm.getVersionManager()).thenReturn(versionManager);
    return dsm;
  }
}
