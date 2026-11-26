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

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode.errorNodeNotPermitted;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode.success;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode;
import org.apache.hadoop.hdds.scm.net.HostAndPort;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.upgrade.DatanodeVersionManager;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for RegisterEndpointTask.
 */
public class TestRegisterEndpointTask {

  private static final String CLUSTER_ID = UUID.randomUUID().toString();

  /**
   * HDDS-17560: when Recon (a passive endpoint) returns errorNodeNotPermitted
   * for any reason (e.g. InvalidTopologyException), the datanode must NOT shut
   * down. The endpoint should stay in REGISTER so the next cycle retries.
   */
  @Test
  public void testPassiveEndpointNotPermittedDoesNotShutdown() throws Exception {
    assertEquals(EndPointStates.REGISTER, runRegistration(errorNodeNotPermitted, true),
        "Passive (Recon) endpoint errorNodeNotPermitted must not shut down the DN and must stay in REGISTER");
  }

  /**
   * HDDS-15482: when SCM (an active endpoint) returns errorNodeNotPermitted
   * due to ZDU version fencing, the datanode must shut down.
   */
  @Test
  public void testActiveEndpointNotPermittedShutdown() throws Exception {
    assertEquals(EndPointStates.SHUTDOWN, runRegistration(errorNodeNotPermitted, false),
        "Active (SCM) endpoint errorNodeNotPermitted must trigger DN shutdown");
  }

  /**
   * Sanity check: a successful registration from any endpoint advances state.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSuccessfulRegistrationAdvancesState(boolean passive) throws Exception {
    assertEquals(EndPointStates.HEARTBEAT, runRegistration(success, passive));
  }

  private EndPointStates runRegistration(ErrorCode errorCode, boolean passive) throws Exception {
    DatanodeDetails dn = ContainerTestUtils.createDatanodeDetails();
    StorageContainerDatanodeProtocolClientSideTranslatorPB rpc =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    when(rpc.register(any(), any(), any(), any(), any())).thenReturn(
        SCMRegisteredResponseProto.newBuilder()
            .setErrorCode(errorCode)
            .setDatanodeUUID(dn.getUuidString())
            .setClusterID(CLUSTER_ID)
            .build());

    EndpointStateMachine endpoint = new EndpointStateMachine(
        new HostAndPort("localhost", 9861), rpc, new OzoneConfiguration(), "test-");
    endpoint.setPassive(passive);
    endpoint.setState(EndPointStates.REGISTER);
    return new RegisterEndpointTask(endpoint, mockOzoneContainer(), newStateContext(dn)).call();
  }

  private static StateContext newStateContext(DatanodeDetails dn) {
    DatanodeStateMachine datanodeStateMachine = mock(DatanodeStateMachine.class);
    when(datanodeStateMachine.getDatanodeDetails()).thenReturn(dn);
    DatanodeVersionManager versionManager = ContainerTestUtils.mockDnVersionManager();
    when(datanodeStateMachine.getVersionManager()).thenReturn(versionManager);
    return new StateContext(new OzoneConfiguration(), DatanodeStates.RUNNING, datanodeStateMachine, "");
  }

  private static OzoneContainer mockOzoneContainer() throws IOException {
    ContainerController controller = mock(ContainerController.class);
    when(controller.getContainerReport()).thenReturn(ContainerReportsProto.getDefaultInstance());
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getNodeReport()).thenReturn(NodeReportProto.getDefaultInstance());
    when(ozoneContainer.getPipelineReport()).thenReturn(PipelineReportsProto.getDefaultInstance());
    return ozoneContainer;
  }
}
