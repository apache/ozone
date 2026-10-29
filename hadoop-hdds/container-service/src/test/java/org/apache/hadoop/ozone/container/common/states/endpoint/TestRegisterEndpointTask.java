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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.scm.net.HostAndPort;
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
    DatanodeDetails dn = newDatanodeDetails();
    EndPointStates result = runRegistration(dn, errorNodeNotPermitted, true);

    assertNotEquals(EndPointStates.SHUTDOWN, result,
        "Passive (Recon) endpoint errorNodeNotPermitted must not trigger DN shutdown");
    assertEquals(EndPointStates.REGISTER, result,
        "Endpoint should stay in REGISTER state for retry");
  }

  /**
   * HDDS-15482: when SCM (an active endpoint) returns errorNodeNotPermitted
   * due to ZDU version fencing, the datanode must shut down.
   */
  @Test
  public void testActiveEndpointNotPermittedShutdown() throws Exception {
    DatanodeDetails dn = newDatanodeDetails();
    EndPointStates result = runRegistration(dn, errorNodeNotPermitted, false);

    assertEquals(EndPointStates.SHUTDOWN, result,
        "Active (SCM) endpoint errorNodeNotPermitted must trigger DN shutdown");
  }

  /**
   * Sanity check: a successful registration from any endpoint advances state.
   */
  @Test
  public void testSuccessfulRegistrationAdvancesState() throws Exception {
    DatanodeDetails dn = newDatanodeDetails();
    EndPointStates result = runRegistration(dn, success, false);

    assertEquals(EndPointStates.HEARTBEAT, result);
  }

  private EndPointStates runRegistration(DatanodeDetails dn,
      SCMRegisteredResponseProto.ErrorCode errorCode, boolean passive) throws Exception {
    StorageContainerDatanodeProtocolClientSideTranslatorPB rpc =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    when(rpc.register(any(), any(), any(), any(), any()))
        .thenReturn(registrationResponse(dn.getUuidString(), errorCode));

    EndpointStateMachine endpointStateMachine = buildEndpointStateMachine(rpc, passive);
    RegisterEndpointTask task = buildTask(dn, endpointStateMachine);
    return task.call();
  }

  private RegisterEndpointTask buildTask(DatanodeDetails dn,
      EndpointStateMachine endpointStateMachine) throws Exception {
    DatanodeVersionManager versionManager = mock(DatanodeVersionManager.class);
    when(versionManager.getSoftwareVersion()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    when(versionManager.getApparentVersion()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    when(versionManager.getVersionForClient()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    DatanodeStateMachine datanodeStateMachine = mock(DatanodeStateMachine.class);
    when(datanodeStateMachine.getDatanodeDetails()).thenReturn(dn);
    when(datanodeStateMachine.getVersionManager()).thenReturn(versionManager);

    OzoneConfiguration conf = new OzoneConfiguration();
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, datanodeStateMachine, "");

    ContainerController controller = mock(ContainerController.class);
    doReturn(ContainerReportsProto.getDefaultInstance()).when(controller).getContainerReport();
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getController()).thenReturn(controller);
    doReturn(NodeReportProto.getDefaultInstance()).when(ozoneContainer).getNodeReport();
    when(ozoneContainer.getPipelineReport()).thenReturn(PipelineReportsProto.getDefaultInstance());

    return new RegisterEndpointTask(endpointStateMachine, ozoneContainer, context);
  }

  private EndpointStateMachine buildEndpointStateMachine(
      StorageContainerDatanodeProtocolClientSideTranslatorPB rpc, boolean passive) {
    EndpointStateMachine machine = new EndpointStateMachine(
        new HostAndPort("localhost", 9861), rpc, new OzoneConfiguration(), "test-");
    machine.setPassive(passive);
    machine.setState(EndPointStates.REGISTER);
    return machine;
  }

  private static DatanodeDetails newDatanodeDetails() {
    return DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .build();
  }

  private static SCMRegisteredResponseProto registrationResponse(
      String datanodeUUID, SCMRegisteredResponseProto.ErrorCode errorCode) {
    return SCMRegisteredResponseProto.newBuilder()
        .setErrorCode(errorCode)
        .setDatanodeUUID(datanodeUUID)
        .setClusterID(CLUSTER_ID)
        .build();
  }
}
