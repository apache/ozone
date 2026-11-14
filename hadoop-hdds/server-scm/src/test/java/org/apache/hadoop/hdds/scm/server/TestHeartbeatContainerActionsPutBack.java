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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.scm.events.SCMEvents.CONTAINER_ACTIONS;
import static org.apache.hadoop.hdds.scm.upgrade.ScmUpgradeTestUtils.mockVersionManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.HostAndPort;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerActionsFromDatanode;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.states.endpoint.HeartbeatEndpointTask;
import org.apache.hadoop.ozone.container.upgrade.DatanodeVersionManager;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test for HDDS-17267: a container CLOSE action which the datanode took into a heartbeat must reach SCM
 * even when SCM does not process that heartbeat, because the reply is a re-register command (SCM restarted and does
 * not know the datanode yet) or because the heartbeat RPC fails with an IOException. The CLOSE action for a full
 * container is only created again by a later write, so a lost one can leave the container OPEN.
 *
 * <p>The datanode side is a real {@link StateContext}, {@link EndpointStateMachine} and
 * {@link HeartbeatEndpointTask}; the SCM side is a real {@link SCMNodeManager} and
 * {@link SCMDatanodeHeartbeatDispatcher}. The RPC proxy is a mock that hands the request to the dispatcher and builds
 * the reply the way {@code SCMDatanodeProtocolServer.sendHeartbeat} does. An SCM restart is a new node manager and
 * dispatcher.
 */
public class TestHeartbeatContainerActionsPutBack {

  private static final HostAndPort SCM_ADDRESS = new HostAndPort("scm-1", 9861);

  private final OzoneConfiguration conf = new OzoneConfiguration();
  private final DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
  private final List<Long> closeActionsReceivedBySCM = new ArrayList<>();
  private final AtomicReference<SCMNodeManager> nodeManager = new AtomicReference<>();
  private final AtomicReference<SCMDatanodeHeartbeatDispatcher> dispatcher = new AtomicReference<>();
  private final AtomicReference<SCMHeartbeatRequestProto> lastRequest = new AtomicReference<>();
  private final AtomicReference<IOException> nextFailure = new AtomicReference<>();
  private final StorageContainerDatanodeProtocolClientSideTranslatorPB proxy =
      mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
  private StateContext context;
  private EndpointStateMachine endpoint;
  private HeartbeatEndpointTask task;

  @BeforeEach
  public void setUp() throws Exception {
    startScm();
    // RETURNS_MOCKS: the queued command counts are a mock that returns 0 for every command type
    DatanodeStateMachine dsm = mock(DatanodeStateMachine.class, RETURNS_MOCKS);
    DatanodeVersionManager versionManager = mock(DatanodeVersionManager.class);
    when(versionManager.getSoftwareVersion()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    when(versionManager.getApparentVersion()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    when(versionManager.getVersionForClient()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    when(dsm.getDatanodeDetails()).thenReturn(datanode);
    when(dsm.getVersionManager()).thenReturn(versionManager);
    context = new StateContext(conf, DatanodeStates.RUNNING, dsm, "");
    context.addEndpoint(SCM_ADDRESS);
    // Hands the request to the dispatcher of the current SCM and builds the reply like SCMDatanodeProtocolServer.
    when(proxy.sendHeartbeat(any())).thenAnswer(invocation -> {
      SCMHeartbeatRequestProto request = invocation.getArgument(0);
      lastRequest.set(request);
      IOException failure = nextFailure.getAndSet(null);
      if (failure != null) {
        throw failure;
      }
      SCMHeartbeatResponseProto.Builder response = SCMHeartbeatResponseProto.newBuilder()
          .setDatanodeUUID(request.getDatanodeDetails().getUuid());
      for (SCMCommand<?> cmd : dispatcher.get().dispatch(request)) {
        response.addCommands(SCMDatanodeProtocolServer.getCommandResponse(cmd, null));
      }
      return response.build();
    });
    endpoint = new EndpointStateMachine(SCM_ADDRESS, proxy, conf, "");
    task = HeartbeatEndpointTask.newBuilder().setConfig(conf).setContext(context).setEndpointStateMachine(endpoint)
        .build();
    registerWithScm();
  }

  @AfterEach
  public void tearDown() throws IOException {
    endpoint.close();
    nodeManager.get().close();
  }

  @Test
  public void closeActionResentAfterScmAsksToReregister() throws Exception {
    // Control: a registered SCM receives the CLOSE action.
    raiseCloseAction(1L);
    sendHeartbeat();
    assertThat(closeActionsReceivedBySCM).containsExactly(1L);

    // SCM restarts and no longer knows the datanode. The next write raises CLOSE for container 2.
    nodeManager.get().close();
    startScm();
    raiseCloseAction(2L);
    SCMHeartbeatRequestProto sent = sendHeartbeat();
    assertThat(sent.getContainerActions().getContainerActionsList()).extracting(ContainerAction::getContainerID)
        .containsExactly(2L);
    assertThat(endpoint.getState()).isEqualTo(EndPointStates.GETVERSION);
    // SCM did not process that heartbeat, so the datanode keeps the action.
    assertThat(context.getContainerActionQueueSize()).containsEntry(SCM_ADDRESS, 1);

    // The datanode registers again and heartbeats; no further write reaches container 2.
    registerWithScm();
    for (int i = 0; i < 3; i++) {
      sendHeartbeat();
    }
    // The CLOSE action for container 2 reaches SCM, once.
    assertThat(closeActionsReceivedBySCM).containsExactly(1L, 2L);
    assertThat(context.getContainerActionQueueSize()).containsEntry(SCM_ADDRESS, 0);
  }

  @Test
  public void closeActionResentAfterHeartbeatFails() throws Exception {
    raiseCloseAction(1L);
    nextFailure.set(new IOException("Injected heartbeat failure"));
    SCMHeartbeatRequestProto sent = sendHeartbeat();
    assertThat(sent.getContainerActions().getContainerActionsList()).extracting(ContainerAction::getContainerID)
        .containsExactly(1L);
    // The heartbeat failed, so the datanode keeps the action.
    assertThat(context.getContainerActionQueueSize()).containsEntry(SCM_ADDRESS, 1);

    for (int i = 0; i < 3; i++) {
      sendHeartbeat();
    }
    // The CLOSE action reaches SCM, once.
    assertThat(closeActionsReceivedBySCM).containsExactly(1L);
    assertThat(context.getContainerActionQueueSize()).containsEntry(SCM_ADDRESS, 0);
  }

  /** A new node manager and dispatcher, which is the in-memory state of SCM after a restart. */
  private void startScm() {
    SCMStorageConfig storageConfig = mock(SCMStorageConfig.class);
    when(storageConfig.getClusterID()).thenReturn("cluster1");
    EventPublisher publisher = new EventPublisher() {
      @Override
      public <PAYLOAD, EVENT extends Event<PAYLOAD>> void fireEvent(EVENT event, PAYLOAD payload) {
        if (event == CONTAINER_ACTIONS) {
          ((ContainerActionsFromDatanode) payload).getReport().getContainerActionsList()
              .forEach(a -> closeActionsReceivedBySCM.add(a.getContainerID()));
        }
      }
    };
    SCMNodeManager scmNodeManager = new SCMNodeManager(conf, storageConfig, publisher, new NetworkTopologyImpl(conf),
        SCMContext.emptyContext(), mockVersionManager());
    nodeManager.set(scmNodeManager);
    dispatcher.set(new SCMDatanodeHeartbeatDispatcher(scmNodeManager, publisher));
  }

  /** Stands in for the GETVERSION and REGISTER endpoint tasks. */
  private void registerWithScm() {
    nodeManager.get().register(DatanodeDetails.getFromProtoBuf(datanode.getProtoBufMessage()),
        HddsTestUtils.getRandomNodeReport(), null);
    endpoint.setState(EndPointStates.HEARTBEAT);
  }

  private void raiseCloseAction(long containerID) {
    context.addContainerActionIfAbsent(ContainerAction.newBuilder().setContainerID(containerID)
        .setAction(ContainerAction.Action.CLOSE).setReason(ContainerAction.Reason.CONTAINER_FULL).build());
  }

  /** Runs one heartbeat task and returns the request the datanode sent. */
  private SCMHeartbeatRequestProto sendHeartbeat() throws Exception {
    lastRequest.set(null);
    task.call();
    return lastRequest.get();
  }
}
