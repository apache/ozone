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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.lease.Lease;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

/**
 * Tests the closeContainerEventHandler class.
 */
public class TestCloseContainerEventHandler {

  private static final ReplicationConfig RATIS_REP_CONFIG
      = RatisReplicationConfig.getInstance(THREE);
  private static final ReplicationConfig EC_REP_CONFIG
      = new ECReplicationConfig(3, 2);

  private ContainerManager containerManager;
  private PipelineManager pipelineManager;
  private EventPublisher eventPublisher;
  private CloseContainerEventHandler eventHandler;

  @Captor
  private ArgumentCaptor<CommandForDatanode> commandCaptor;

  @BeforeEach
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);
    containerManager = mock(ContainerManager.class);
    pipelineManager = mock(PipelineManager.class);
    SCMContext scmContext = mock(SCMContext.class);
    when(scmContext.isLeader()).thenReturn(true);
    eventPublisher = mock(EventPublisher.class);
    LeaseManager leaseManager = mock(LeaseManager.class);
    when(leaseManager.acquire(any(), anyLong(), any())).thenAnswer(
        invocation -> invocation.getArgument(2, Callable.class).call());
    eventHandler = new CloseContainerEventHandler(
        pipelineManager, containerManager, scmContext, leaseManager, 0);
  }

  @Test
  public void testCloseContainerEventWithInvalidContainer()
      throws ContainerNotFoundException, PipelineNotFoundException {
    when(containerManager.getContainer(any())).thenThrow(ContainerNotFoundException.class);
    when(pipelineManager.getPipeline(any())).thenReturn(createPipeline(RATIS_REP_CONFIG, 3));

    eventHandler.onMessage(ContainerID.valueOf(1234), eventPublisher);
    verify(eventPublisher, never()).fireEvent(any(), any());
  }

  @Test
  public void testCloseContainerInInvalidState()
      throws ContainerNotFoundException {
    final Pipeline pipeline = createPipeline(RATIS_REP_CONFIG, 3);
    final ContainerInfo container =
        createContainer(RATIS_REP_CONFIG, pipeline.getId());
    container.setState(HddsProtos.LifeCycleState.CLOSED);
    when(containerManager.getContainer(container.containerID())).thenReturn(container);

    eventHandler.onMessage(container.containerID(), eventPublisher);
    verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());
  }

  @Test
  public void testCloseContainerWithDelayByLeaseManager()
      throws Exception {
    final Pipeline pipeline = createPipeline(RATIS_REP_CONFIG, 3);
    final ContainerInfo container =
        createContainer(RATIS_REP_CONFIG, pipeline.getId());
    container.setState(HddsProtos.LifeCycleState.CLOSING);
    when(containerManager.getContainer(container.containerID())).thenReturn(container);

    SCMContext scmContext = mock(SCMContext.class);
    when(scmContext.isLeader()).thenReturn(true);
    long timeoutInMs = 2000;
    when(pipelineManager.getPipeline(pipeline.getId())).thenReturn(pipeline);
    LeaseManager<Object> leaseManager = new LeaseManager<>("test", timeoutInMs);
    leaseManager.start();
    LeaseManager mockLeaseManager = mock(LeaseManager.class);
    List<Lease<Object>> leaseList = new ArrayList<>(1);
    when(mockLeaseManager.acquire(any(), anyLong(), any())).thenAnswer(
        invocation -> {
          leaseList.add(leaseManager.acquire(
              invocation.getArgument(0, Object.class),
              invocation.getArgument(1),
              invocation.getArgument(2, Callable.class)));
          return leaseList.get(0);
        });
    CloseContainerEventHandler closeHandler = new CloseContainerEventHandler(
        pipelineManager, containerManager, scmContext,
        mockLeaseManager, timeoutInMs);
    closeHandler.onMessage(container.containerID(), eventPublisher);
    verify(mockLeaseManager, atLeastOnce()).acquire(any(), anyLong(), any());
    assertThat(leaseList.size()).isGreaterThan(0);
    // immediate check if event is published
    verify(eventPublisher, never()).fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());
    // wait for event to happen
    GenericTestUtils.waitFor(() -> {
      try {
        verify(eventPublisher, atLeastOnce())
            .fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());
      } catch (Throwable ex) {
        return false;
      }
      return true;
    }, 1000, (int) timeoutInMs * 3);
    leaseManager.shutdown();
  }

  @Test
  public void testCloseContainerEventWithRatisContainers()
      throws IOException, InvalidStateTransitionException, TimeoutException {
    closeContainerForValidContainer(RATIS_REP_CONFIG, 3, false);
  }

  @Test
  public void testCloseContainerEventECContainer()
      throws InvalidStateTransitionException, IOException, TimeoutException {
    closeContainerForValidContainer(EC_REP_CONFIG, 5, true);
  }

  private void closeContainerForValidContainer(ReplicationConfig repConfig,
      int nodeCount, boolean forceClose)
      throws IOException, InvalidStateTransitionException, TimeoutException {
    final Pipeline pipeline = createPipeline(repConfig, nodeCount);
    final ContainerInfo container =
        createContainer(repConfig, pipeline.getId());
    when(containerManager.getContainer(container.containerID()))
        .thenReturn(container);
    doAnswer(
        i -> {
          container.setState(HddsProtos.LifeCycleState.CLOSING);
          return null;
        }).when(containerManager).updateContainerState(container.containerID(),
        HddsProtos.LifeCycleEvent.FINALIZE);
    when(pipelineManager.getPipeline(pipeline.getId())).thenReturn(pipeline);

    eventHandler.onMessage(container.containerID(), eventPublisher);

    verify(containerManager).updateContainerState(any(), any());
    verify(eventPublisher, times(nodeCount))
        .fireEvent(eq(DATANODE_COMMAND), commandCaptor.capture());

    List<CommandForDatanode> cmds = commandCaptor.getAllValues();
    final Set<DatanodeID> pipelineDNs = pipeline
        .getNodes()
        .stream()
        .map(DatanodeDetails::getID)
        .collect(Collectors.toSet());
    for (CommandForDatanode c : cmds) {
      assertThat(pipelineDNs).contains(c.getDatanodeId());
      pipelineDNs.remove(c.getDatanodeId());
      CloseContainerCommand ccc = (CloseContainerCommand)c.getCommand();
      assertEquals(container.getContainerID(), ccc.getContainerID());
      assertEquals(pipeline.getId(), ccc.getPipelineID());
      assertEquals(forceClose, ccc.getProto().getForce());
    }
    assertEquals(0, pipelineDNs.size());
  }

  private Pipeline createPipeline(ReplicationConfig repConfig, int nodes) {
    Pipeline.Builder builder = Pipeline.newBuilder();
    builder.setId(PipelineID.randomId());
    builder.setReplicationConfig(repConfig);
    builder.setState(Pipeline.PipelineState.OPEN);
    List<DatanodeDetails> dns = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      dns.add(MockDatanodeDetails.randomDatanodeDetails());
    }
    builder.setNodes(dns);
    builder.setLeaderId(dns.get(0).getID());
    return builder.build();
  }

  private ContainerInfo createContainer(ReplicationConfig repConfig,
      PipelineID pipelineID) {
    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    builder.setContainerID(1);
    builder.setOwner("Ozone");
    builder.setPipelineID(pipelineID);
    builder.setReplicationConfig(repConfig);
    builder.setState(HddsProtos.LifeCycleState.OPEN);
    return builder.build();
  }
}
