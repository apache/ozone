/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineActionsFromDatanode;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Tests for Pipeline Closing.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(300)
public class TestPipelineClose {

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private StorageContainerManager scm;
  private ContainerWithPipeline ratisContainer;
  private ContainerManager containerManager;
  private PipelineManager pipelineManager;

  private long pipelineDestroyTimeoutInMillis;
  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @BeforeAll
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, "5s");
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, 1000,
        TimeUnit.MILLISECONDS);
    pipelineDestroyTimeoutInMillis = 1000;
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT,
        pipelineDestroyTimeoutInMillis, TimeUnit.MILLISECONDS);
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    containerManager = scm.getContainerManager();
    pipelineManager = scm.getPipelineManager();
  }

  @BeforeEach
  void createContainer() throws IOException {
    ContainerInfo containerInfo = containerManager
        .allocateContainer(RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), "testOwner");
    ratisContainer = new ContainerWithPipeline(containerInfo,
        pipelineManager.getPipeline(containerInfo.getPipelineID()));
    // At this stage, there should be 2 pipeline one with 1 open container each.
    // Try closing the both the pipelines, one with a closed container and
    // the other with an open container.
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterAll
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testPipelineCloseWithClosedContainer() throws IOException,
      InvalidStateTransitionException, TimeoutException {
    Set<ContainerID> set = pipelineManager
        .getContainersInPipeline(ratisContainer.getPipeline().getId());

    ContainerID cId = ratisContainer.getContainerInfo().containerID();
    assertEquals(1, set.size());
    set.forEach(containerID -> assertEquals(containerID, cId));

    // Now close the container and it should not show up while fetching
    // containers by pipeline
    containerManager
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.CLOSE);

    Set<ContainerID> setClosed = pipelineManager
        .getContainersInPipeline(ratisContainer.getPipeline().getId());
    assertEquals(0, setClosed.size());

    pipelineManager.closePipeline(ratisContainer.getPipeline().getId());
    pipelineManager.deletePipeline(ratisContainer.getPipeline().getId());
    for (DatanodeDetails dn : ratisContainer.getPipeline().getNodes()) {
      // Assert that the pipeline has been removed from Node2PipelineMap as well
      assertThat(scm.getScmNodeManager().getPipelines(dn))
          .doesNotContain(ratisContainer.getPipeline().getId());
    }
  }

  @Test
  public void testPipelineCloseWithOpenContainer()
      throws IOException, TimeoutException, InterruptedException {
    Set<ContainerID> setOpen = pipelineManager.getContainersInPipeline(
        ratisContainer.getPipeline().getId());
    assertEquals(1, setOpen.size());

    pipelineManager
        .closePipeline(ratisContainer.getPipeline(), false);
    GenericTestUtils.waitFor(() -> {
      try {
        return containerManager
            .getContainer(ratisContainer.getContainerInfo().containerID())
            .getState() == HddsProtos.LifeCycleState.CLOSING;
      } catch (ContainerNotFoundException e) {
        return false;
      }
    }, 100, 10000);
  }

  @Test
  public void testPipelineCloseWithPipelineAction() throws Exception {
    final List<DatanodeDetails> dns = ratisContainer.getPipeline().getNodes();
    final PipelineID pipelineID = ratisContainer.getPipeline().getId();

    final PipelineActionsFromDatanode pipelineActionsFromDatanode =
        HddsTestUtils.getPipelineActionFromDatanode(dns.get(0), pipelineID);

    // send closing action for pipeline
    final PipelineActionHandler pipelineActionHandler =
        new PipelineActionHandler(pipelineManager,
            SCMContext.emptyContext(), conf);

    pipelineActionHandler.onMessage(
        pipelineActionsFromDatanode, new EventQueue());

    final OzoneContainer ozoneContainer = cluster.getHddsDatanodes()
        .get(0).getDatanodeStateMachine().getContainer();
    final HddsProtos.PipelineID pid = pipelineID.getProtobuf();

    // ensure the pipeline is not reported by the dn
    GenericTestUtils.waitFor(() -> {
      final List<PipelineReport> pipelineReports = ozoneContainer
          .getPipelineReport().getPipelineReportList();
      for (PipelineReport pipelineReport : pipelineReports) {
        if (pipelineReport.getPipelineID().equals(pid)) {
          return false;
        }
      }
      return true;
    }, 500, 10000);

    assertThrows(PipelineNotFoundException.class, () ->
            pipelineManager.getPipeline(pipelineID),
        "Pipeline should not exist in SCM");
  }

  @Test
  void testPipelineCloseWithLogFailure()
      throws IOException, TimeoutException {
    EventQueue eventQ = (EventQueue) scm.getEventQueue();
    PipelineActionHandler pipelineActionTest =
        mock(PipelineActionHandler.class);
    eventQ.addHandler(SCMEvents.PIPELINE_ACTIONS, pipelineActionTest);
    ArgumentCaptor<PipelineActionsFromDatanode> actionCaptor =
        ArgumentCaptor.forClass(PipelineActionsFromDatanode.class);

    ContainerInfo containerInfo = containerManager
        .allocateContainer(RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), "testOwner");
    ContainerWithPipeline containerWithPipeline =
        new ContainerWithPipeline(containerInfo,
            pipelineManager.getPipeline(containerInfo.getPipelineID()));
    Pipeline openPipeline = containerWithPipeline.getPipeline();
    RaftGroupId groupId = RaftGroupId.valueOf(openPipeline.getId().getId());

    pipelineManager.getPipeline(openPipeline.getId());

    DatanodeDetails datanodeDetails = openPipeline.getNodes().get(0);
    int index = cluster.getHddsDatanodeIndex(datanodeDetails);

    XceiverServerRatis xceiverRatis =
        (XceiverServerRatis) cluster.getHddsDatanodes().get(index)
        .getDatanodeStateMachine().getContainer().getWriteChannel();

    /**
     * Notify Datanode Ratis Server endpoint of a Ratis log failure.
     * This is expected to trigger an immediate pipeline actions report to SCM
     */
    xceiverRatis.handleNodeLogFailure(groupId, null);
    verify(pipelineActionTest, timeout(1500).atLeastOnce())
        .onMessage(
            actionCaptor.capture(),
            any(EventPublisher.class));

    PipelineActionsFromDatanode actionsFromDatanode =
        actionCaptor.getValue();

    // match the pipeline id
    verifyCloseForPipeline(openPipeline, actionsFromDatanode);
  }

  private boolean verifyCloseForPipeline(Pipeline pipeline,
      PipelineActionsFromDatanode report) {
    UUID uuidToFind = pipeline.getId().getId();

    boolean found = false;
    for (StorageContainerDatanodeProtocolProtos.PipelineAction action :
        report.getReport().getPipelineActionsList()) {
      if (action.getAction() ==
          StorageContainerDatanodeProtocolProtos.PipelineAction.Action.CLOSE) {
        PipelineID closedPipelineId = PipelineID.
              getFromProtobuf(action.getClosePipeline().getPipelineID());

        if (closedPipelineId.getId().equals(uuidToFind)) {
          found = true;
        }
      }
    }

    assertTrue(found, "SCM did not receive a Close action for the Pipeline");
    return true;
  }
}
