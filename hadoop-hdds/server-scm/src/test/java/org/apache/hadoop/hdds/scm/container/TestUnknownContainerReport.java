/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.scm.TestUtils.getContainer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.server
    .SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test container deletion behaviour of unknown containers
 * that reported by Datanodes.
 */
public class TestUnknownContainerReport {

  private NodeManager nodeManager;
  private ContainerManager containerManager;
  private ContainerStateManager containerStateManager;
  private EventPublisher publisher;

  @Before
  public void setup() throws IOException {
    final ConfigurationSource conf = new OzoneConfiguration();
    this.nodeManager = new MockNodeManager(true, 10);
    this.containerManager = Mockito.mock(ContainerManager.class);
    this.containerStateManager = new ContainerStateManager(conf);
    this.publisher = Mockito.mock(EventPublisher.class);

    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenThrow(new ContainerNotFoundException());
  }

  @After
  public void tearDown() throws IOException {
    containerStateManager.close();
  }

  @Test
  public void testUnknownContainerNotDeleted() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    sendContainerReport(conf);

    // By default, unknown containers won't be taken delete action by SCM
    verify(publisher, times(0)).fireEvent(
        Mockito.eq(SCMEvents.DATANODE_COMMAND),
        Mockito.any(CommandForDatanode.class));
  }

  @Test
  public void testUnknownContainerDeleted() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(
        ScmConfig.HDDS_SCM_UNKNOWN_CONTAINER_ACTION,
        ContainerReportHandler.UNKNOWN_CONTAINER_ACTION_DELETE);

    sendContainerReport(conf);
    verify(publisher, times(1)).fireEvent(
        Mockito.eq(SCMEvents.DATANODE_COMMAND),
        Mockito.any(CommandForDatanode.class));
  }

  /**
   * Trigger datanode to send unknown container report to SCM.
   * @param conf OzoneConfiguration instance to initialize
   *             ContainerReportHandler
   */
  private void sendContainerReport(OzoneConfiguration conf) {
    ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager, conf);

    ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    Iterator<DatanodeDetails> nodeIterator = nodeManager
        .getNodes(NodeStatus.inServiceHealthy()).iterator();
    DatanodeDetails datanode = nodeIterator.next();

    ContainerReportsProto containerReport = getContainerReportsProto(
        container.containerID(), ContainerReplicaProto.State.CLOSED,
        datanode.getUuidString());
    ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanode, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);
  }

  private static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId) {
    final ContainerReportsProto.Builder crBuilder =
        ContainerReportsProto.newBuilder();
    final ContainerReplicaProto replicaProto =
        ContainerReplicaProto.newBuilder()
            .setContainerID(containerId.getId())
            .setState(state)
            .setOriginNodeId(originNodeId)
            .setFinalhash("e16cc9d6024365750ed8dbd194ea46d2")
            .setSize(5368709120L)
            .setUsed(2000000000L)
            .setKeyCount(100000000L)
            .setReadCount(100000000L)
            .setWriteCount(100000000L)
            .setReadBytes(2000000000L)
            .setWriteBytes(2000000000L)
            .setBlockCommitSequenceId(10000L)
            .setDeleteTransactionId(0)
            .build();
    return crBuilder.addReports(replicaProto).build();
  }

}