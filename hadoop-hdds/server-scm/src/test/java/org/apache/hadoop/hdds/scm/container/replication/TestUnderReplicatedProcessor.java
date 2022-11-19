/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ozone.test.TestClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;

/**
 * Tests for the UnderReplicatedProcessor class.
 */
public class TestUnderReplicatedProcessor {

  private ConfigurationSource conf;
  private TestClock clock;
  private ContainerReplicaPendingOps pendingOps;
  private ReplicationManager replicationManager;
  private EventPublisher eventPublisher;
  private ECReplicationConfig repConfig;
  private UnderReplicatedProcessor underReplicatedProcessor;

  @Before
  public void setup() {
    conf = new OzoneConfiguration();
    ReplicationManagerConfiguration rmConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    pendingOps = new ContainerReplicaPendingOps(conf, clock);
    replicationManager = Mockito.mock(ReplicationManager.class);
    eventPublisher = Mockito.mock(EventPublisher.class);
    repConfig = new ECReplicationConfig(3, 2);
    underReplicatedProcessor = new UnderReplicatedProcessor(
        replicationManager, pendingOps, eventPublisher,
        rmConf.getUnderReplicatedInterval());
    Mockito.when(replicationManager.shouldRun()).thenReturn(true);
    Mockito.when(replicationManager.getMetrics())
        .thenReturn(ReplicationManagerMetrics.create(replicationManager));
  }

  @Test
  public void testEcReconstructionCommand() throws IOException {
    ContainerInfo container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    Mockito.when(replicationManager.dequeueUnderReplicatedContainer())
        .thenReturn(new ContainerHealthResult
                .UnderReplicatedHealthResult(container, 3, false, false, false),
            (ContainerHealthResult.UnderReplicatedHealthResult) null);
    List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex>
        sourceNodes = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      sourceNodes.add(
          new ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex(
              MockDatanodeDetails.randomDatanodeDetails(), i));
    }
    List<DatanodeDetails> targetNodes = new ArrayList<>();
    targetNodes.add(MockDatanodeDetails.randomDatanodeDetails());
    targetNodes.add(MockDatanodeDetails.randomDatanodeDetails());
    byte[] missingIndexes = {4, 5};

    Map<DatanodeDetails, SCMCommand<?>> commands = new HashMap<>();
    commands.put(MockDatanodeDetails.randomDatanodeDetails(),
        new ReconstructECContainersCommand(container.getContainerID(),
            sourceNodes, targetNodes, missingIndexes, repConfig));

    Mockito.when(replicationManager
            .processUnderReplicatedContainer(Mockito.any()))
        .thenReturn(commands);
    underReplicatedProcessor.processAll();

    Mockito.verify(eventPublisher, Mockito.times(1))
        .fireEvent(eq(SCMEvents.DATANODE_COMMAND), Mockito.any());
    Mockito.verify(replicationManager, Mockito.times(0))
        .requeueUnderReplicatedContainer(Mockito.any());

    // Ensure pending ops is updated for the target DNs in the command and the
    // correct indexes.
    List<ContainerReplicaOp> ops = pendingOps
        .getPendingOps(container.containerID());
    Assert.assertEquals(2, ops.size());
    for (ContainerReplicaOp op : ops) {
      int ind = targetNodes.indexOf(op.getTarget());
      Assert.assertEquals(missingIndexes[ind], op.getReplicaIndex());
    }
  }

  @Test
  public void testEcReplicationCommand() throws IOException {
    ContainerInfo container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    Mockito.when(replicationManager.dequeueUnderReplicatedContainer())
        .thenReturn(new ContainerHealthResult
                .UnderReplicatedHealthResult(container, 3, true, false, false),
            (ContainerHealthResult.UnderReplicatedHealthResult) null);
    List<DatanodeDetails> sourceDns = new ArrayList<>();
    sourceDns.add(MockDatanodeDetails.randomDatanodeDetails());
    DatanodeDetails targetDn = MockDatanodeDetails.randomDatanodeDetails();
    ReplicateContainerCommand rcc = new ReplicateContainerCommand(
        container.getContainerID(), sourceDns);
    rcc.setReplicaIndex(3);

    Map<DatanodeDetails, SCMCommand<?>> commands = new HashMap<>();
    commands.put(targetDn, rcc);

    Mockito.when(replicationManager
            .processUnderReplicatedContainer(Mockito.any()))
        .thenReturn(commands);
    underReplicatedProcessor.processAll();

    Mockito.verify(eventPublisher, Mockito.times(1))
        .fireEvent(eq(SCMEvents.DATANODE_COMMAND), Mockito.any());
    Mockito.verify(replicationManager, Mockito.times(0))
        .requeueUnderReplicatedContainer(Mockito.any());

    // Ensure pending ops is updated for the target DNs in the command and the
    // correct indexes.
    List<ContainerReplicaOp> ops = pendingOps
        .getPendingOps(container.containerID());
    //Check InFlight Replication
    Assert.assertEquals(pendingOps
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD), 1);
    Assert.assertEquals(1, ops.size());
    Assert.assertEquals(3, ops.get(0).getReplicaIndex());
  }

  @Test
  public void testMessageRequeuedOnException() throws IOException {
    ContainerInfo container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    Mockito.when(replicationManager.dequeueUnderReplicatedContainer())
        .thenReturn(new ContainerHealthResult
                .UnderReplicatedHealthResult(container, 3, false, false, false),
            (ContainerHealthResult.UnderReplicatedHealthResult) null);

    Mockito.when(replicationManager
            .processUnderReplicatedContainer(Mockito.any()))
        .thenThrow(new IOException("Test Exception"));
    underReplicatedProcessor.processAll();

    Mockito.verify(eventPublisher, Mockito.times(0))
        .fireEvent(eq(SCMEvents.DATANODE_COMMAND), Mockito.any());
    Mockito.verify(replicationManager, Mockito.times(1))
        .requeueUnderReplicatedContainer(Mockito.any());

    // Ensure pending ops has nothing for this container.
    List<ContainerReplicaOp> ops = pendingOps
        .getPendingOps(container.containerID());
    Assert.assertEquals(0, ops.size());
  }
}
