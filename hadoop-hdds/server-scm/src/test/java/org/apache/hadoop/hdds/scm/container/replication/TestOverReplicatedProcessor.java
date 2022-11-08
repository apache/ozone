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
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ozone.test.TestClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;

/**
 * Tests for the OverReplicatedProcessor class.
 */
public class TestOverReplicatedProcessor {

  private ConfigurationSource conf;
  private TestClock clock;
  private ContainerReplicaPendingOps pendingOps;
  private ReplicationManager replicationManager;
  private EventPublisher eventPublisher;
  private ECReplicationConfig repConfig;
  private OverReplicatedProcessor overReplicatedProcessor;

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
    overReplicatedProcessor = new OverReplicatedProcessor(
        replicationManager, pendingOps, eventPublisher,
        rmConf.getOverReplicatedInterval());
    Mockito.when(replicationManager.shouldRun()).thenReturn(true);
  }

  @Test
  public void testDeleteContainerCommand() throws IOException {
    ContainerInfo container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    Mockito.when(replicationManager.dequeueOverReplicatedContainer())
        .thenReturn(
            new ContainerHealthResult.OverReplicatedHealthResult(container, 3,
                false), null);
    Map<DatanodeDetails, SCMCommand<?>> commands = new HashMap<>();
    DeleteContainerCommand cmd =
        new DeleteContainerCommand(container.getContainerID());
    cmd.setReplicaIndex(5);
    commands.put(MockDatanodeDetails.randomDatanodeDetails(), cmd);

    Mockito
        .when(replicationManager.processOverReplicatedContainer(Mockito.any()))
        .thenReturn(commands);
    overReplicatedProcessor.processAll();

    // Ensure pending ops is updated for the target DNs in the command and the
    // correct indexes.
    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(container.containerID());
    //Check InFlight Deletion
    Assert.assertEquals(pendingOps
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE), 1);
    Assert.assertEquals(1, ops.size());
    for (ContainerReplicaOp op : ops) {
      Assert.assertEquals(5, op.getReplicaIndex());
    }
  }

  @Test
  public void testMessageRequeuedOnException() throws IOException {
    ContainerInfo container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    Mockito.when(replicationManager.dequeueOverReplicatedContainer())
        .thenReturn(new ContainerHealthResult
                .OverReplicatedHealthResult(container, 3, false),
            null);

    Mockito.when(replicationManager
            .processOverReplicatedContainer(Mockito.any()))
        .thenThrow(new IOException("Test Exception"));
    overReplicatedProcessor.processAll();

    Mockito.verify(eventPublisher, Mockito.times(0))
        .fireEvent(eq(SCMEvents.DATANODE_COMMAND), Mockito.any());
    Mockito.verify(replicationManager, Mockito.times(1))
        .requeueOverReplicatedContainer(Mockito.any());

    // Ensure pending ops has nothing for this container.
    List<ContainerReplicaOp> ops = pendingOps
        .getPendingOps(container.containerID());
    Assert.assertEquals(0, ops.size());
  }
}
