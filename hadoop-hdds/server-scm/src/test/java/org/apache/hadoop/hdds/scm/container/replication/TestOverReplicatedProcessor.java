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
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;

/**
 * Tests for the OverReplicatedProcessor class.
 */
public class TestOverReplicatedProcessor {

  private ConfigurationSource conf;
  private ReplicationManager replicationManager;
  private ECReplicationConfig repConfig;
  private OverReplicatedProcessor overReplicatedProcessor;

  @Before
  public void setup() {
    conf = new OzoneConfiguration();
    ReplicationManagerConfiguration rmConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    replicationManager = Mockito.mock(ReplicationManager.class);
    repConfig = new ECReplicationConfig(3, 2);
    overReplicatedProcessor = new OverReplicatedProcessor(
        replicationManager, rmConf.getOverReplicatedInterval());
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
        .when(replicationManager.processOverReplicatedContainer(any()))
        .thenReturn(commands);
    overReplicatedProcessor.processAll();

    Mockito.verify(replicationManager).sendDatanodeCommand(any(), any(), any());
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
            .processOverReplicatedContainer(any()))
        .thenThrow(new IOException("Test Exception"));
    overReplicatedProcessor.processAll();

    Mockito.verify(replicationManager, Mockito.times(0))
        .sendDatanodeCommand(any(), any(), any());
    Mockito.verify(replicationManager, Mockito.times(1))
        .requeueOverReplicatedContainer(any());

  }
}
