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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.OverReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;

/**
 * Tests for the OverReplicatedProcessor class.
 */
public class TestOverReplicatedProcessor {

  private ReplicationManager replicationManager;
  private ECReplicationConfig repConfig;
  private OverReplicatedProcessor overReplicatedProcessor;
  private ReplicationQueue queue;

  @Before
  public void setup() {
    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationManagerConfiguration rmConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    replicationManager = Mockito.mock(ReplicationManager.class);

    // use real queue
    queue = new ReplicationQueue();
    repConfig = new ECReplicationConfig(3, 2);
    overReplicatedProcessor = new OverReplicatedProcessor(
        replicationManager, rmConf.getOverReplicatedInterval());
    Mockito.when(replicationManager.shouldRun()).thenReturn(true);

    // Even through the limit has been exceeded, it should not stop over-rep
    // processing, as the over-rep handler ignores the limit as it only does
    // deletes.
    Mockito.when(replicationManager.getReplicationInFlightLimit())
        .thenReturn(1L);
    Mockito.when(replicationManager.getInflightReplicationCount())
        .thenReturn(2L);
  }

  @Test
  public void testSuccessfulRun() throws IOException {
    ContainerInfo container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    queue.enqueue(new OverReplicatedHealthResult(
        container, 3, false));

    Mockito.when(replicationManager.processOverReplicatedContainer(any()))
        .thenReturn(1);
    overReplicatedProcessor.processAll(queue);

    assertEquals(0, queue.overReplicatedQueueSize());
  }

  @Test
  public void testMessageRequeuedOnException() throws IOException {
    ContainerInfo container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    OverReplicatedHealthResult result = new OverReplicatedHealthResult(
        container, 3, false);
    queue.enqueue(result);

    Mockito.when(replicationManager
            .processOverReplicatedContainer(any()))
        .thenThrow(new IOException("Test Exception"))
        .thenThrow(new AssertionError("Should process only one item"));

    overReplicatedProcessor.processAll(queue);

    assertEquals(1, queue.overReplicatedQueueSize());
    assertSame(result, queue.dequeueOverReplicatedContainer());
  }
}
