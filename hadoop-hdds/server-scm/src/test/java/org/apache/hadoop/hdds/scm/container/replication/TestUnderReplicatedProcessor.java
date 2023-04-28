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
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.UnderReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;

/**
 * Tests for the UnderReplicatedProcessor class.
 */
public class TestUnderReplicatedProcessor {

  private ConfigurationSource conf;
  private ReplicationManager replicationManager;
  private ECReplicationConfig repConfig;
  private UnderReplicatedProcessor underReplicatedProcessor;
  private ReplicationQueue queue;

  @Before
  public void setup() {
    conf = new OzoneConfiguration();
    ReplicationManagerConfiguration rmConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    replicationManager = Mockito.mock(ReplicationManager.class);

    // use real queue
    queue = new ReplicationQueue();
    repConfig = new ECReplicationConfig(3, 2);
    underReplicatedProcessor = new UnderReplicatedProcessor(
        replicationManager, rmConf.getUnderReplicatedInterval());
    Mockito.when(replicationManager.shouldRun()).thenReturn(true);
    Mockito.when(replicationManager.getMetrics())
        .thenReturn(ReplicationManagerMetrics.create(replicationManager));
  }

  @Test
  public void testSuccessfulCommand() throws IOException {
    ContainerInfo container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    queue.enqueue(new UnderReplicatedHealthResult(
        container, 3, true, false, false));
    Mockito.when(replicationManager
            .processUnderReplicatedContainer(any()))
        .thenReturn(1);
    underReplicatedProcessor.processAll(queue);

    assertEquals(0, queue.underReplicatedQueueSize());
  }

  @Test
  public void testMessageRequeuedOnException() throws IOException {
    ContainerInfo container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    UnderReplicatedHealthResult result = new UnderReplicatedHealthResult(
        container, 3, false, false, false);
    queue.enqueue(result);

    Mockito.when(replicationManager
            .processUnderReplicatedContainer(any()))
        .thenThrow(new IOException("Test Exception"))
        .thenThrow(new AssertionError("Should process only one item"));
    underReplicatedProcessor.processAll(queue);

    assertEquals(1, queue.underReplicatedQueueSize());
    assertSame(result, queue.dequeueUnderReplicatedContainer());
  }
}
