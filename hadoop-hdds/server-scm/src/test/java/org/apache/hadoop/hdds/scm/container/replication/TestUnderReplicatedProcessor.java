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

package org.apache.hadoop.hdds.scm.container.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.UnderReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the UnderReplicatedProcessor class.
 */
public class TestUnderReplicatedProcessor {

  private ReplicationManager replicationManager;
  private ECReplicationConfig repConfig;
  private UnderReplicatedProcessor underReplicatedProcessor;
  private ReplicationQueue queue;
  private ReplicationManagerMetrics rmMetrics;

  @BeforeEach
  public void setup() {
    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationManagerConfiguration rmConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    replicationManager = mock(ReplicationManager.class);

    // use real queue
    queue = new ReplicationQueue();
    repConfig = new ECReplicationConfig(3, 2);
    when(replicationManager.shouldRun()).thenReturn(true);
    when(replicationManager.getConfig()).thenReturn(rmConf);
    rmMetrics = ReplicationManagerMetrics.create(replicationManager);
    when(replicationManager.getMetrics()).thenReturn(rmMetrics);
    when(replicationManager.getReplicationInFlightLimit()).thenReturn(0L);
    underReplicatedProcessor = new UnderReplicatedProcessor(
        replicationManager, rmConf::getUnderReplicatedInterval);
  }

  @Test
  public void testSuccessfulCommand() throws IOException {
    ContainerInfo container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    queue.enqueue(new UnderReplicatedHealthResult(
        container, 3, true, false, false));
    when(replicationManager
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

    when(replicationManager
            .processUnderReplicatedContainer(any()))
        .thenThrow(new IOException("Test Exception"))
        .thenThrow(new AssertionError("Should process only one item"));
    underReplicatedProcessor.processAll(queue);

    assertEquals(1, queue.underReplicatedQueueSize());
    assertSame(result, queue.dequeueUnderReplicatedContainer());
  }

  @Test
  public void testMessageNotProcessedIfGlobalLimitReached() throws IOException {
    AtomicLong inFlightReplications = new AtomicLong(11);
    when(replicationManager.getReplicationInFlightLimit()).thenReturn(10L);
    doAnswer(invocation -> inFlightReplications.get())
        .when(replicationManager).getInflightReplicationCount();
    when(replicationManager.processUnderReplicatedContainer(any())).thenReturn(1);

    ContainerInfo container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    UnderReplicatedHealthResult result = new UnderReplicatedHealthResult(
        container, 3, false, false, false);
    queue.enqueue(result);

    underReplicatedProcessor.processAll(queue);

    // The message should not be processed and still be on the queue (re-queued)
    assertEquals(1, queue.underReplicatedQueueSize());
    // We should not have processed anything in RM
    verify(replicationManager, times(0)).processUnderReplicatedContainer(any());

    // Change the inflight replications to a value below the limit
    inFlightReplications.set(8);
    underReplicatedProcessor.processAll(queue);

    assertEquals(0, queue.underReplicatedQueueSize());
    // We should have processed the message now
    verify(replicationManager, times(1)).processUnderReplicatedContainer(any());
    assertEquals(1, rmMetrics.getPendingReplicationLimitReachedTotal());
  }

}
