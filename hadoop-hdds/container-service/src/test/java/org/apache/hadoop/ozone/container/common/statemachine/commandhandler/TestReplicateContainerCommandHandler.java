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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.container.common.helpers.CommandHandlerMetrics;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.ContainerReplicator;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.container.replication.ReplicationTask;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test cases to verify {@link ReplicateContainerCommandHandler}.
 */
public class TestReplicateContainerCommandHandler {
  private OzoneConfiguration conf;
  private ReplicationSupervisor supervisor;
  private ContainerReplicator downloadReplicator;
  private ContainerReplicator pushReplicator;
  private OzoneContainer ozoneContainer;
  private StateContext stateContext;
  private SCMConnectionManager connectionManager;

  @BeforeEach
  public void setUp() {
    conf = new OzoneConfiguration();
    supervisor = mock(ReplicationSupervisor.class);
    downloadReplicator = mock(ContainerReplicator.class);
    pushReplicator = mock(ContainerReplicator.class);
    ozoneContainer = mock(OzoneContainer.class);
    connectionManager = mock(SCMConnectionManager.class);
    stateContext = mock(StateContext.class);
  }

  @Test
  public void testMetrics() {
    ReplicateContainerCommandHandler commandHandler =
        new ReplicateContainerCommandHandler(conf, supervisor,
            downloadReplicator, pushReplicator);
    Map<SCMCommandProto.Type, CommandHandler> handlerMap = new HashMap<>();
    handlerMap.put(commandHandler.getCommandType(), commandHandler);
    CommandHandlerMetrics metrics = CommandHandlerMetrics.create(handlerMap);
    try {
      doNothing().when(supervisor).addTask(any());
      DatanodeDetails source = MockDatanodeDetails.randomDatanodeDetails();
      DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
      List<DatanodeDetails> sourceList = new ArrayList<>();
      sourceList.add(source);

      ReplicateContainerCommand command = ReplicateContainerCommand.fromSources(
          1, sourceList);
      commandHandler.handle(command, ozoneContainer, stateContext, connectionManager);
      String metricsName = ReplicationTask.METRIC_NAME;
      assertEquals(commandHandler.getMetricsName(), metricsName);
      when(supervisor.getReplicationRequestCount(metricsName)).thenReturn(1L);
      assertEquals(commandHandler.getInvocationCount(), 1);

      commandHandler.handle(ReplicateContainerCommand.fromSources(2, sourceList),
          ozoneContainer, stateContext, connectionManager);
      commandHandler.handle(ReplicateContainerCommand.fromSources(3, sourceList),
          ozoneContainer, stateContext, connectionManager);
      commandHandler.handle(ReplicateContainerCommand.toTarget(4, target),
          ozoneContainer, stateContext, connectionManager);
      commandHandler.handle(ReplicateContainerCommand.toTarget(5, target),
          ozoneContainer, stateContext, connectionManager);
      commandHandler.handle(ReplicateContainerCommand.fromSources(6, sourceList),
          ozoneContainer, stateContext, connectionManager);

      when(supervisor.getReplicationRequestCount(metricsName)).thenReturn(5L);
      when(supervisor.getReplicationRequestTotalTime(metricsName)).thenReturn(10L);
      when(supervisor.getReplicationRequestAvgTime(metricsName)).thenReturn(3L);
      when(supervisor.getReplicationQueuedCount(metricsName)).thenReturn(1L);
      assertEquals(commandHandler.getInvocationCount(), 5);
      assertEquals(commandHandler.getQueuedCount(), 1);
      assertEquals(commandHandler.getTotalRunTime(), 10);
      assertEquals(commandHandler.getAverageRunTime(), 3);

      MetricsCollectorImpl metricsCollector = new MetricsCollectorImpl();
      metrics.getMetrics(metricsCollector, true);
      assertEquals(1, metricsCollector.getRecords().size());
    } finally {
      metrics.unRegister();
    }
  }
}
