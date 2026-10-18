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

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.container.common.helpers.CommandHandlerMetrics;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinator;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinatorTask;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test cases to verify {@link ReconstructECContainersCommandHandler}.
 */
public class TestReconstructECContainersCommandHandler {
  private OzoneConfiguration conf;
  private ReplicationSupervisor supervisor;
  private ECReconstructionCoordinator coordinator;
  private OzoneContainer ozoneContainer;
  private StateContext stateContext;
  private SCMConnectionManager connectionManager;

  @BeforeEach
  public void setUp() {
    supervisor = mock(ReplicationSupervisor.class);
    coordinator = mock(ECReconstructionCoordinator.class);
    conf = new OzoneConfiguration();
    ozoneContainer = mock(OzoneContainer.class);
    connectionManager = mock(SCMConnectionManager.class);
    stateContext = mock(StateContext.class);
  }

  @Test
  public void testMetrics() {
    ReconstructECContainersCommandHandler commandHandler =
        new ReconstructECContainersCommandHandler(conf, supervisor, coordinator);
    doNothing().when(supervisor).addTask(any());
    Map<SCMCommandProto.Type, CommandHandler> handlerMap = new HashMap<>();
    handlerMap.put(commandHandler.getCommandType(), commandHandler);
    CommandHandlerMetrics metrics = CommandHandlerMetrics.create(handlerMap);
    try {
      byte[] missingIndexes = {1, 2};
      ByteString missingContainerIndexes = UnsafeByteOperations.unsafeWrap(missingIndexes);
      ECReplicationConfig ecReplicationConfig = new ECReplicationConfig(3, 2);
      List<DatanodeDetails> dnDetails = getDNDetails(5);
      List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex> sources =
          dnDetails.stream().map(a -> new ReconstructECContainersCommand
              .DatanodeDetailsAndReplicaIndex(a, dnDetails.indexOf(a)))
                  .collect(Collectors.toList());
      List<DatanodeDetails> targets = getDNDetails(2);
      ReconstructECContainersCommand reconstructECContainersCommand =
          new ReconstructECContainersCommand(1L, sources, targets,
              missingContainerIndexes, ecReplicationConfig);

      commandHandler.handle(reconstructECContainersCommand, ozoneContainer,
          stateContext, connectionManager);
      String metricsName = ECReconstructionCoordinatorTask.METRIC_NAME;
      assertEquals(commandHandler.getMetricsName(), metricsName);
      when(supervisor.getReplicationRequestCount(metricsName)).thenReturn(1L);
      assertEquals(commandHandler.getInvocationCount(), 1);

      commandHandler.handle(new ReconstructECContainersCommand(2L, sources,
          targets, missingContainerIndexes, ecReplicationConfig), ozoneContainer,
              stateContext, connectionManager);
      commandHandler.handle(new ReconstructECContainersCommand(3L, sources,
          targets, missingContainerIndexes, ecReplicationConfig), ozoneContainer,
              stateContext, connectionManager);
      commandHandler.handle(new ReconstructECContainersCommand(4L, sources,
          targets, missingContainerIndexes, ecReplicationConfig), ozoneContainer,
              stateContext, connectionManager);
      commandHandler.handle(new ReconstructECContainersCommand(5L, sources,
          targets, missingContainerIndexes, ecReplicationConfig), ozoneContainer,
              stateContext, connectionManager);
      commandHandler.handle(new ReconstructECContainersCommand(6L, sources,
          targets, missingContainerIndexes, ecReplicationConfig), ozoneContainer,
              stateContext, connectionManager);

      when(supervisor.getReplicationRequestCount(metricsName)).thenReturn(5L);
      when(supervisor.getReplicationRequestTotalTime(metricsName)).thenReturn(10L);
      when(supervisor.getReplicationRequestAvgTime(metricsName)).thenReturn(2L);
      when(supervisor.getReplicationQueuedCount(metricsName)).thenReturn(1L);
      assertEquals(commandHandler.getInvocationCount(), 5);
      assertEquals(commandHandler.getQueuedCount(), 1);
      assertEquals(commandHandler.getTotalRunTime(), 10);
      assertEquals(commandHandler.getAverageRunTime(), 2);

      MetricsCollectorImpl metricsCollector = new MetricsCollectorImpl();
      metrics.getMetrics(metricsCollector, true);
      assertEquals(1, metricsCollector.getRecords().size());
    } finally {
      metrics.unRegister();
    }
  }

  private List<DatanodeDetails> getDNDetails(int numDns) {
    List<DatanodeDetails> dns = new ArrayList<>();
    for (int i = 0; i < numDns; i++) {
      dns.add(MockDatanodeDetails.randomDatanodeDetails());
    }
    return dns;
  }
}
