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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CMD_STATUS_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CONTAINER_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.NODE_REPORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.CommandStatusReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.NodeReportFromDatanode;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.ReregisterCommand;
import org.junit.jupiter.api.Test;

/**
 * This class tests the behavior of SCMDatanodeHeartbeatDispatcher.
 */
public class TestSCMDatanodeHeartbeatDispatcher {

  @Test
  public void testNodeReportDispatcher() throws IOException {

    AtomicInteger eventReceived = new AtomicInteger();

    NodeReportProto nodeReport = NodeReportProto.getDefaultInstance();

    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.isNodeRegistered(any()))
        .thenReturn(true);

    SCMDatanodeHeartbeatDispatcher dispatcher =
        new SCMDatanodeHeartbeatDispatcher(mockNodeManager,
            new EventPublisher() {
              @Override
              public <PAYLOAD, EVENT extends Event<PAYLOAD>> void fireEvent(
                  EVENT event, PAYLOAD payload) {
                assertEquals(event, NODE_REPORT);
                eventReceived.incrementAndGet();
                assertEquals(nodeReport, ((NodeReportFromDatanode)payload).getReport());
              }
            });

    DatanodeDetails datanodeDetails = randomDatanodeDetails();

    SCMHeartbeatRequestProto heartbeat =
        SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanodeDetails.getProtoBufMessage())
        .setNodeReport(nodeReport)
        .build();
    dispatcher.dispatch(heartbeat);
    assertEquals(1, eventReceived.get());


  }

  @Test
  public void testContainerReportDispatcher() throws IOException {


    AtomicInteger eventReceived = new AtomicInteger();

    ContainerReportsProto containerReport =
        ContainerReportsProto.getDefaultInstance();
    CommandStatusReportsProto commandStatusReport =
        CommandStatusReportsProto.getDefaultInstance();

    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.isNodeRegistered(any()))
        .thenReturn(true);

    SCMDatanodeHeartbeatDispatcher dispatcher =
        new SCMDatanodeHeartbeatDispatcher(
            mockNodeManager,
            new EventPublisher() {
              @Override
              public <PAYLOAD, EVENT extends Event<PAYLOAD>> void fireEvent(
                  EVENT event, PAYLOAD payload) {
                assertTrue(
                    event.equals(CONTAINER_REPORT)
                        || event.equals(CMD_STATUS_REPORT));

                if (payload instanceof ContainerReportFromDatanode) {
                  assertEquals(containerReport,
                      ((ContainerReportFromDatanode) payload).getReport());
                }
                if (payload instanceof CommandStatusReportFromDatanode) {
                  assertEquals(commandStatusReport,
                      ((CommandStatusReportFromDatanode) payload).getReport());
                }
                eventReceived.incrementAndGet();
              }
            });

    DatanodeDetails datanodeDetails = randomDatanodeDetails();

    SCMHeartbeatRequestProto heartbeat =
        SCMHeartbeatRequestProto.newBuilder()
            .setDatanodeDetails(datanodeDetails.getProtoBufMessage())
            .setContainerReport(containerReport)
            .addCommandStatusReports(commandStatusReport)
            .build();
    dispatcher.dispatch(heartbeat);
    assertEquals(2, eventReceived.get());


  }

  /**
   * Asserts scm informs datanodes to re-register on a restart.
   *
   * @throws Exception
   */
  @Test
  public void testScmHeartbeatAfterRestart() throws Exception {

    NodeManager mockNodeManager = mock(NodeManager.class);
    SCMDatanodeHeartbeatDispatcher dispatcher =
        new SCMDatanodeHeartbeatDispatcher(
            mockNodeManager, mock(EventPublisher.class));

    DatanodeDetails datanodeDetails = randomDatanodeDetails();

    SCMHeartbeatRequestProto heartbeat =
        SCMHeartbeatRequestProto.newBuilder()
            .setDatanodeDetails(datanodeDetails.getProtoBufMessage())
            .build();

    dispatcher.dispatch(heartbeat);
    // If SCM receives heartbeat from a node after it restarts and the node
    // is not registered, it should send a Re-Register command back to the node.
    verify(mockNodeManager, times(1)).addDatanodeCommand(
        any(DatanodeID.class), any(ReregisterCommand.class));
  }
}
