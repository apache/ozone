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
import static org.apache.hadoop.hdds.scm.events.SCMEvents.INCREMENTAL_CONTAINER_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.NODE_REGISTRATION_CONT_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.NODE_REPORT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DatanodeVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.CommandStatusReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.NodeReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.scm.server.SCMFullContainerReportLeaseManager.LeaseClaim;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.ReregisterCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This class tests the behavior of SCMDatanodeHeartbeatDispatcher.
 */
public class TestSCMDatanodeHeartbeatDispatcher {

  @Test
  public void testNodeReportDispatcher() throws IOException, NodeNotFoundException {

    AtomicInteger eventReceived = new AtomicInteger();

    NodeReportProto nodeReport = NodeReportProto.getDefaultInstance();

    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.isNodeRegistered(any()))
        .thenReturn(true);
    when(mockNodeManager.getNodeStatus(any())).thenReturn(NodeStatus.inServiceHealthy());

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
    dispatcher.dispatch(heartbeat, null);
    assertEquals(1, eventReceived.get());


  }

  @Test
  public void testContainerReportDispatcher() throws IOException, NodeNotFoundException {


    AtomicInteger eventReceived = new AtomicInteger();

    ContainerReportsProto containerReport =
        ContainerReportsProto.getDefaultInstance();
    CommandStatusReportsProto commandStatusReport =
        CommandStatusReportsProto.getDefaultInstance();

    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.isNodeRegistered(any()))
        .thenReturn(true);
    when(mockNodeManager.getNodeStatus(any())).thenReturn(NodeStatus.inServiceHealthy());

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
    dispatcher.dispatch(heartbeat, new ContainerReportFromDatanode(datanodeDetails, containerReport));
    assertEquals(2, eventReceived.get());


  }

  @Test
  public void testVersionReportProcessedOnlyWhenPresent() throws IOException, NodeNotFoundException {
    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.isNodeRegistered(any())).thenReturn(true);
    when(mockNodeManager.getNodeStatus(any())).thenReturn(NodeStatus.inServiceHealthy());

    SCMDatanodeHeartbeatDispatcher dispatcher =
        new SCMDatanodeHeartbeatDispatcher(mockNodeManager, mock(EventPublisher.class));

    DatanodeDetails datanodeDetails = randomDatanodeDetails();

    // Heartbeat without a version report: SCM should not process a version.
    dispatcher.dispatch(SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanodeDetails.getProtoBufMessage())
        .build(), null);
    verify(mockNodeManager, times(0)).processVersionReport(any(), any());

    // Heartbeat with a version report: SCM should process it.
    DatanodeVersionProto version = DatanodeVersionProto.newBuilder()
        .setApparentVersion(1).setSoftwareVersion(1).build();
    dispatcher.dispatch(SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanodeDetails.getProtoBufMessage())
        .setDatanodeVersion(version)
        .build(), null);
    verify(mockNodeManager, times(1)).processVersionReport(any(), any());
  }

  private static Stream<NodeStatus> unhealthyNodeStatuses() {
    return Stream.of(NodeStatus.inServiceStale(), NodeStatus.inServiceDead());
  }

  @Test
  public void testHealthyNodeWithVersionReportIsNeverFenced() throws Exception {
    // Healthy nodes passed the version gate at registration; their version only changes on restart,
    // which causes re-registration on its own. The dispatcher must never fence a healthy node.
    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.isNodeRegistered(any())).thenReturn(true);
    when(mockNodeManager.getNodeStatus(any())).thenReturn(NodeStatus.inServiceHealthy());

    SCMDatanodeHeartbeatDispatcher dispatcher =
        new SCMDatanodeHeartbeatDispatcher(mockNodeManager, mock(EventPublisher.class));

    DatanodeVersionProto version = DatanodeVersionProto.newBuilder()
        .setApparentVersion(1).setSoftwareVersion(1).build();
    SCMHeartbeatRequestProto heartbeat = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(randomDatanodeDetails().getProtoBufMessage())
        .setDatanodeVersion(version)
        .build();

    dispatcher.dispatch(heartbeat, null);

    verify(mockNodeManager, never()).addDatanodeCommand(any(DatanodeID.class), any(ReregisterCommand.class));
    verify(mockNodeManager, times(1)).processHeartbeat(any(), any());
  }

  @ParameterizedTest
  @MethodSource("unhealthyNodeStatuses")
  public void testUnhealthyNodeWithIncompatibleVersionGetsReregisterCommand(
      NodeStatus nodeStatus) throws Exception {
    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.isNodeRegistered(any())).thenReturn(true);
    when(mockNodeManager.getNodeStatus(any())).thenReturn(nodeStatus);
    when(mockNodeManager.shouldFenceDatanode(any(), any())).thenReturn(true);

    SCMDatanodeHeartbeatDispatcher dispatcher =
        new SCMDatanodeHeartbeatDispatcher(mockNodeManager, mock(EventPublisher.class));

    DatanodeVersionProto version = DatanodeVersionProto.newBuilder()
        .setApparentVersion(1).setSoftwareVersion(1).build();
    SCMHeartbeatRequestProto heartbeat = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(randomDatanodeDetails().getProtoBufMessage())
        .setDatanodeVersion(version)
        .build();

    dispatcher.dispatch(heartbeat, null);

    verify(mockNodeManager, times(1)).addDatanodeCommand(any(DatanodeID.class), any(ReregisterCommand.class));
    verify(mockNodeManager, never()).processHeartbeat(any(), any());
  }

  @ParameterizedTest
  @MethodSource("unhealthyNodeStatuses")
  public void testUnhealthyNodeWithCompatibleVersionProcessesHeartbeatNormally(
      NodeStatus nodeStatus) throws Exception {
    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.isNodeRegistered(any())).thenReturn(true);
    when(mockNodeManager.getNodeStatus(any())).thenReturn(nodeStatus);
    when(mockNodeManager.shouldFenceDatanode(any(), any())).thenReturn(false);

    SCMDatanodeHeartbeatDispatcher dispatcher =
        new SCMDatanodeHeartbeatDispatcher(mockNodeManager, mock(EventPublisher.class));

    DatanodeVersionProto version = DatanodeVersionProto.newBuilder()
        .setApparentVersion(1).setSoftwareVersion(1).build();
    SCMHeartbeatRequestProto heartbeat = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(randomDatanodeDetails().getProtoBufMessage())
        .setDatanodeVersion(version)
        .build();

    dispatcher.dispatch(heartbeat, null);

    verify(mockNodeManager, never()).addDatanodeCommand(any(DatanodeID.class), any(ReregisterCommand.class));
    verify(mockNodeManager, times(1)).processHeartbeat(any(), any());
  }

  @Test
  public void testHeartbeatContainerReportIsIgnoredWithoutPreparedReport() throws NodeNotFoundException {
    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.isNodeRegistered(any())).thenReturn(true);
    when(nodeManager.getNodeStatus(any())).thenReturn(NodeStatus.inServiceHealthy());
    EventPublisher publisher = mock(EventPublisher.class);
    SCMDatanodeHeartbeatDispatcher dispatcher = new SCMDatanodeHeartbeatDispatcher(nodeManager, publisher);
    SCMHeartbeatRequestProto heartbeat = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(randomDatanodeDetails().getProtoBufMessage())
        .setContainerReport(ContainerReportsProto.getDefaultInstance())
        .setNodeReport(NodeReportProto.getDefaultInstance())
        .addIncrementalContainerReport(IncrementalContainerReportProto.getDefaultInstance())
        .build();

    dispatcher.dispatch(heartbeat, null);

    verify(publisher, never()).fireEvent(eq(CONTAINER_REPORT), any());
    verify(publisher).fireEvent(eq(NODE_REPORT), any());
    verify(publisher).fireEvent(eq(INCREMENTAL_CONTAINER_REPORT), any());
  }

  @Test
  public void testRegistrationContainerReportDoesNotRetainReplicaPayload() {
    DatanodeDetails datanode = randomDatanodeDetails();
    ContainerReportsProto report = ContainerReportsProto.newBuilder()
        .addReports(ContainerReplicaProto.newBuilder().setContainerID(42L)
            .setState(ContainerReplicaProto.State.CLOSED).setUsed(1024L).setKeyCount(100L))
        .addReports(ContainerReplicaProto.newBuilder().setContainerID(Long.MAX_VALUE)
            .setState(ContainerReplicaProto.State.QUASI_CLOSED).setDataChecksum(99L))
        .build();

    NodeRegistrationContainerReport registration = new NodeRegistrationContainerReport(datanode, report);

    assertSame(datanode, registration.getDatanodeDetails());
    assertArrayEquals(new long[] {42L, Long.MAX_VALUE}, registration.getContainerIDs());
    assertEquals(Object.class, NodeRegistrationContainerReport.class.getSuperclass());
    for (Field field : NodeRegistrationContainerReport.class.getDeclaredFields()) {
      assertTrue(field.getType() == DatanodeDetails.class || field.getType() == long[].class,
          "Registration events must only retain datanode details and primitive container IDs");
    }
  }

  @Test
  public void testRegistrationContainerReportWithNullOrEmptyReport() {
    DatanodeDetails datanode = randomDatanodeDetails();

    assertArrayEquals(new long[0], new NodeRegistrationContainerReport(datanode, null).getContainerIDs());
    assertArrayEquals(new long[0],
        new NodeRegistrationContainerReport(datanode, ContainerReportsProto.getDefaultInstance()).getContainerIDs());
  }

  @Test
  public void testContainerReportDispatcherUsesProvidedReport() throws NodeNotFoundException {
    AtomicBoolean providedReportDispatched = new AtomicBoolean();
    AtomicBoolean registrationReportDispatched = new AtomicBoolean();
    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.isNodeRegistered(any())).thenReturn(true);
    when(nodeManager.getNodeStatus(any())).thenReturn(NodeStatus.inServiceHealthy());
    EventPublisher publisher = new EventPublisher() {
      @Override
      public <PAYLOAD, EVENT extends Event<PAYLOAD>> void fireEvent(
          EVENT event, PAYLOAD payload) {
        if (event.equals(CONTAINER_REPORT)) {
          providedReportDispatched.set(((ContainerReportFromDatanode) payload).isRegistrationReport());
          assertEquals(1, ((ContainerReportFromDatanode) payload).getReport().getReportsCount());
        } else if (event.equals(NODE_REGISTRATION_CONT_REPORT)) {
          registrationReportDispatched.set(true);
          assertArrayEquals(new long[0], ((NodeRegistrationContainerReport) payload).getContainerIDs());
        }
      }
    };
    SCMDatanodeHeartbeatDispatcher dispatcher =
        new SCMDatanodeHeartbeatDispatcher(nodeManager, publisher);
    DatanodeDetails datanode = randomDatanodeDetails();
    ContainerReportsProto report = ContainerReportsProto.newBuilder()
        .addReports(ContainerReplicaProto.newBuilder().setContainerID(42L).setState(ContainerReplicaProto.State.CLOSED))
        .build();
    ContainerReportFromDatanode providedReport =
        new ContainerReportFromDatanode(datanode, report, true);
    SCMHeartbeatRequestProto heartbeat = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanode.getProtoBufMessage())
        .build();

    dispatcher.dispatch(heartbeat, providedReport);

    assertTrue(providedReportDispatched.get());
    assertFalse(registrationReportDispatched.get());
  }

  @Test
  public void testContainerReportDispatcherCompletesUndispatchedReport() {
    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.isNodeRegistered(any())).thenReturn(false);
    SCMDatanodeHeartbeatDispatcher dispatcher =
        new SCMDatanodeHeartbeatDispatcher(nodeManager,
            mock(EventPublisher.class));
    DatanodeDetails datanode = randomDatanodeDetails();
    LeaseClaim claim = spy(claimLease(datanode));
    ContainerReportsProto report = ContainerReportsProto.getDefaultInstance();
    ContainerReportFromDatanode providedReport =
        new ContainerReportFromDatanode(datanode, report, claim);
    SCMHeartbeatRequestProto heartbeat = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanode.getProtoBufMessage())
        .setContainerReport(report)
        .build();

    dispatcher.dispatch(heartbeat, providedReport);

    verify(claim).complete(false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testLeasedReportDerivesRegistrationStatusFromClaim(boolean registrationDeferred) {
    DatanodeDetails datanode = randomDatanodeDetails();
    SCMFullContainerReportLeaseManager leases = new SCMFullContainerReportLeaseManager(1, 5_000L, true, () -> 0L, null);
    if (registrationDeferred) {
      leases.markFullContainerReportDeferred(datanode);
    }
    long leaseId = leases.requestLease(datanode).getAsLong();
    LeaseClaim claim = leases.claimLease(datanode, leaseId).orElseThrow(AssertionError::new);

    ContainerReportFromDatanode report =
        new ContainerReportFromDatanode(datanode, ContainerReportsProto.getDefaultInstance(), claim);

    assertEquals(registrationDeferred, report.isRegistrationReport());
    assertTrue(report.startProcessing());
    report.complete(true);
    assertTrue(leases.requestLease(datanode).isPresent());
  }

  @Test
  public void testLeasedReportRequiresClaim() {
    assertThrows(NullPointerException.class, () -> new ContainerReportFromDatanode(
        randomDatanodeDetails(), ContainerReportsProto.getDefaultInstance(), (LeaseClaim) null));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testUndispatchedContainerReportReleasesLeaseOnPublicationFailure(boolean failOnContainerReport)
      throws NodeNotFoundException {
    DatanodeDetails datanode = randomDatanodeDetails();
    SCMFullContainerReportLeaseManager leases = new SCMFullContainerReportLeaseManager(1, 5_000L, true, () -> 0L, null);
    long leaseId = leases.requestLease(datanode).getAsLong();
    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        leases.claimLease(datanode, leaseId).orElseThrow(AssertionError::new);
    ContainerReportFromDatanode report =
        new ContainerReportFromDatanode(datanode, ContainerReportsProto.getDefaultInstance(), claim);
    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.isNodeRegistered(any())).thenReturn(true);
    when(nodeManager.getNodeStatus(any())).thenReturn(NodeStatus.inServiceHealthy());
    EventPublisher publisher = new EventPublisher() {
      @Override
      public <PAYLOAD, EVENT extends Event<PAYLOAD>> void fireEvent(EVENT event, PAYLOAD payload) {
        if (event.equals(failOnContainerReport ? CONTAINER_REPORT : NODE_REPORT)) {
          throw new IllegalStateException("Report dispatch failed");
        }
      }
    };
    SCMDatanodeHeartbeatDispatcher dispatcher = new SCMDatanodeHeartbeatDispatcher(nodeManager, publisher);
    SCMHeartbeatRequestProto heartbeat = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanode.getProtoBufMessage())
        .setNodeReport(NodeReportProto.getDefaultInstance())
        .setContainerReport(report.getReport())
        .build();

    assertThrows(IllegalStateException.class, () -> dispatcher.dispatch(heartbeat, report));

    assertTrue(leases.requestLease(randomDatanodeDetails()).isPresent());
  }

  @Test
  public void testContainerReportCompletesWhenRegistrationCheckFails() {
    DatanodeDetails datanode = randomDatanodeDetails();
    LeaseClaim claim = spy(claimLease(datanode));
    ContainerReportFromDatanode report = new ContainerReportFromDatanode(
        datanode, ContainerReportsProto.getDefaultInstance(), claim);
    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.isNodeRegistered(any())).thenThrow(new IllegalStateException("Registration check failed"));
    SCMDatanodeHeartbeatDispatcher dispatcher =
        new SCMDatanodeHeartbeatDispatcher(nodeManager, mock(EventPublisher.class));
    SCMHeartbeatRequestProto heartbeat = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanode.getProtoBufMessage()).build();

    assertThrows(IllegalStateException.class, () -> dispatcher.dispatch(heartbeat, report));
    verify(claim).complete(false);
    report.complete(false);

    verify(claim).complete(false);
  }

  @Test
  public void testDispatchedContainerReportKeepsCompletionOwnership() throws NodeNotFoundException {
    DatanodeDetails datanode = randomDatanodeDetails();
    SCMFullContainerReportLeaseManager leases = new SCMFullContainerReportLeaseManager(1, 5_000L, true, () -> 0L, null);
    long leaseId = leases.requestLease(datanode).getAsLong();
    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        leases.claimLease(datanode, leaseId).orElseThrow(AssertionError::new);
    ContainerReportQueue queue = new ContainerReportQueue();
    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.isNodeRegistered(any())).thenReturn(true);
    when(nodeManager.getNodeStatus(any())).thenReturn(NodeStatus.inServiceHealthy());
    EventPublisher publisher = new EventPublisher() {
      @Override
      public <PAYLOAD, EVENT extends Event<PAYLOAD>> void fireEvent(
          EVENT event, PAYLOAD payload) {
        if (event.equals(CONTAINER_REPORT)) {
          queue.add((ContainerReportFromDatanode) payload);
        } else if (event.equals(INCREMENTAL_CONTAINER_REPORT)) {
          throw new IllegalStateException("ICR dispatch failed");
        }
      }
    };
    SCMDatanodeHeartbeatDispatcher dispatcher =
        new SCMDatanodeHeartbeatDispatcher(nodeManager, publisher);
    ContainerReportsProto report = ContainerReportsProto.getDefaultInstance();
    ContainerReportFromDatanode providedReport =
        new ContainerReportFromDatanode(datanode, report, claim);
    SCMHeartbeatRequestProto heartbeat = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanode.getProtoBufMessage())
        .setContainerReport(report)
        .addIncrementalContainerReport(
            IncrementalContainerReportProto.getDefaultInstance())
        .build();

    assertThrows(IllegalStateException.class,
        () -> dispatcher.dispatch(heartbeat, providedReport));
    DatanodeDetails nextDatanode = randomDatanodeDetails();
    assertFalse(leases.requestLease(nextDatanode).isPresent());
    ContainerReportFromDatanode queuedReport = (ContainerReportFromDatanode) queue.remove();
    assertSame(providedReport, queuedReport);
    assertTrue(queuedReport.startProcessing());
    queuedReport.complete(true);
    assertTrue(leases.requestLease(nextDatanode).isPresent());
  }

  private static LeaseClaim claimLease(DatanodeDetails datanode) {
    SCMFullContainerReportLeaseManager leases = new SCMFullContainerReportLeaseManager(1, 5_000L, true, () -> 0L, null);
    long leaseId = leases.requestLease(datanode).getAsLong();
    return leases.claimLease(datanode, leaseId).orElseThrow(AssertionError::new);
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

    dispatcher.dispatch(heartbeat, null);
    // If SCM receives heartbeat from a node after it restarts and the node
    // is not registered, it should send a Re-Register command back to the node.
    verify(mockNodeManager, times(1)).addDatanodeCommand(
        any(DatanodeID.class), any(ReregisterCommand.class));
  }
}
