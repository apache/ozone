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

package org.apache.hadoop.ozone.om.jobworker.node;

import static org.apache.hadoop.ozone.OzoneConsts.OM_SERVICE_ID_DEFAULT;
import static org.apache.hadoop.ozone.om.jobworker.OMJobworkerEvents.NEW_JOBWORKER;
import static org.apache.hadoop.ozone.om.jobworker.OMJobworkerEvents.STALE_JOBWORKER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerNodeReportProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerStorageReportProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse.ReturnCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerPortType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.conf.OMJobworkerConfiguration;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeNotFoundException;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.apache.hadoop.ozone.util.RemoteAddressInterceptor;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test {@link JobworkerNodeManager}.
 */
@ExtendWith(MockitoExtension.class)
public class TestJobworkerNodeManager {
  private OzoneManager ozoneManager;
  @TempDir
  private Path folder;
  private String omServiceId1;

  public JobworkerNodeManager getNodeManager(OzoneConfiguration conf) throws Exception {
    OmTestManagers omTestManagers = new OmTestManagers(conf);
    ozoneManager = omTestManagers.getOzoneManager();
    omServiceId1 = ozoneManager.getOMServiceId();
    return ozoneManager.getJobworkerNodemanager();
  }

  @Test
  public void testRegisterJobworker() throws Exception {
    // Prepare
    JobworkerNodeManager nodeManager = getNodeManager(createNewTestPath());
    InetAddress mockInetAddress = mock(InetAddress.class);
    when(mockInetAddress.getHostAddress()).thenReturn("1.2.3.4");
    when(mockInetAddress.getHostName()).thenReturn("test-hostname");
    Context ctx = Context.current().withValue(RemoteAddressInterceptor.REMOTE_ADDR, mockInetAddress);
    UUID jobworkerUuid = UUID.randomUUID();
    JobworkerDetails jobworkerDetails = JobworkerDetails.newBuilder()
        .setUuid(jobworkerUuid)
        .addPort(JobworkerPortType.HTTP, 100)
        .build();
    Context originalContext = ctx.attach();
    try {
      RegisterJobworkerResponse response = nodeManager.registerJobworker(
          jobworkerDetails);

      // Verify response
      assertNotNull(response);
      assertEquals(ReturnCode.SUCCESS, response.getReturnCode());
      assertEquals(ozoneManager.getOMServiceId(), response.getOmServiceId());
      assertEquals(jobworkerUuid.getMostSignificantBits(), response.getJobworkerUUID().getMostSigBits());
      assertEquals(jobworkerUuid.getLeastSignificantBits(), response.getJobworkerUUID().getLeastSigBits());
      assertEquals(ozoneManager.getOmStorage().getClusterID(), response.getClusterID());
      assertEquals(ozoneManager.getNodeDetails().getServiceId(), response.getOmServiceId());
      assertEquals("test-hostname", response.getHostname());
      assertEquals("1.2.3.4", response.getIpAddress());
      assertTrue(nodeManager.isJobworkerNodeRegistered(jobworkerUuid));
    } finally {
      ctx.detach(originalContext);
    }
  }

  @Test
  public void testJobworkerNodeReport() throws Exception {
    // Prepare
    JobworkerNodeManager nodeManager = getNodeManager(createNewTestPath());
    JobworkerDetails jobworkerDetails = registerJobworker(nodeManager);
    UUID jobworkerUuid = jobworkerDetails.getUuid();
    final long capacity = 2000;
    final long remaining = 1900;
    JobworkerStorageReportProto storageReport =
        JobworkerStorageReportProto.newBuilder()
            .setStorageUuid(jobworkerUuid.toString())
            .setStorageLocation(folder.toString())
            .setCapacity(capacity)
            .setRemaining(remaining)
            .setFailed(false)
            .build();
    JobworkerNodeReportProto nodeReport = JobworkerNodeReportProto.newBuilder()
        .addStorageReport(storageReport)
        .build();

    nodeManager.processNodeReport(jobworkerDetails, nodeReport);

    // Verify storage reports were updated
    JobworkerInfo nodeInfo = nodeManager.getNodeStateManager().getNodeInfo(jobworkerUuid);
    List<JobworkerStorageReportProto> storageSummary = nodeInfo.getStorageReports();

    assertEquals(1, storageSummary.size());
    assertEquals(capacity, storageSummary.get(0).getCapacity());
    assertEquals(remaining, storageSummary.get(0).getRemaining());
    assertEquals(jobworkerUuid.toString(), storageSummary.get(0).getStorageUuid());

    // Test with multiple storage reports
    JobworkerStorageReportProto storageReport2 =
        JobworkerStorageReportProto.newBuilder()
            .setStorageUuid(UUID.randomUUID().toString())
            .setStorageLocation(folder.toString() + "/second")
            .setCapacity(capacity)
            .setRemaining(remaining)
            .setFailed(false)
            .build();
    JobworkerNodeReportProto nodeReport2 = JobworkerNodeReportProto.newBuilder()
        .addStorageReport(storageReport)
        .addStorageReport(storageReport2)
        .build();

    // Process the updated node report
    nodeManager.processNodeReport(jobworkerDetails, nodeReport2);

    // Verify the updated storage reports
    storageSummary = nodeInfo.getStorageReports();
    assertEquals(2, storageSummary.size());
    assertEquals(capacity * 2, storageSummary.stream().mapToLong(JobworkerStorageReportProto::getCapacity).sum());
    assertEquals(remaining * 2, storageSummary.stream().mapToLong(JobworkerStorageReportProto::getRemaining).sum());

    // Test with one failed volume
    JobworkerStorageReportProto failedReport =
        JobworkerStorageReportProto.newBuilder()
            .setStorageUuid(UUID.randomUUID().toString())
            .setStorageLocation(folder.toString() + "/failed")
            .setCapacity(0)
            .setRemaining(0)
            .setFailed(true)
            .build();
    JobworkerNodeReportProto nodeReport3 = JobworkerNodeReportProto.newBuilder()
        .addStorageReport(storageReport)
        .addStorageReport(storageReport2)
        .addStorageReport(failedReport)
        .build();

    // Process the report with a failed volume
    nodeManager.processNodeReport(jobworkerDetails, nodeReport3);

    // Verify the stats including the failed volume
    storageSummary = nodeInfo.getStorageReports();
    assertEquals(3, storageSummary.size());
    assertEquals(1, storageSummary.stream().filter(JobworkerStorageReportProto::getFailed).count());
    assertEquals(2, nodeInfo.getHealthyVolumeCount());

    // Test processing a node report from an unregistered node
    JobworkerDetails unregisteredNode = MockJobworkerDetails.randomJobworkerDetails();
    JobworkerNodeReportProto unregReport = JobworkerNodeReportProto.newBuilder()
        .addStorageReport(storageReport)
        .build();

    nodeManager.processNodeReport(unregisteredNode, unregReport);
    storageSummary = nodeInfo.getStorageReports();
    assertEquals(3, storageSummary.size());
  }

  @Test
  public void testJobworkerHeartbeat() throws Exception {
    JobworkerNodeManager nodeManager = getNodeManager(createNewTestPath());
    JobworkerDetails jobworkerDetails = registerJobworker(nodeManager);
    UUID jobworkerUuid = jobworkerDetails.getUuid();
    SendHeartbeatRequest heartbeatRequest = createHeartbeatRequest(jobworkerDetails.getProtoBufMessage());

    long heartbeatTime1 = nodeManager.getNodeStateManager()
        .getNodeInfo(jobworkerUuid).getLastHeartbeatTime();
    assertTrue(nodeManager.isJobworkerNodeRegistered(jobworkerUuid));

    SendHeartbeatResponseProto responseProto =
        ozoneManager.getJobworkerServerProtocol().sendHeartbeat(heartbeatRequest);
    assertTrue(responseProto.getTerm() > 0);
    assertEquals(jobworkerUuid, ProtobufUtils.fromProtobuf(responseProto.getJobworkerUUID()));
    assertEquals(OM_SERVICE_ID_DEFAULT, responseProto.getOmServiceId());
    long heartbeatTime2 = nodeManager.getNodeStateManager()
        .getNodeInfo(jobworkerUuid).getLastHeartbeatTime();
    assertTrue(heartbeatTime2 > heartbeatTime1);
  }

  @Test
  public void testJobworkerNodeStateTransitionAfterHeartbeatTimeout()
      throws Exception {
    // GIVEN
    OzoneConfiguration conf = createNewTestPath();
    OMJobworkerConfiguration jwConf = conf.getObject(OMJobworkerConfiguration.class);
    jwConf.setStaleNodeIntervalMs(500);
    conf.setFromObject(jwConf);
    JobworkerNodeManager nodeManager = getNodeManager(conf);

    JobworkerDetails jobworker = registerJobworker(nodeManager);
    UUID jobworkerUuid = jobworker.getUuid();
    JobworkerInfo nodeInfo = nodeManager.getNodeStateManager()
        .getNodeInfo(jobworkerUuid);
    assertEquals(NodeState.HEALTHY, nodeInfo.getNodeStatus().getHealthState());
    // WHEN
    Thread.sleep(800); // Slightly longer than the stale interval

    nodeManager.checkNodesHealth();

    // THEN
    // Verify the node has transitioned to STALE state
    assertEquals(NodeState.STALE,
        nodeManager.getNodeStateManager().getNodeInfo(jobworkerUuid)
            .getNodeStatus().getHealthState());
    assertEquals(1, nodeManager.getNodeStateManager().getNodeCount(
        NodeState.STALE));
    assertEquals(0, nodeManager.getNodeStateManager().getNodeCount(
        NodeState.HEALTHY));
  }

  @Test
  public void testNodeStateTransitionFromStaleToHealthy() throws Exception {
    // GIVEN
    OzoneConfiguration conf = createNewTestPath();
    OMJobworkerConfiguration jwConf = conf.getObject(OMJobworkerConfiguration.class);
    jwConf.setStaleNodeIntervalMs(500);
    jwConf.setHeartbeatProcessIntervalMs(200);
    conf.setFromObject(jwConf);
    JobworkerNodeManager nodeManager = getNodeManager(conf);
    JobworkerDetails jobworker = registerJobworker(nodeManager);
    UUID jobworkerUuid = jobworker.getUuid();

    // Make the node stale
    Thread.sleep(800);
    nodeManager.checkNodesHealth();

    assertEquals(NodeState.STALE,
        nodeManager.getNodeStateManager().getNodeInfo(jobworkerUuid)
            .getNodeStatus().getHealthState());

    // WHEN
    nodeManager.processHeartbeat(jobworker);

    // THEN
    // Verify node transitions back to HEALTHY
    assertEquals(NodeState.HEALTHY,
        nodeManager.getNodeStateManager().getNodeInfo(jobworkerUuid)
            .getNodeStatus().getHealthState());
  }

  @Test
  public void testStaleJobworkerRemovalTimeout() throws Exception {
    // GIVEN
    OzoneConfiguration conf = createNewTestPath();
    OMJobworkerConfiguration jwConf = conf.getObject(OMJobworkerConfiguration.class);
    jwConf.setStaleNodeIntervalMs(100);
    jwConf.setRemovalTimeoutMs(1000);
    conf.setFromObject(jwConf);
    JobworkerNodeManager nodeManager = getNodeManager(conf);

    JobworkerDetails jobworker = registerJobworker(nodeManager);
    UUID jobworkerUuid = jobworker.getUuid();
    assertTrue(nodeManager.isJobworkerNodeRegistered(jobworkerUuid));
    assertEquals(NodeState.HEALTHY,
        nodeManager.getNodeStateManager().getNodeInfo(jobworkerUuid)
            .getNodeStatus().getHealthState());

    // WHEN
    Thread.sleep(200);
    nodeManager.checkNodesHealth();
    assertEquals(NodeState.STALE,
        nodeManager.getNodeStateManager().getNodeInfo(jobworkerUuid)
            .getNodeStatus().getHealthState());
    assertTrue(nodeManager.isJobworkerNodeRegistered(jobworkerUuid));

    // Wait for removal timeout to pass
    Thread.sleep(1000);
    nodeManager.checkNodesHealth();

    // THEN
    assertFalse(nodeManager.isJobworkerNodeRegistered(jobworkerUuid));
    assertEquals(0, nodeManager.getNodeStateManager().getTotalNodeCount());
  }

  @Test
  public void testJobworkerNodeStateMachine() throws Exception {
    // GIVEN
    JobworkerNodeStateMachine stateMachine = new JobworkerNodeStateMachine();

    // WHEN & THEN
    // Test all valid transitions
    assertEquals(NodeState.STALE,
        stateMachine.getNextState(NodeState.HEALTHY,
            JobworkerNodeStateMachine.NodeLifeCycleEvent.TIMEOUT));

    assertEquals(NodeState.HEALTHY,
        stateMachine.getNextState(NodeState.STALE,
            JobworkerNodeStateMachine.NodeLifeCycleEvent.RESTORE));

    // Test invalid transitions
    try {
      stateMachine.getNextState(NodeState.HEALTHY_READONLY,
          JobworkerNodeStateMachine.NodeLifeCycleEvent.TIMEOUT);
      fail("Expected InvalidStateTransitionException");
    } catch (InvalidStateTransitionException e) {
      // Expected, HEALTHY_READONLY state isn't supported for JobWorker
    }
  }

  @Test
  public void testConcurrentHeartbeats() throws Exception {
    // GIVEN
    OzoneConfiguration conf = createNewTestPath();
    OMJobworkerConfiguration jwConf = conf.getObject(OMJobworkerConfiguration.class);
    jwConf.setHeartbeatProcessIntervalMs(200);
    jwConf.setStaleNodeIntervalMs(10000);
    conf.setFromObject(jwConf);
    JobworkerNodeManager nodeManager = getNodeManager(conf);
    final int nodeCount = 20;
    final CountDownLatch startSignal = new CountDownLatch(1);
    final CountDownLatch doneSignal = new CountDownLatch(nodeCount);
    final ConcurrentHashMap<UUID, JobworkerDetails> allNodes = new ConcurrentHashMap<>();
    for (int i = 0; i < nodeCount; i++) {
      JobworkerDetails jobworker = registerJobworker(nodeManager);
      allNodes.put(jobworker.getUuid(), jobworker);
    }

    // WHEN
    // Simulate concurrent heartbeats from multiple threads
    ExecutorService executor = Executors.newFixedThreadPool(10);
    for (JobworkerDetails jobworker : allNodes.values()) {
      executor.submit(() -> {
        try {
          startSignal.await();
          // Send multiple heartbeats for each node
          for (int i = 0; i < 5; i++) {
            nodeManager.processHeartbeat(jobworker);
            Thread.sleep(5);
          }
        } catch (Exception e) {
          fail("Unexpected exception: " + e.getMessage());
        } finally {
          doneSignal.countDown();
        }
      });
    }

    startSignal.countDown();
    doneSignal.await(5, TimeUnit.SECONDS);
    executor.shutdown();

    // THEN
    // Verify all nodes are still in HEALTHY state
    for (UUID uuid : allNodes.keySet()) {
      assertEquals(NodeState.HEALTHY,
          nodeManager.getNodeStateManager().getNodeInfo(uuid)
              .getNodeStatus().getHealthState());
    }

    // Do not send heartbeat for OM all nodes will be entry STALE state
    GenericTestUtils.waitFor(() -> {
      boolean allNodeStale = true;
      for (UUID uuid : allNodes.keySet()) {
        try {
          if (!nodeManager.getNodeStateManager().getNodeInfo(uuid).getNodeStatus().isStale()) {
            allNodeStale = false;
          }
        } catch (JobworkerNodeNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
      return allNodeStale;
    }, 1000, 30000);

    for (JobworkerDetails jobworkerDetails : allNodes.values()) {
      nodeManager.processHeartbeat(jobworkerDetails);
    }
    // Verify all nodes are recovery HEALTHY
    for (UUID uuid : allNodes.keySet()) {
      assertEquals(NodeState.HEALTHY,
          nodeManager.getNodeStateManager().getNodeInfo(uuid)
              .getNodeStatus().getHealthState());
    }
  }

  @Test
  public void testEventFiredWhenNodeBecomesStale() throws Exception {
    // GIVEN
    OzoneConfiguration conf = createNewTestPath();
    OMJobworkerConfiguration jwConf = conf.getObject(OMJobworkerConfiguration.class);
    jwConf.setStaleNodeIntervalMs(500);
    conf.setFromObject(jwConf);

    // Use a mock EventPublisher to track events
    EventPublisher mockPublisher = mock(EventPublisher.class);
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    Function<String, String> nodeResolver = hostname -> null;

    OMNodeDetails decommNodeDetails = new OMNodeDetails.Builder()
        .setOMNodeId("decommNodeId")
        .setOMServiceId("omServiceId1")
        .setHostAddress("localhost")
        .build();

    JobworkerNodeManager nodeManager = new JobworkerNodeManager(
        nodeResolver, clusterMap, new OMStorage(conf), decommNodeDetails,
        conf, mockPublisher);

    JobworkerDetails jobworker = MockJobworkerDetails.randomJobworkerDetails();
    nodeManager.registerJobworker(jobworker);

    // WHEN - Wait for stale interval and check health
    Thread.sleep(600);
    nodeManager.checkNodesHealth();

    // THEN - STALE_JOBWORKER event should be fired
    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    ArgumentCaptor<JobworkerDetails> nodeCaptor = ArgumentCaptor.forClass(JobworkerDetails.class);

    verify(mockPublisher, times(2)).fireEvent(eventCaptor.capture(), nodeCaptor.capture());

    List<Event> allEvents = eventCaptor.getAllValues();
    List<JobworkerDetails> allNodes = nodeCaptor.getAllValues();

    assertEquals(NEW_JOBWORKER, allEvents.get(0));
    assertEquals(jobworker.getUuid(), allNodes.get(0).getUuid());

    assertEquals(STALE_JOBWORKER, allEvents.get(1));
    assertEquals(jobworker.getUuid(), allNodes.get(1).getUuid());

    nodeManager.close();
  }

  @Test
  public void testMultipleNodesInDifferentHealthStates() throws Exception {
    // GIVEN
    OzoneConfiguration conf = createNewTestPath();
    OMJobworkerConfiguration jwConf = conf.getObject(OMJobworkerConfiguration.class);
    jwConf.setStaleNodeIntervalMs(500);
    jwConf.setRemovalTimeoutMs(1000);
    conf.setFromObject(jwConf);
    JobworkerNodeManager nodeManager = getNodeManager(conf);
    // Create nodes - healthyNodes will remain healthy, staleNodes will become stale
    List<JobworkerDetails> healthyNodes = new ArrayList<>();
    List<JobworkerDetails> staleNodes = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      healthyNodes.add(registerJobworker(nodeManager));
    }
    for (int i = 0; i < 2; i++) {
      staleNodes.add(registerJobworker(nodeManager));
    }

    assertEquals(5, nodeManager.getNodeStateManager().getTotalNodeCount());
    assertEquals(5, nodeManager.getNodeStateManager().getNodeCount(NodeState.HEALTHY));

    // Send heartbeats for all nodes
    for (JobworkerDetails node : healthyNodes) {
      nodeManager.processHeartbeat(node);
    }
    for (JobworkerDetails node : staleNodes) {
      nodeManager.processHeartbeat(node);
    }

    // WHEN - Wait for a stale interval and keep sending heartbeats only for healthy nodes
    Thread.sleep(300);
    for (JobworkerDetails node : healthyNodes) {
      nodeManager.processHeartbeat(node);
    }

    Thread.sleep(300);
    nodeManager.checkNodesHealth();

    assertEquals(3, nodeManager.getNodeStateManager().getNodeCount(NodeState.HEALTHY));
    assertEquals(2, nodeManager.getNodeStateManager().getNodeCount(NodeState.STALE));

    // WHEN - Wait for removal timeout
    Thread.sleep(300);
    for (JobworkerDetails node : healthyNodes) {
      nodeManager.processHeartbeat(node);
    }

    Thread.sleep(800);
    nodeManager.checkNodesHealth();

    // THEN - the HEALTHY nodes have become to remain STALE and the STALE node have been removed.
    assertEquals(3, nodeManager.getNodeStateManager().getTotalNodeCount());
    for (JobworkerDetails node : healthyNodes) {
      assertEquals(NodeState.STALE,
          nodeManager.getNodeStateManager().getNodeInfo(node.getUuid()).getNodeStatus().getHealthState());
    }
  }

  @Test
  public void testJvmPauseHandling() throws Exception {
    // GIVEN
    OzoneConfiguration conf = createNewTestPath();
    OMJobworkerConfiguration jwConf = conf.getObject(OMJobworkerConfiguration.class);
    jwConf.setStaleNodeIntervalMs(500);
    conf.setFromObject(jwConf);
    JobworkerNodeManager nodeManager = getNodeManager(conf);

    for (int i = 0; i < 3; i++) {
      JobworkerDetails jobworker = registerJobworker(nodeManager);
      nodeManager.processHeartbeat(jobworker);
    }
    assertEquals(3, nodeManager.getNodeStateManager().getNodeCount(NodeState.HEALTHY));

    // WHEN - Simulate a JVM pause by manipulating lastHealthCheck
    nodeManager.setLastHealthCheck(Time.monotonicNow() - (jwConf.getStaleNodeIntervalMs() * 2));
    nodeManager.run();

    // THEN - Check should be skipped and nodes should remain HEALTHY
    assertEquals(3, nodeManager.getNodeStateManager().getNodeCount(NodeState.HEALTHY));
    assertEquals(0, nodeManager.getNodeStateManager().getNodeCount(NodeState.STALE));
  }

  private JobworkerDetails registerJobworker(JobworkerNodeManager nodeManager) throws IOException {
    JobworkerDetails jobworkerDetails = MockJobworkerDetails.randomLocalJobworkerDetails();
    RegisterJobworkerResponse registerResponse = nodeManager.registerJobworker(
        jobworkerDetails);
    assertEquals(ReturnCode.SUCCESS, registerResponse.getReturnCode());
    return jobworkerDetails;
  }

  private SendHeartbeatRequest createHeartbeatRequest(JobworkerDetailsProto jobworkerDetailProto) {
    return SendHeartbeatRequest.newBuilder()
        .setJobworkerDetails(jobworkerDetailProto)
        .setOmServiceId(omServiceId1)
        .build();
  }

  private OzoneConfiguration createNewTestPath() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("ozone.om.jobworker.service.enabled", true);
    conf.setInt("ozone.om.jobworker.grpc.port", 0);
    File newFolder = folder.toFile();
    if (!newFolder.exists()) {
      Assertions.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    return conf;
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (ozoneManager != null) {
      ozoneManager.close();
    }
  }
}
