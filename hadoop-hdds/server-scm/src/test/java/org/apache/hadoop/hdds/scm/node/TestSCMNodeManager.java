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

package org.apache.hadoop.hdds.scm.node;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.createDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.finalizeNewLayoutVersionCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode.errorNodeNotPermitted;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode.success;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getRandomPipelineReports;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND_COUNT_UPDATED;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.NEW_NODE;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.toLayoutVersionProto;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.NodeReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the SCM Node Manager class.
 */
public class TestSCMNodeManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSCMNodeManager.class);

  private File testDir;
  private StorageContainerManager scm;
  private SCMContext scmContext;

  private static final int MAX_LV = HDDSLayoutVersionManager.maxLayoutVersion();
  private static final LayoutVersionProto LARGER_SLV_LAYOUT_PROTO =
      toLayoutVersionProto(MAX_LV, MAX_LV + 1);
  private static final LayoutVersionProto SMALLER_MLV_LAYOUT_PROTO =
      toLayoutVersionProto(MAX_LV - 1, MAX_LV);
  // In a real cluster, startup is disallowed if MLV is larger than SLV, so
  // increase both numbers to test smaller SLV or larger MLV.
  private static final LayoutVersionProto SMALLER_MLV_SLV_LAYOUT_PROTO =
      toLayoutVersionProto(MAX_LV - 1, MAX_LV - 1);
  private static final LayoutVersionProto LARGER_MLV_SLV_LAYOUT_PROTO =
      toLayoutVersionProto(MAX_LV + 1, MAX_LV + 1);
  private static final LayoutVersionProto CORRECT_LAYOUT_PROTO =
      toLayoutVersionProto(MAX_LV, MAX_LV);

  @BeforeEach
  public void setup() {
    testDir = PathUtils.getTestDir(
        TestSCMNodeManager.class);
  }

  @AfterEach
  public void cleanup() {
    if (scm != null) {
      scm.stop();
      scm.join();
    }
    FileUtil.fullyDelete(testDir);
  }

  /**
   * Returns a new copy of Configuration.
   *
   * @return Config
   */
  OzoneConfiguration getConf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
    return conf;
  }

  /**
   * Creates a NodeManager.
   *
   * @param config - Config for the node manager.
   * @return SCNNodeManager
   * @throws IOException
   */

  SCMNodeManager createNodeManager(OzoneConfiguration config)
      throws IOException, AuthenticationException {
    scm = HddsTestUtils.getScm(config);
    scmContext = new SCMContext.Builder()
        .setSafeModeStatus(SCMSafeModeManager.SafeModeStatus.PRE_CHECKS_PASSED)
        .setLeader(true)
        .setSCM(scm).build();
    PipelineManagerImpl pipelineManager =
        (PipelineManagerImpl) scm.getPipelineManager();
    pipelineManager.setScmContext(scmContext);
    return (SCMNodeManager) scm.getScmNodeManager();
  }

  /**
   * Tests that Node manager handles heartbeats correctly, and comes out of
   * safe Mode.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmHeartbeat()
      throws IOException, InterruptedException, AuthenticationException {

    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {
      int registeredNodes = 5;
      // Send some heartbeats from different nodes.
      for (int x = 0; x < registeredNodes; x++) {
        DatanodeDetails datanodeDetails = HddsTestUtils
            .createRandomDatanodeAndRegister(nodeManager);
        nodeManager.processHeartbeat(datanodeDetails);
      }

      //TODO: wait for heartbeat to be processed
      Thread.sleep(4 * 1000);
      assertEquals(nodeManager.getAllNodes().size(), registeredNodes,
          "Heartbeat thread should have picked up the scheduled heartbeats.");
    }
  }

  @Test
  public void testGetLastHeartbeatTimeDiff() throws Exception {
    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {
      String timeNow = nodeManager.getLastHeartbeatTimeDiff(Time.monotonicNow());
      assertEquals("Just now", timeNow);

      String time1s = nodeManager.getLastHeartbeatTimeDiff(Time.monotonicNow() - 10000);
      assertEquals("10s ago", time1s);

      String time1m = nodeManager.getLastHeartbeatTimeDiff(Time.monotonicNow() - 60000);
      assertEquals("1m ago", time1m);

      String time1m10s = nodeManager.getLastHeartbeatTimeDiff(Time.monotonicNow() - 70000);
      assertEquals("1m 10s ago", time1m10s);

      // 1h 1m 10s
      // 10000ms = 10s
      // 60000ms = 1m
      // 60000ms * 60 = 3600000ms = 1h
      String time1h1m10s = nodeManager.getLastHeartbeatTimeDiff(Time.monotonicNow() - 3670000);
      assertEquals("1h 1m 10s ago", time1h1m10s);

      // 1d 1h 1m 10s
      // 10000ms = 10s
      // 60000ms = 1m
      // 60000ms * 60 = 3600000ms = 1h
      // 3600000ms * 24 = 86400000ms = 1d
      String time1d1h1m10s = nodeManager.getLastHeartbeatTimeDiff(Time.monotonicNow() - 90070000);
      assertEquals("1d 1h 1m 10s ago", time1d1h1m10s);
    }
  }

  /**
   * Create {@link DatanodeDetails} to register with {@code nodeManager}, and
   * provide the datanode maximum capacity so that space used does not block
   * pipeline creation.
   * @return The created {@link DatanodeDetails}.
   */
  private DatanodeDetails registerWithCapacity(SCMNodeManager nodeManager) {
    return registerWithCapacity(nodeManager,
        UpgradeUtils.defaultLayoutVersionProto(), success);
  }

  /**
   * Create {@link DatanodeDetails} to register with {@code nodeManager}, and
   * provide the datanode maximum capacity so that space used does not block
   * pipeline creation. Also check that the result of registering matched
   * {@code expectedResult}.
   * @return The created {@link DatanodeDetails}.
   */
  private DatanodeDetails registerWithCapacity(SCMNodeManager nodeManager,
      LayoutVersionProto layout, ErrorCode expectedResult) {
    DatanodeDetails details = MockDatanodeDetails.randomDatanodeDetails();

    StorageReportProto storageReport =
        HddsTestUtils.createStorageReport(details.getID(),
            details.getNetworkFullPath(), Long.MAX_VALUE);
    MetadataStorageReportProto metadataStorageReport =
        HddsTestUtils.createMetadataStorageReport(details.getNetworkFullPath(),
            Long.MAX_VALUE);

    RegisteredCommand cmd = nodeManager.register(
        MockDatanodeDetails.randomDatanodeDetails(),
        HddsTestUtils.createNodeReport(Arrays.asList(storageReport),
            Arrays.asList(metadataStorageReport)),
        getRandomPipelineReports(), layout);

    assertEquals(expectedResult, cmd.getError());
    return cmd.getDatanode();
  }

  /**
   * Tests that node manager handles layout versions for newly registered nodes
   * correctly.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmLayoutOnRegister()
      throws Exception {

    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL,
        1, TimeUnit.DAYS);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      assertTrue(scm.getScmContext().isLeader());
      // Nodes with mismatched SLV cannot join the cluster.
      registerWithCapacity(nodeManager,
          LARGER_SLV_LAYOUT_PROTO, errorNodeNotPermitted);
      registerWithCapacity(nodeManager,
          SMALLER_MLV_SLV_LAYOUT_PROTO, errorNodeNotPermitted);
      registerWithCapacity(nodeManager,
          LARGER_MLV_SLV_LAYOUT_PROTO, errorNodeNotPermitted);
      // Nodes with mismatched MLV can join, but should not be allowed in
      // pipelines.
      DatanodeDetails badMlvNode1 = registerWithCapacity(nodeManager,
          SMALLER_MLV_LAYOUT_PROTO, success);
      DatanodeDetails badMlvNode2 = registerWithCapacity(nodeManager,
          SMALLER_MLV_LAYOUT_PROTO, success);
      // This node has correct MLV and SLV, so it can join and be used in
      // pipelines.
      DatanodeDetails goodNode = registerWithCapacity(nodeManager,
          CORRECT_LAYOUT_PROTO, success);

      assertEquals(3, nodeManager.getAllNodes().size());

      scm.exitSafeMode();

      assertPipelines(HddsProtos.ReplicationFactor.ONE,
          count -> count == 3,
          Arrays.asList(badMlvNode1, badMlvNode2, goodNode));
      assertPipelines(HddsProtos.ReplicationFactor.THREE,
          count -> count >= 1,
          Arrays.asList(badMlvNode1, badMlvNode2, goodNode));
    }
  }

  private void assertPipelines(HddsProtos.ReplicationFactor factor,
      Predicate<Integer> countCheck, Collection<DatanodeDetails> allowedDNs)
      throws Exception {

    Set<String> allowedDnIds = allowedDNs.stream()
        .map(DatanodeDetails::getUuidString)
        .collect(Collectors.toSet());

    RatisReplicationConfig replConfig = RatisReplicationConfig
        .getInstance(factor);

    // Wait for the expected number of pipelines using allowed DNs.
    GenericTestUtils.waitFor(() -> {
      // Closed pipelines are no longer in operation so we should not count
      // them. We cannot check for open pipelines only because this is a mock
      // test so the pipelines may remain in ALLOCATED state.
      List<Pipeline> pipelines = scm.getPipelineManager()
          .getPipelines(replConfig)
          .stream()
          .filter(p -> p.getPipelineState() != Pipeline.PipelineState.CLOSED)
          .collect(Collectors.toList());
      LOG.info("Found {} non-closed pipelines of type {} and factor {}.",
          pipelines.size(),
          replConfig.getReplicationType(), replConfig.getReplicationFactor());
      boolean success = countCheck.test(pipelines.size());

      // If we have the correct number of pipelines, make sure that none of
      // these pipelines use nodes outside of allowedDNs.
      if (success) {
        for (Pipeline pipeline: pipelines) {
          for (DatanodeDetails pipelineDN: pipeline.getNodes()) {
            // Do not wait for this condition to be true. Disallowed DNs should
            // never be used once we have the expected number of pipelines.
            if (!allowedDnIds.contains(pipelineDN.getUuidString())) {
              String message = String.format("Pipeline %s used datanode %s " +
                      "which is not in the set of allowed datanodes: %s",
                  pipeline.getId().toString(), pipelineDN.getUuidString(),
                  allowedDnIds);
              fail(message);
            }
          }
        }
      }

      return success;
    }, 1000, 10000);
  }

  /**
   * asserts that if we send no heartbeats node manager stays in safemode.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmNoHeartbeats()
      throws IOException, InterruptedException, AuthenticationException {

    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {
      //TODO: wait for heartbeat to be processed
      Thread.sleep(4 * 1000);
      assertEquals(0, nodeManager.getAllNodes().size(),
          "No heartbeats, 0 nodes should be registered");
    }
  }

  /**
   * Asserts that adding heartbeats after shutdown does not work. This implies
   * that heartbeat thread has been shutdown safely by closing the node
   * manager.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmShutdown()
      throws IOException, InterruptedException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    conf.getTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    SCMNodeManager nodeManager = createNodeManager(conf);
    DatanodeDetails datanodeDetails = HddsTestUtils
        .createRandomDatanodeAndRegister(nodeManager);
    // These should never be processed.
    nodeManager.processHeartbeat(datanodeDetails);

    // Let us just wait for 2 seconds to prove that HBs are not processed.
    Thread.sleep(2 * 1000);

    //TODO: add assertion
  }

  /**
   * Asserts that we detect as many healthy nodes as we have generated heartbeat
   * for.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmHealthyNodeCount()
      throws IOException, InterruptedException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    final int count = 10;

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      for (int x = 0; x < count; x++) {
        DatanodeDetails datanodeDetails = HddsTestUtils
            .createRandomDatanodeAndRegister(nodeManager);
        nodeManager.processHeartbeat(datanodeDetails);
      }
      //TODO: wait for heartbeat to be processed
      Thread.sleep(4 * 1000);
      assertEquals(count, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));

      Map<String, Map<String, Integer>> nodeCounts = nodeManager.getNodeCount();
      assertEquals(count,
          nodeCounts.get(HddsProtos.NodeOperationalState.IN_SERVICE.name())
              .get(HddsProtos.NodeState.HEALTHY.name()).intValue());
    }
  }

  /**
   * Asserts that if Stale Interval value is more than 5 times the value of HB
   * processing thread it is a sane value.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmSanityOfUserConfig2()
      throws IOException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    final int interval = 100;
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, TimeUnit.SECONDS);

    // This should be 5 times more than  OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL
    // and 3 times more than OZONE_SCM_HEARTBEAT_INTERVAL
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3 * 1000, MILLISECONDS);
    createNodeManager(conf).close();
  }

  /**
   * For leader SCM, ensure that a change to the operationalState of a node
   * fires a SCMCommand of type SetNodeOperationalStateCommand.
   *
   * For follower SCM, no SetNodeOperationalStateCommand should be fired, yet
   * operationalState of the node will be updated according to the heartbeat.
   */
  @Test
  public void testSetNodeOpStateAndCommandFired()
      throws IOException, NodeNotFoundException, AuthenticationException {
    final int interval = 100;

    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
        MILLISECONDS);
    // If factor 1 pipelines are auto created, registering the new node will
    // trigger a pipeline creation command which may interfere with command
    // checking in this test.
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      DatanodeDetails dn = HddsTestUtils.createRandomDatanodeAndRegister(
          nodeManager);

      long expiry = System.currentTimeMillis() / 1000 + 1000;
      nodeManager.setNodeOperationalState(dn,
          HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE, expiry);

      // If found mismatch, leader SCM fires a SetNodeOperationalStateCommand
      // to update the opState persisted in Datanode.
      scm.getScmContext().updateLeaderAndTerm(true, 1);
      List<SCMCommand<?>> commands = nodeManager.processHeartbeat(dn);

      assertEquals(SetNodeOperationalStateCommand.class,
          commands.get(0).getClass());
      assertEquals(1, commands.size());

      // If found mismatch, follower SCM update its own opState according
      // to the heartbeat, and no SCMCommand will be fired.
      scm.getScmContext().updateLeaderAndTerm(false, 2);
      commands = nodeManager.processHeartbeat(dn);

      assertEquals(0, commands.size());

      NodeStatus scmStatus = nodeManager.getNodeStatus(dn);
      assertTrue(scmStatus.getOperationalState() == dn.getPersistedOpState()
          && scmStatus.getOpStateExpiryEpochSeconds()
          == dn.getPersistedOpStateExpiryEpochSec());
    }
  }

  /**
   * Asserts that a single node moves from Healthy to stale node, then from
   * stale node to dead node if it misses enough heartbeats.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmDetectStaleAndDeadNode()
      throws IOException, InterruptedException, AuthenticationException {
    final int interval = 100;
    final int nodeCount = 10;

    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
        MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);


    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      List<DatanodeDetails> nodeList = createNodeSet(nodeManager, nodeCount);


      DatanodeDetails staleNode = HddsTestUtils.createRandomDatanodeAndRegister(
          nodeManager);

      // Heartbeat once
      nodeManager.processHeartbeat(staleNode);

      // Heartbeat all other nodes.
      for (DatanodeDetails dn : nodeList) {
        nodeManager.processHeartbeat(dn);
      }

      // Wait for 2 seconds .. and heartbeat good nodes again.
      Thread.sleep(2 * 1000);

      for (DatanodeDetails dn : nodeList) {
        nodeManager.processHeartbeat(dn);
      }

      // Wait for 2 seconds, wait a total of 4 seconds to make sure that the
      // node moves into stale state.
      Thread.sleep(2 * 1000);
      List<DatanodeDetails> staleNodeList =
          nodeManager.getNodes(NodeStatus.inServiceStale());
      assertEquals(1, nodeManager.getNodeCount(NodeStatus.inServiceStale()),
          "Expected to find 1 stale node");
      assertEquals(1, staleNodeList.size(),
          "Expected to find 1 stale node");
      assertEquals(staleNode.getID(), staleNodeList.get(0).getID(),
          "Stale node is not the expected ID");

      Map<String, Map<String, Integer>> nodeCounts = nodeManager.getNodeCount();
      assertEquals(1,
          nodeCounts.get(HddsProtos.NodeOperationalState.IN_SERVICE.name())
              .get(HddsProtos.NodeState.STALE.name()).intValue());

      Thread.sleep(1000);
      // heartbeat good nodes again.
      for (DatanodeDetails dn : nodeList) {
        nodeManager.processHeartbeat(dn);
      }

      //  6 seconds is the dead window for this test , so we wait a total of
      // 7 seconds to make sure that the node moves into dead state.
      Thread.sleep(2 * 1000);

      // the stale node has been removed
      staleNodeList = nodeManager.getNodes(NodeStatus.inServiceStale());
      nodeCounts = nodeManager.getNodeCount();
      assertEquals(0, nodeManager.getNodeCount(NodeStatus.inServiceStale()),
          "Expected to find 1 stale node");
      assertEquals(0, staleNodeList.size(),
          "Expected to find 1 stale node");
      assertEquals(0,
          nodeCounts.get(HddsProtos.NodeOperationalState.IN_SERVICE.name())
              .get(HddsProtos.NodeState.STALE.name()).intValue());

      // Check for the dead node now.
      List<DatanodeDetails> deadNodeList =
          nodeManager.getNodes(NodeStatus.inServiceDead());
      assertEquals(1, nodeManager.getNodeCount(NodeStatus.inServiceDead()),
          "Expected to find 1 dead node");
      assertEquals(1, deadNodeList.size(), "Expected to find 1 dead node");
      assertEquals(1,
          nodeCounts.get(HddsProtos.NodeOperationalState.IN_SERVICE.name())
              .get(HddsProtos.NodeState.DEAD.name()).intValue());
      assertEquals(staleNode.getID(), deadNodeList.get(0).getID(),
          "Dead node is not the expected ID");
    }
  }

  /**
   * Simulate a JVM Pause by pausing the health check process
   * Ensure that none of the nodes with heartbeats become Dead or Stale.
   * @throws IOException
   * @throws InterruptedException
   * @throws AuthenticationException
   */
  @Test
  void testScmHandleJvmPause() throws Exception {
    final int healthCheckInterval = 200; // milliseconds
    final int heartbeatInterval = 1; // seconds
    final int staleNodeInterval = 3; // seconds
    final int deadNodeInterval = 6; // seconds
    ScheduledFuture schedFuture;

    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        healthCheckInterval, MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL,
        heartbeatInterval, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        staleNodeInterval, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL,
        deadNodeInterval, SECONDS);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      DatanodeDetails node1 =
          HddsTestUtils.createRandomDatanodeAndRegister(nodeManager);
      DatanodeDetails node2 =
          HddsTestUtils.createRandomDatanodeAndRegister(nodeManager);

      nodeManager.processHeartbeat(node1);
      nodeManager.processHeartbeat(node2);

      // Sleep so that heartbeat processing thread gets to run.
      Thread.sleep(1000);

      //Assert all nodes are healthy.
      assertEquals(2, nodeManager.getAllNodes().size());
      assertEquals(2,
          nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));
      /**
       * Simulate a JVM Pause and subsequent handling in following steps:
       * Step 1 : stop heartbeat check process for stale node interval
       * Step 2 : resume heartbeat check
       * Step 3 : wait for 1 iteration of heartbeat check thread
       * Step 4 : retrieve the state of all nodes - assert all are HEALTHY
       * Step 5 : heartbeat for node1
       * [TODO : what if there is scheduling delay of test thread in Step 5?]
       * Step 6 : wait for some time to allow iterations of check process
       * Step 7 : retrieve the state of all nodes -  assert node2 is STALE
       * and node1 is HEALTHY
       */

      // Step 1 : stop health check process (simulate JVM pause)
      nodeManager.pauseHealthCheck();
      Thread.sleep(MILLISECONDS.convert(staleNodeInterval, SECONDS));

      // Step 2 : resume health check
      assertEquals(0, nodeManager.getSkippedHealthChecks(),
          "Unexpected, already skipped heartbeat checks");
      schedFuture = nodeManager.unpauseHealthCheck();

      // Step 3 : wait for 1 iteration of health check

      schedFuture.get();
      assertThat(nodeManager.getSkippedHealthChecks())
          .withFailMessage("We did not skip any heartbeat checks")
          .isGreaterThan(0);

      // Step 4 : all nodes should still be HEALTHY
      assertEquals(2, nodeManager.getAllNodes().size());
      assertEquals(2, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));

      // Step 5 : heartbeat for node1
      nodeManager.processHeartbeat(node1);

      // Step 6 : wait for health check process to run
      Thread.sleep(1000);

      // Step 7 : node2 should transition to STALE
      assertEquals(1, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));
      assertEquals(1, nodeManager.getNodeCount(NodeStatus.inServiceStale()));
    }
  }

  @ParameterizedTest
  @EnumSource(FinalizationCheckpoint.class)
  public void testProcessLayoutVersion(FinalizationCheckpoint checkpoint) throws IOException {
    LOG.info("Testing with SCM finalization checkpoint {}", checkpoint);
    testProcessLayoutVersionLowerMlv(checkpoint);
    testProcessLayoutVersionReportHigherMlv(checkpoint);
  }

  // Currently invoked by testProcessLayoutVersion.
  public void testProcessLayoutVersionReportHigherMlv(
      FinalizationCheckpoint currentCheckpoint)
      throws IOException {
    final int healthCheckInterval = 200; // milliseconds
    final int heartbeatInterval = 1; // seconds

    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        healthCheckInterval, MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL,
        heartbeatInterval, SECONDS);

    SCMStorageConfig scmStorageConfig = mock(SCMStorageConfig.class);
    when(scmStorageConfig.getClusterID()).thenReturn("xyz111");
    EventPublisher eventPublisher = mock(EventPublisher.class);
    HDDSLayoutVersionManager lvm  =
        new HDDSLayoutVersionManager(scmStorageConfig.getLayoutVersion());
    SCMContext nodeManagerContext = SCMContext.emptyContext();
    nodeManagerContext.setFinalizationCheckpoint(currentCheckpoint);
    SCMNodeManager nodeManager  = new SCMNodeManager(conf,
        scmStorageConfig, eventPublisher, new NetworkTopologyImpl(conf),
        nodeManagerContext, lvm);

    // Regardless of SCM's finalization checkpoint, datanodes with higher MLV
    // than SCM should not be found in the cluster.
    DatanodeDetails node1 =
        HddsTestUtils.createRandomDatanodeAndRegister(nodeManager);
    LogCapturer logCapturer = LogCapturer.captureLogs(SCMNodeManager.class);
    int scmMlv =
        nodeManager.getLayoutVersionManager().getMetadataLayoutVersion();
    int scmSlv =
        nodeManager.getLayoutVersionManager().getSoftwareLayoutVersion();
    nodeManager.processLayoutVersionReport(node1,
        LayoutVersionProto.newBuilder()
            .setMetadataLayoutVersion(scmMlv + 1)
            .setSoftwareLayoutVersion(scmSlv + 1)
            .build());
    assertThat(logCapturer.getOutput())
        .contains("Invalid data node in the cluster");
    nodeManager.close();
  }

  // Currently invoked by testProcessLayoutVersion.
  public void testProcessLayoutVersionLowerMlv(FinalizationCheckpoint
      currentCheckpoint) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    SCMStorageConfig scmStorageConfig = mock(SCMStorageConfig.class);
    when(scmStorageConfig.getClusterID()).thenReturn("xyz111");
    EventPublisher eventPublisher = mock(EventPublisher.class);
    HDDSLayoutVersionManager lvm  =
        new HDDSLayoutVersionManager(scmStorageConfig.getLayoutVersion());
    SCMContext nodeManagerContext = SCMContext.emptyContext();
    nodeManagerContext.setFinalizationCheckpoint(currentCheckpoint);
    SCMNodeManager nodeManager  = new SCMNodeManager(conf,
        scmStorageConfig, eventPublisher, new NetworkTopologyImpl(conf),
        nodeManagerContext, lvm);
    DatanodeDetails node1 =
        HddsTestUtils.createRandomDatanodeAndRegister(nodeManager);
    verify(eventPublisher,
        times(1)).fireEvent(NEW_NODE, node1);
    int scmMlv =
        nodeManager.getLayoutVersionManager().getMetadataLayoutVersion();
    nodeManager.processLayoutVersionReport(node1,
        LayoutVersionProto.newBuilder()
            .setMetadataLayoutVersion(scmMlv - 1)
            .setSoftwareLayoutVersion(scmMlv)
            .build());
    ArgumentCaptor<CommandForDatanode> captor =
        ArgumentCaptor.forClass(CommandForDatanode.class);

    if (currentCheckpoint.hasCrossed(FinalizationCheckpoint.MLV_EQUALS_SLV)) {
      // If the mlv equals slv checkpoint passed, datanodes with older mlvs
      // should be instructed to finalize.
      verify(eventPublisher, times(1))
          .fireEvent(eq(DATANODE_COMMAND), captor.capture());
      assertEquals(captor.getValue().getDatanodeId(), node1.getID());
      assertEquals(captor.getValue().getCommand().getType(),
          finalizeNewLayoutVersionCommand);
    } else {
      // SCM has not finished finalizing its mlv, so datanodes with older
      // mlvs should not be instructed to finalize yet.
      verify(eventPublisher, times(0))
          .fireEvent(eq(DATANODE_COMMAND), captor.capture());
    }
  }

  @Test
  public void testProcessCommandQueueReport()
      throws IOException, NodeNotFoundException, AuthenticationException {
    OzoneConfiguration conf = new OzoneConfiguration();
    SCMStorageConfig scmStorageConfig = mock(SCMStorageConfig.class);
    when(scmStorageConfig.getClusterID()).thenReturn("xyz111");
    EventPublisher eventPublisher = mock(EventPublisher.class);
    HDDSLayoutVersionManager lvm  =
        new HDDSLayoutVersionManager(scmStorageConfig.getLayoutVersion());
    createNodeManager(getConf());
    SCMNodeManager nodeManager  = new SCMNodeManager(conf,
        scmStorageConfig, eventPublisher, new NetworkTopologyImpl(conf),
        scmContext, lvm);

    DatanodeDetails node1 =
        HddsTestUtils.createRandomDatanodeAndRegister(nodeManager);
    verify(eventPublisher,
        times(1)).fireEvent(NEW_NODE, node1);
    for (int i = 0; i < 3; i++) {
      nodeManager.addDatanodeCommand(node1.getID(), ReplicateContainerCommand
          .toTarget(1, MockDatanodeDetails.randomDatanodeDetails()));
    }
    for (int i = 0; i < 5; i++) {
      nodeManager.addDatanodeCommand(node1.getID(),
          new DeleteBlocksCommand(emptyList()));
    }

    assertEquals(3, nodeManager.getTotalDatanodeCommandCount(
        node1, SCMCommandProto.Type.replicateContainerCommand));
    assertEquals(5, nodeManager.getTotalDatanodeCommandCount(
        node1, SCMCommandProto.Type.deleteBlocksCommand));

    nodeManager.processHeartbeat(node1,
        CommandQueueReportProto.newBuilder()
            .addCommand(SCMCommandProto.Type.replicateContainerCommand)
            .addCount(123)
            .addCommand(SCMCommandProto.Type.closeContainerCommand)
            .addCount(11)
            .build());
    assertEquals(-1, nodeManager.getNodeQueuedCommandCount(
        node1, SCMCommandProto.Type.closePipelineCommand));
    assertEquals(126, nodeManager.getNodeQueuedCommandCount(
        node1, SCMCommandProto.Type.replicateContainerCommand));
    assertEquals(11, nodeManager.getNodeQueuedCommandCount(
        node1, SCMCommandProto.Type.closeContainerCommand));
    assertEquals(5, nodeManager.getNodeQueuedCommandCount(
        node1, SCMCommandProto.Type.deleteBlocksCommand));

    assertEquals(126, nodeManager.getTotalDatanodeCommandCount(
        node1, SCMCommandProto.Type.replicateContainerCommand));
    assertEquals(5, nodeManager.getTotalDatanodeCommandCount(
        node1, SCMCommandProto.Type.deleteBlocksCommand));

    ArgumentCaptor<DatanodeDetails> captor =
        ArgumentCaptor.forClass(DatanodeDetails.class);
    verify(eventPublisher, times(1))
        .fireEvent(eq(DATANODE_COMMAND_COUNT_UPDATED),
            captor.capture());
    assertEquals(node1, captor.getValue());

    // Send another report missing an earlier entry, and ensure it is not
    // still reported as a stale value.
    nodeManager.processHeartbeat(node1,
        CommandQueueReportProto.newBuilder()
            .addCommand(SCMCommandProto.Type.closeContainerCommand)
            .addCount(11)
            .build());
    assertEquals(-1, nodeManager.getNodeQueuedCommandCount(
        node1, SCMCommandProto.Type.replicateContainerCommand));
    assertEquals(11, nodeManager.getNodeQueuedCommandCount(
        node1, SCMCommandProto.Type.closeContainerCommand));

    verify(eventPublisher, times(2))
        .fireEvent(eq(DATANODE_COMMAND_COUNT_UPDATED),
            captor.capture());
    assertEquals(node1, captor.getValue());

    // Add a a few more commands to the queue and check the counts are the sum.
    for (int i = 0; i < 5; i++) {
      nodeManager.addDatanodeCommand(node1.getID(),
          new CloseContainerCommand(1, PipelineID.randomId()));
    }
    assertEquals(0, nodeManager.getTotalDatanodeCommandCount(
        node1, SCMCommandProto.Type.replicateContainerCommand));
    assertEquals(16, nodeManager.getTotalDatanodeCommandCount(
        node1, SCMCommandProto.Type.closeContainerCommand));
    Map<SCMCommandProto.Type, Integer> counts =
        nodeManager.getTotalDatanodeCommandCounts(node1,
            SCMCommandProto.Type.replicateContainerCommand,
            SCMCommandProto.Type.closeContainerCommand);
    assertEquals(0,
        counts.get(SCMCommandProto.Type.replicateContainerCommand));
    assertEquals(16,
        counts.get(SCMCommandProto.Type.closeContainerCommand));
  }

  @Test
  public void testCommandCount()
      throws AuthenticationException, IOException {
    SCMNodeManager nodeManager = createNodeManager(getConf());

    DatanodeID datanode1 = DatanodeID.randomID();
    DatanodeID datanode2 = DatanodeID.randomID();
    long containerID = 1;

    SCMCommand<?> closeContainerCommand =
        new CloseContainerCommand(containerID, PipelineID.randomId());
    SCMCommand<?> createPipelineCommand =
        new CreatePipelineCommand(PipelineID.randomId(),
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, emptyList());

    nodeManager.onMessage(
        new CommandForDatanode<>(datanode1, closeContainerCommand), null);
    nodeManager.onMessage(
        new CommandForDatanode<>(datanode1, closeContainerCommand), null);
    nodeManager.onMessage(
        new CommandForDatanode<>(datanode1, createPipelineCommand), null);

    assertEquals(2, nodeManager.getCommandQueueCount(
        datanode1, SCMCommandProto.Type.closeContainerCommand));
    assertEquals(1, nodeManager.getCommandQueueCount(
        datanode1, SCMCommandProto.Type.createPipelineCommand));
    assertEquals(0, nodeManager.getCommandQueueCount(
        datanode1, SCMCommandProto.Type.closePipelineCommand));

    assertEquals(0, nodeManager.getCommandQueueCount(
        datanode2, SCMCommandProto.Type.closeContainerCommand));
  }

  /**
   * Check for NPE when datanodeDetails is passed null for sendHeartbeat.
   *
   * @throws IOException
   */
  @Test
  public void testScmCheckForErrorOnNullDatanodeDetails() throws IOException, AuthenticationException {
    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {
      NullPointerException npe = assertThrows(NullPointerException.class,
          () -> nodeManager.processHeartbeat(null));
      assertThat(npe).hasMessage("Heartbeat is missing DatanodeDetails.");
    }
  }

  /**
   * Asserts that a dead node, stale node and healthy nodes co-exist. The counts
   * , lists and node ID match the expected node state.
   * <p/>
   * This test is pretty complicated because it explores all states of Node
   * manager in a single test. Please read thru the comments to get an idea of
   * the current state of the node Manager.
   * <p/>
   * This test is written like a state machine to avoid threads and concurrency
   * issues. This test is replicated below with the use of threads. Avoiding
   * threads make it easy to debug the state machine.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  /**
   * These values are very important. Here is what it means so you don't
   * have to look it up while reading this code.
   *
   *  OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL - This the frequency of the
   *  HB processing thread that is running in the SCM. This thread must run
   *  for the SCM  to process the Heartbeats.
   *
   *  OZONE_SCM_HEARTBEAT_INTERVAL - This is the frequency at which
   *  datanodes will send heartbeats to SCM. Please note: This is the only
   *  config value for node manager that is specified in seconds. We don't
   *  want SCM heartbeat resolution to be more than in seconds.
   *  In this test it is not used, but we are forced to set it because we
   *  have validation code that checks Stale Node interval and Dead Node
   *  interval is larger than the value of
   *  OZONE_SCM_HEARTBEAT_INTERVAL.
   *
   *  OZONE_SCM_STALENODE_INTERVAL - This is the time that must elapse
   *  from the last heartbeat for us to mark a node as stale. In this test
   *  we set that to 3. That is if a node has not heartbeat SCM for last 3
   *  seconds we will mark it as stale.
   *
   *  OZONE_SCM_DEADNODE_INTERVAL - This is the time that must elapse
   *  from the last heartbeat for a node to be marked dead. We have an
   *  additional constraint that this must be at least 2 times bigger than
   *  Stale node Interval.
   *
   *  With these we are trying to explore the state of this cluster with
   *  various timeouts. Each section is commented so that you can keep
   *  track of the state of the cluster nodes.
   *
   */

  @Test
  public void testScmClusterIsInExpectedState1()
      throws IOException, InterruptedException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);


    /**
     * Cluster state: Healthy: All nodes are heartbeat-ing like normal.
     */
    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      DatanodeDetails healthyNode =
          HddsTestUtils.createRandomDatanodeAndRegister(nodeManager);
      DatanodeDetails staleNode =
          HddsTestUtils.createRandomDatanodeAndRegister(nodeManager);
      DatanodeDetails deadNode =
          HddsTestUtils.createRandomDatanodeAndRegister(nodeManager);
      nodeManager.processHeartbeat(healthyNode);
      nodeManager.processHeartbeat(staleNode);
      nodeManager.processHeartbeat(deadNode);

      // Sleep so that heartbeat processing thread gets to run.
      Thread.sleep(500);

      //Assert all nodes are healthy.
      assertEquals(3, nodeManager.getAllNodes().size());
      assertEquals(3, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));

      /**
       * Cluster state: Quiesced: We are going to sleep for 3 seconds. Which
       * means that no node is heartbeating. All nodes should move to Stale.
       */
      Thread.sleep(3 * 1000);
      assertEquals(3, nodeManager.getAllNodes().size());
      assertEquals(3, nodeManager.getNodeCount(NodeStatus.inServiceStale()));


      /**
       * Cluster State : Move healthy node back to healthy state, move other 2
       * nodes to Stale State.
       *
       * We heartbeat healthy node after 1 second and let other 2 nodes elapse
       * the 3 second windows.
       */

      nodeManager.processHeartbeat(healthyNode);
      nodeManager.processHeartbeat(staleNode);
      nodeManager.processHeartbeat(deadNode);

      Thread.sleep(1500);
      nodeManager.processHeartbeat(healthyNode);
      Thread.sleep(2 * 1000);
      assertEquals(1, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));


      // 3.5 seconds from last heartbeat for the stale and deadNode. So those
      //  2 nodes must move to Stale state and the healthy node must
      // remain in the healthy State.
      List<DatanodeDetails> healthyList = nodeManager.getNodes(
          NodeStatus.inServiceHealthy());
      assertEquals(1, healthyList.size(), "Expected one healthy node");
      assertEquals(healthyNode.getID(), healthyList.get(0).getID(),
          "Healthy node is not the expected ID");

      assertEquals(2, nodeManager.getNodeCount(NodeStatus.inServiceStale()));

      /**
       * Cluster State: Allow healthyNode to remain in healthy state and
       * staleNode to move to stale state and deadNode to move to dead state.
       */

      nodeManager.processHeartbeat(healthyNode);
      nodeManager.processHeartbeat(staleNode);
      Thread.sleep(1500);
      nodeManager.processHeartbeat(healthyNode);
      Thread.sleep(2 * 1000);

      // 3.5 seconds have elapsed for stale node, so it moves into Stale.
      // 7 seconds have elapsed for dead node, so it moves into dead.
      // 2 Seconds have elapsed for healthy node, so it stays in healthy state.
      healthyList = nodeManager.getNodes((NodeStatus.inServiceHealthy()));
      List<DatanodeDetails> staleList =
          nodeManager.getNodes(NodeStatus.inServiceStale());
      List<DatanodeDetails> deadList =
          nodeManager.getNodes(NodeStatus.inServiceDead());

      assertEquals(3, nodeManager.getAllNodes().size());
      assertEquals(1, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));
      assertEquals(1, nodeManager.getNodeCount(NodeStatus.inServiceStale()));
      assertEquals(1, nodeManager.getNodeCount(NodeStatus.inServiceDead()));

      assertEquals(1, healthyList.size(), "Expected one healthy node");
      assertEquals(healthyNode.getID(), healthyList.get(0).getID(),
          "Healthy node is not the expected ID");

      assertEquals(1, staleList.size(), "Expected one stale node");
      assertEquals(staleNode.getID(), staleList.get(0).getID(),
          "Stale node is not the expected ID");

      assertEquals(1, deadList.size(), "Expected one dead node");
      assertEquals(deadNode.getID(), deadList.get(0).getID(),
          "Dead node is not the expected ID");
      /**
       * Cluster State : let us heartbeat all the nodes and verify that we get
       * back all the nodes in healthy state.
       */
      nodeManager.processHeartbeat(healthyNode);
      nodeManager.processHeartbeat(staleNode);
      nodeManager.processHeartbeat(deadNode);
      Thread.sleep(500);
      //Assert all nodes are healthy.
      assertEquals(3, nodeManager.getAllNodes().size());
      assertEquals(3, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));
    }
  }

  /**
   * Heartbeat a given set of nodes at a specified frequency.
   *
   * @param manager       - Node Manager
   * @param list          - List of datanodeIDs
   * @param sleepDuration - Duration to sleep between heartbeats.
   * @throws InterruptedException
   */
  private void heartbeatNodeSet(SCMNodeManager manager,
                                List<DatanodeDetails> list,
                                int sleepDuration) throws InterruptedException {
    while (!Thread.currentThread().isInterrupted()) {
      for (DatanodeDetails dn : list) {
        manager.processHeartbeat(dn);
      }
      Thread.sleep(sleepDuration);
    }
  }

  /**
   * Create a set of Nodes with a given prefix.
   *
   * @param count  - number of nodes.
   * @return List of Nodes.
   */
  private List<DatanodeDetails> createNodeSet(SCMNodeManager nodeManager, int
      count) {
    List<DatanodeDetails> list = new ArrayList<>();
    for (int x = 0; x < count; x++) {
      DatanodeDetails datanodeDetails = HddsTestUtils
          .createRandomDatanodeAndRegister(nodeManager);
      list.add(datanodeDetails);
    }
    return list;
  }

  /**
   * Function that tells us if we found the right number of stale nodes.
   *
   * @param nodeManager - node manager
   * @param count       - number of stale nodes to look for.
   * @return true if we found the expected number.
   */
  private boolean findNodes(NodeManager nodeManager, int count,
                            HddsProtos.NodeState state) {
    return count == nodeManager.getNodeCount(NodeStatus.inServiceStale());
  }

  /**
   * Asserts that we can create a set of nodes that send its heartbeats from
   * different threads and NodeManager behaves as expected.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testScmClusterIsInExpectedState2()
      throws IOException, InterruptedException, TimeoutException,
      AuthenticationException {
    final int healthyCount = 5000;
    final int staleCount = 100;
    final int deadCount = 10;

    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);


    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      List<DatanodeDetails> healthyNodeList = createNodeSet(nodeManager,
          healthyCount);
      List<DatanodeDetails> staleNodeList = createNodeSet(nodeManager,
          staleCount);
      List<DatanodeDetails> deadNodeList = createNodeSet(nodeManager,
          deadCount);

      Runnable healthyNodeTask = () -> {
        try {
          // 2 second heartbeat makes these nodes stay healthy.
          heartbeatNodeSet(nodeManager, healthyNodeList, 2 * 1000);
        } catch (InterruptedException ignored) {
        }
      };

      Runnable staleNodeTask = () -> {
        try {
          // 4 second heartbeat makes these nodes go to stale and back to
          // healthy again.
          heartbeatNodeSet(nodeManager, staleNodeList, 4 * 1000);
        } catch (InterruptedException ignored) {
        }
      };

      // No Thread just one time HBs the node manager, so that these will be
      // marked as dead nodes eventually.
      for (DatanodeDetails dn : deadNodeList) {
        nodeManager.processHeartbeat(dn);
      }


      Thread thread1 = new Thread(healthyNodeTask);
      thread1.setDaemon(true);
      thread1.start();


      Thread thread2 = new Thread(staleNodeTask);
      thread2.setDaemon(true);
      thread2.start();

      Thread.sleep(10 * 1000);

      // Assert all healthy nodes are healthy now, this has to be a greater
      // than check since Stale nodes can be healthy when we check the state.

      assertThat(nodeManager.getNodeCount(NodeStatus.inServiceHealthy()))
          .isGreaterThanOrEqualTo(healthyCount);

      assertEquals(deadCount,
          nodeManager.getNodeCount(NodeStatus.inServiceDead()));

      List<DatanodeDetails> deadList =
          nodeManager.getNodes(NodeStatus.inServiceDead());

      for (DatanodeDetails node : deadList) {
        assertThat(deadNodeList).contains(node);
      }



      // Checking stale nodes is tricky since they have to move between
      // healthy and stale to avoid becoming dead nodes. So we search for
      // that state for a while, if we don't find that state waitfor will
      // throw.
      GenericTestUtils.waitFor(() -> findNodes(nodeManager, staleCount, STALE),
          500, 4 * 1000);

      thread1.interrupt();
      thread2.interrupt();
    }
  }

  /**
   * Asserts that we can handle 6000+ nodes heartbeating SCM.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmCanHandleScale()
      throws IOException, InterruptedException, TimeoutException,
      AuthenticationException {
    final int healthyCount = 3000;
    final int staleCount = 3000;
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1,
        SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3 * 1000,
        MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6 * 1000,
        MILLISECONDS);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      List<DatanodeDetails> healthyList = createNodeSet(nodeManager,
          healthyCount);
      List<DatanodeDetails> staleList = createNodeSet(nodeManager,
          staleCount);

      Runnable healthyNodeTask = () -> {
        try {
          heartbeatNodeSet(nodeManager, healthyList, 2 * 1000);
        } catch (InterruptedException ignored) {

        }
      };

      Runnable staleNodeTask = () -> {
        try {
          heartbeatNodeSet(nodeManager, staleList, 4 * 1000);
        } catch (InterruptedException ignored) {
        }
      };

      Thread thread1 = new Thread(healthyNodeTask);
      thread1.setDaemon(true);
      thread1.start();

      Thread thread2 = new Thread(staleNodeTask);
      thread2.setDaemon(true);
      thread2.start();
      Thread.sleep(3 * 1000);

      GenericTestUtils.waitFor(() -> findNodes(nodeManager, staleCount, STALE),
          500, 20 * 1000);
      assertEquals(healthyCount + staleCount,
          nodeManager.getAllNodes().size(), "Node count mismatch");

      thread1.interrupt();
      thread2.interrupt();
    }
  }

  /**
   * Test multiple nodes sending initial heartbeat with their node report.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmStatsFromNodeReport()
      throws IOException, InterruptedException, AuthenticationException, NodeNotFoundException {
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 1000,
        MILLISECONDS);
    final int nodeCount = 10;
    final long capacity = 2000;
    final long used = 100;
    final long remaining = capacity - used;
    List<DatanodeDetails> dnList = new ArrayList<>(nodeCount);
    try (SCMNodeManager nodeManager = createNodeManager(conf)) {

      EventQueue eventQueue = (EventQueue) scm.getEventQueue();
      for (int x = 0; x < nodeCount; x++) {
        DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
        dnList.add(dn);
        DatanodeID dnId = dn.getID();
        long free = capacity - used;
        String storagePath = testDir.getAbsolutePath() + "/" + dnId;
        StorageReportProto report = HddsTestUtils
            .createStorageReport(dnId, storagePath, capacity, used, free, null);
        nodeManager.register(dn, HddsTestUtils.createNodeReport(
            Arrays.asList(report), emptyList()), null);
        nodeManager.processHeartbeat(dn);
      }
      //TODO: wait for EventQueue to be processed
      eventQueue.processAll(8000L);

      assertEquals(nodeCount, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));
      assertEquals(capacity * nodeCount, (long) nodeManager.getStats().getCapacity().get());
      assertEquals(used * nodeCount, (long) nodeManager.getStats().getScmUsed().get());
      assertEquals(remaining * nodeCount, (long) nodeManager.getStats().getRemaining().get());
      for (DatanodeDetails dn : dnList) {
        assertEquals(1,
            nodeManager.getNodeStateManager().getNode(dn).getHealthyVolumeCount());
      }
      dnList.clear();
    }
  }

  private List<StorageReportProto> generateStorageReportProto(
      int volumeCount, DatanodeID dnId, long capacity, long used, long remaining) {
    List<StorageReportProto> reports = new ArrayList<>(volumeCount);
    boolean failed = true;
    for (int x = 0; x < volumeCount; x++) {
      String storagePath = testDir.getAbsolutePath() + "/" + dnId;
      reports.add(HddsTestUtils
          .createStorageReport(dnId, storagePath, capacity,
              used, remaining, null, failed));
      failed = !failed;
    }
    return reports;
  }

  private static Stream<Arguments> calculateStoragePercentageScenarios() {
    return Stream.of(
        Arguments.of(600, 65, 500, 1, "600.0B", "10.83", "5.83"),
        Arguments.of(10000, 1000, 8800, 12, "117.2KB", "10.00", "2.00"),
        Arguments.of(100000000, 1000, 899999, 12, "1.1GB", "0.00", "99.10"),
        Arguments.of(10000, 1000, 0, 0, "0.0B", "N/A", "N/A"),
        Arguments.of(0, 0, 0, 0, "0.0B", "N/A", "N/A"),
        Arguments.of(1010, 547, 400, 5, "4.9KB", "54.16", "6.24")
    );
  }

  @ParameterizedTest
  @MethodSource("calculateStoragePercentageScenarios")
  public void testCalculateStoragePercentage(long perCapacity,
      long used, long remaining, int volumeCount, String totalCapacity,
          String scmUsedPerc, String nonScmUsedPerc) {
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeID dnId = dn.getID();
    List<StorageReportProto> reports = volumeCount > 0 ?
        generateStorageReportProto(volumeCount, dnId, perCapacity,
            used, remaining) : null;
    String capacityResult = SCMNodeManager.calculateStorageCapacity(reports);
    assertEquals(totalCapacity, capacityResult);
    String[] storagePercentage = SCMNodeManager.calculateStoragePercentage(
        reports);
    assertEquals(scmUsedPerc, storagePercentage[0]);
    assertEquals(nonScmUsedPerc, storagePercentage[1]);
  }

  /**
   * Test multiple nodes sending initial heartbeat with their node report
   * with multiple volumes.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void tesVolumeInfoFromNodeReport()
      throws IOException, InterruptedException, AuthenticationException, NodeNotFoundException {
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 1000,
        MILLISECONDS);
    final int volumeCount = 10;
    final long capacity = 2000;
    final long used = 100;
    List<DatanodeDetails> dnList = new ArrayList<>(1);
    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      EventQueue eventQueue = (EventQueue) scm.getEventQueue();
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dnList.add(dn);
      DatanodeID dnId = dn.getID();
      long free = capacity - used;
      List<StorageReportProto> reports = new ArrayList<>(volumeCount);
      boolean failed = true;
      for (int x = 0; x < volumeCount; x++) {
        String storagePath = testDir.getAbsolutePath() + "/" + dnId;
        reports.add(HddsTestUtils
            .createStorageReport(dnId, storagePath, capacity,
                used, free, null, failed));
        failed = !failed;
      }
      nodeManager.register(dn, HddsTestUtils.createNodeReport(reports,
          emptyList()), null);
      nodeManager.processHeartbeat(dn);
      //TODO: wait for EventQueue to be processed
      eventQueue.processAll(8000L);

      assertEquals(1, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));
      assertEquals(volumeCount / 2,
          nodeManager.getNodeStateManager().getNode(dn).getHealthyVolumeCount());
      dnList.clear();
    }
  }


  /**
   * Test single node stat update based on nodereport from different heartbeat
   * status (healthy, stale and dead).
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmNodeReportUpdate()
      throws IOException, InterruptedException, TimeoutException,
      AuthenticationException {
    OzoneConfiguration conf = getConf();
    final int heartbeatCount = 5;
    final int nodeCount = 1;
    final int interval = 100;

    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
        MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      DatanodeDetails datanodeDetails =
          HddsTestUtils.createRandomDatanodeAndRegister(nodeManager);
      NodeReportHandler nodeReportHandler = new NodeReportHandler(nodeManager);
      EventPublisher publisher = mock(EventPublisher.class);
      final long capacity = 2000;
      final long usedPerHeartbeat = 100;
      DatanodeID dnId = datanodeDetails.getID();
      for (int x = 0; x < heartbeatCount; x++) {
        long scmUsed = x * usedPerHeartbeat;
        long remaining = capacity - scmUsed;
        String storagePath = testDir.getAbsolutePath() + "/" + dnId;
        StorageReportProto report = HddsTestUtils
            .createStorageReport(dnId, storagePath, capacity, scmUsed,
                remaining, null);
        NodeReportProto nodeReportProto = HddsTestUtils.createNodeReport(
            Arrays.asList(report), emptyList());
        nodeReportHandler.onMessage(
            new NodeReportFromDatanode(datanodeDetails, nodeReportProto),
            publisher);
        nodeManager.processHeartbeat(datanodeDetails);
        Thread.sleep(100);
      }

      long expectedScmUsed = usedPerHeartbeat * (heartbeatCount - 1);
      long expectedRemaining = capacity - expectedScmUsed;

      GenericTestUtils.waitFor(
          () -> nodeManager.getStats().getScmUsed().get() == expectedScmUsed,
          100, 4 * 1000);

      long foundCapacity = nodeManager.getStats().getCapacity().get();
      assertEquals(capacity, foundCapacity);

      long foundScmUsed = nodeManager.getStats().getScmUsed().get();
      assertEquals(expectedScmUsed, foundScmUsed);

      long foundRemaining = nodeManager.getStats().getRemaining().get();
      assertEquals(expectedRemaining, foundRemaining);

      // Test NodeManager#getNodeStats
      assertEquals(nodeCount, nodeManager.getNodeStats().size());
      long nodeCapacity = nodeManager.getNodeStat(datanodeDetails).get()
          .getCapacity().get();
      assertEquals(capacity, nodeCapacity);

      foundScmUsed = nodeManager.getNodeStat(datanodeDetails).get().getScmUsed()
          .get();
      assertEquals(expectedScmUsed, foundScmUsed);

      foundRemaining = nodeManager.getNodeStat(datanodeDetails).get()
          .getRemaining().get();
      assertEquals(expectedRemaining, foundRemaining);

      // Compare the result from
      // NodeManager#getNodeStats and NodeManager#getNodeStat
      SCMNodeStat stat1 = nodeManager.getNodeStats().
          get(datanodeDetails);
      SCMNodeStat stat2 = nodeManager.getNodeStat(datanodeDetails).get();
      assertEquals(stat1, stat2);

      // Wait up to 4s so that the node becomes stale
      // Verify the usage info should be unchanged.
      GenericTestUtils.waitFor(
          () -> nodeManager.getNodeCount(NodeStatus.inServiceStale()) == 1, 100,
          4 * 1000);
      assertEquals(nodeCount, nodeManager.getNodeStats().size());

      foundCapacity = nodeManager.getNodeStat(datanodeDetails).get()
          .getCapacity().get();
      assertEquals(capacity, foundCapacity);
      foundScmUsed = nodeManager.getNodeStat(datanodeDetails).get()
          .getScmUsed().get();
      assertEquals(expectedScmUsed, foundScmUsed);

      foundRemaining = nodeManager.getNodeStat(datanodeDetails).get().
          getRemaining().get();
      assertEquals(expectedRemaining, foundRemaining);

      // Wait up to 4 more seconds so the node becomes dead
      // Verify usage info should be updated.
      GenericTestUtils.waitFor(
          () -> nodeManager.getNodeCount(NodeStatus.inServiceDead()) == 1, 100,
          4 * 1000);

      assertEquals(0, nodeManager.getNodeStats().size());
      foundCapacity = nodeManager.getStats().getCapacity().get();
      assertEquals(0, foundCapacity);

      foundScmUsed = nodeManager.getStats().getScmUsed().get();
      assertEquals(0, foundScmUsed);

      foundRemaining = nodeManager.getStats().getRemaining().get();
      assertEquals(0, foundRemaining);

      nodeManager.processHeartbeat(datanodeDetails);

      // Wait up to 5 seconds so that the dead node becomes healthy
      // Verify usage info should be updated.
      GenericTestUtils.waitFor(
          () -> nodeManager.getNodeCount(NodeStatus.inServiceHealthy()) == 1,
          100, 5 * 1000);
      GenericTestUtils.waitFor(
          () -> nodeManager.getStats().getScmUsed().get() == expectedScmUsed,
          100, 4 * 1000);
      assertEquals(nodeCount, nodeManager.getNodeStats().size());
      foundCapacity = nodeManager.getNodeStat(datanodeDetails).get()
          .getCapacity().get();
      assertEquals(capacity, foundCapacity);
      foundScmUsed = nodeManager.getNodeStat(datanodeDetails).get().getScmUsed()
          .get();
      assertEquals(expectedScmUsed, foundScmUsed);
      foundRemaining = nodeManager.getNodeStat(datanodeDetails).get()
          .getRemaining().get();
      assertEquals(expectedRemaining, foundRemaining);
    }
  }

  @Test
  public void testHandlingSCMCommandEvent()
      throws IOException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    conf.getTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);

    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    DatanodeID dnId = datanodeDetails.getID();
    String storagePath = testDir.getAbsolutePath() + "/" + dnId;
    StorageReportProto report =
        HddsTestUtils.createStorageReport(dnId, storagePath, 100, 10, 90, null);

    EventQueue eq = new EventQueue();
    try (SCMNodeManager nodemanager = createNodeManager(conf)) {
      eq.addHandler(DATANODE_COMMAND, nodemanager);

      nodemanager
          .register(datanodeDetails, HddsTestUtils.createNodeReport(
              Arrays.asList(report), emptyList()),
                  HddsTestUtils.getRandomPipelineReports());
      eq.fireEvent(DATANODE_COMMAND,
          new CommandForDatanode<>(datanodeDetails,
              new CloseContainerCommand(1L,
                  PipelineID.randomId())));

      eq.processAll(1000L);
      List<SCMCommand<?>> command =
          nodemanager.processHeartbeat(datanodeDetails);
      // With dh registered, SCM will send create pipeline command to dn
      assertThat(command.size()).isGreaterThanOrEqualTo(1);
      assertTrue(command.get(0).getClass().equals(
          CloseContainerCommand.class) ||
          command.get(1).getClass().equals(CloseContainerCommand.class));
    } catch (IOException e) {
      e.printStackTrace();
      throw  e;
    }
  }

  /**
   * Test add node into a 4-layer network topology during node register.
   */
  @Test
  public void testScmRegisterNodeWith4LayerNetworkTopology()
      throws IOException, InterruptedException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 1000,
        MILLISECONDS);

    // create table mapping file
    String[] hostNames = {"host1", "host2", "host3", "host4"};
    String[] ipAddress = {"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"};
    String mapFile = this.getClass().getClassLoader()
        .getResource("nodegroup-mapping").getPath();

    // create and register nodes
    conf.set(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        "org.apache.hadoop.net.TableMapping");
    conf.set(NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, mapFile);
    conf.set(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE,
        "network-topology-nodegroup.xml");
    final int nodeCount = hostNames.length;
    // use default IP address to resolve node
    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      for (int i = 0; i < nodeCount; i++) {
        DatanodeDetails node = createDatanodeDetails(
            DatanodeID.randomID(), hostNames[i], ipAddress[i], null);
        nodeManager.register(node, null, null);
      }

      // verify network topology cluster has all the registered nodes
      Thread.sleep(4 * 1000);
      NetworkTopology clusterMap = scm.getClusterMap();
      assertEquals(nodeCount, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));
      assertEquals(nodeCount, clusterMap.getNumOfLeafNode(""));
      assertEquals(4, clusterMap.getMaxLevel());
      final List<DatanodeInfo> nodeList = nodeManager.getAllNodes();
      nodeList.forEach(node -> assertTrue(
          node.getNetworkLocation().startsWith("/rack1/ng")));
      nodeList.forEach(node -> assertNotNull(node.getParent()));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testScmRegisterNodeWithNetworkTopology(boolean useHostname)
      throws IOException, InterruptedException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 1000,
        MILLISECONDS);
    conf.setBoolean(HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME,
        useHostname);

    // create table mapping file
    String[] hostNames = {"host1", "host2", "host3", "host4"};
    String[] ipAddress = {"1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"};
    String mapFile = this.getClass().getClassLoader()
        .getResource("rack-mapping").getPath();

    // create and register nodes
    conf.set(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        "org.apache.hadoop.net.TableMapping");
    conf.set(NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, mapFile);

    final int nodeCount = hostNames.length;
    // use default IP address to resolve node
    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      for (int i = 0; i < nodeCount; i++) {
        DatanodeDetails node = createDatanodeDetails(
            DatanodeID.randomID(), hostNames[i], ipAddress[i], null);
        nodeManager.register(node, null, null);
      }

      // verify network topology cluster has all the registered nodes
      Thread.sleep(4 * 1000);
      NetworkTopology clusterMap = scm.getClusterMap();
      assertEquals(nodeCount,
          nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));
      assertEquals(nodeCount, clusterMap.getNumOfLeafNode(""));
      assertEquals(3, clusterMap.getMaxLevel());
      final List<DatanodeInfo> nodeList = nodeManager.getAllNodes();
      nodeList.forEach(node ->
          assertEquals("/rack1", node.getNetworkLocation()));

      // test get node
      Arrays.stream(hostNames).forEach(hostname -> assertNotEquals(0,
          nodeManager.getNodesByAddress(hostname).size()));

      Arrays.stream(ipAddress).forEach(ip -> assertNotEquals(0,
          nodeManager.getNodesByAddress(ip).size()));
    }
  }

  @Test
  public void testGetNodeInfo()
      throws IOException, InterruptedException, NodeNotFoundException,
      AuthenticationException {
    OzoneConfiguration conf = getConf();
    final int nodeCount = 6;
    SCMNodeManager nodeManager = createNodeManager(conf);

    for (int i = 0; i < nodeCount; i++) {
      DatanodeDetails datanodeDetails =
          MockDatanodeDetails.randomDatanodeDetails();
      final long capacity = 2000;
      final long used = 100;
      final long remaining = 1900;
      DatanodeID dnId = datanodeDetails.getID();
      String storagePath = testDir.getAbsolutePath() + "/" + dnId;
      StorageReportProto report = HddsTestUtils
          .createStorageReport(dnId, storagePath, capacity, used,
              remaining, null)
          .toBuilder()
          .setFsCapacity(3000L)
          .setFsAvailable(2400L)
          .build();

      nodeManager.register(datanodeDetails, HddsTestUtils.createNodeReport(
          Arrays.asList(report), emptyList()),
          HddsTestUtils.getRandomPipelineReports());

      nodeManager.register(datanodeDetails,
          HddsTestUtils.createNodeReport(Arrays.asList(report),
              emptyList()),
          HddsTestUtils.getRandomPipelineReports());
      nodeManager.processHeartbeat(datanodeDetails);
      if (i == 5) {
        nodeManager.setNodeOperationalState(datanodeDetails,
            HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE);
      }
      if (i == 3 || i == 4) {
        nodeManager.setNodeOperationalState(datanodeDetails,
            HddsProtos.NodeOperationalState.DECOMMISSIONED);
      }
    }
    Thread.sleep(100);

    Map<String, Long> stats = nodeManager.getNodeInfo();
    // 3 IN_SERVICE nodes:
    assertEquals(6000, stats.get("DiskCapacity").longValue());
    assertEquals(300, stats.get("DiskUsed").longValue());
    assertEquals(5700, stats.get("DiskRemaining").longValue());

    // 2 Decommissioned nodes
    assertEquals(4000, stats.get("DecommissionedDiskCapacity").longValue());
    assertEquals(200, stats.get("DecommissionedDiskUsed").longValue());
    assertEquals(3800, stats.get("DecommissionedDiskRemaining").longValue());

    // 1 Maintenance node
    assertEquals(2000, stats.get("MaintenanceDiskCapacity").longValue());
    assertEquals(100, stats.get("MaintenanceDiskUsed").longValue());
    assertEquals(1900, stats.get("MaintenanceDiskRemaining").longValue());

    // All nodes
    assertEquals(12000, stats.get("TotalOzoneCapacity").longValue());
    assertEquals(600, stats.get("TotalOzoneUsed").longValue());
    assertEquals(18000, stats.get("TotalFilesystemCapacity").longValue());
    assertEquals(14400, stats.get("TotalFilesystemAvailable").longValue());
    assertEquals(3600, stats.get("TotalFilesystemUsed").longValue());
  }

  @Test
  public void testGetTotalFilesystemUsage()
      throws IOException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      DatanodeDetails datanodeDetails = MockDatanodeDetails.randomDatanodeDetails();
      DatanodeID dnId = datanodeDetails.getID();

      StorageReportProto report1 = HddsTestUtils
          .createStorageReport(dnId, testDir.getAbsolutePath() + "/vol-1",
              500L, 100L, 400L, null)
          .toBuilder()
          .setFsCapacity(1000L)
          .setFsAvailable(600L)
          .build();
      StorageReportProto report2 = HddsTestUtils
          .createStorageReport(dnId, testDir.getAbsolutePath() + "/vol-2",
              700L, 200L, 500L, null)
          .toBuilder()
          .setFsCapacity(2000L)
          .setFsAvailable(1500L)
          .build();

      nodeManager.register(datanodeDetails,
          HddsTestUtils.createNodeReport(Arrays.asList(report1, report2), emptyList()), null);
      nodeManager.processHeartbeat(datanodeDetails);

      SpaceUsageSource.Fixed totals = nodeManager.getTotalFilesystemUsage(datanodeDetails);
      assertNotNull(totals);
      assertEquals(3000L, totals.getCapacity());
      assertEquals(2100L, totals.getAvailable());
      assertEquals(900L, totals.getUsedSpace());
    }
  }

  /**
   * Test add node into a 4-layer network topology during node register.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetNodesByAddress(boolean useHostname)
      throws IOException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 1000,
        MILLISECONDS);
    conf.setBoolean(HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME,
        useHostname);

    // create a set of hosts - note two hosts on "host1"
    String[] hostNames = {"host1", "host1", "host2", "host3", "host4"};
    String[] ipAddress =
        {"1.2.3.4", "1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7"};

    final int nodeCount = hostNames.length;
    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      for (int i = 0; i < nodeCount; i++) {
        DatanodeDetails node = createDatanodeDetails(
            DatanodeID.randomID(), hostNames[i], ipAddress[i], null);
        nodeManager.register(node, null, null);
      }
      // test get node
      assertEquals(0, nodeManager.getNodesByAddress(null).size());

      assertEquals(2, nodeManager.getNodesByAddress("host1").size());
      assertEquals(1, nodeManager.getNodesByAddress("host2").size());
      assertEquals(0, nodeManager.getNodesByAddress("unknown").size());

      assertEquals(2, nodeManager.getNodesByAddress("1.2.3.4").size());
      assertEquals(1, nodeManager.getNodesByAddress("2.3.4.5").size());
      assertEquals(0, nodeManager.getNodesByAddress("1.9.8.7").size());
    }
  }

  /**
   * Test node register with updated IP and host name.
   */
  @Test
  public void testScmRegisterNodeWithUpdatedIpAndHostname()
          throws IOException, InterruptedException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 1000,
            MILLISECONDS);

    // create table mapping file
    String hostName = "host1";
    String ipAddress = "1.2.3.4";
    String mapFile = this.getClass().getClassLoader()
            .getResource("nodegroup-mapping").getPath();
    conf.set(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            "org.apache.hadoop.net.TableMapping");
    conf.set(NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, mapFile);
    conf.set(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE,
            "network-topology-nodegroup.xml");

    // use default IP address to resolve node
    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      DatanodeID nodeId = DatanodeID.randomID();
      DatanodeDetails node = createDatanodeDetails(
          nodeId, hostName, ipAddress, null);
      nodeManager.register(node, null, null);

      // verify network topology cluster has all the registered nodes
      Thread.sleep(2 * 1000);
      NetworkTopology clusterMap = scm.getClusterMap();
      assertEquals(1,
              nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));
      assertEquals(1, clusterMap.getNumOfLeafNode(""));
      assertEquals(4, clusterMap.getMaxLevel());
      final List<DatanodeInfo> nodeList = nodeManager.getAllNodes();
      assertEquals(1, nodeList.size());

      DatanodeDetails returnedNode = nodeList.get(0);
      assertEquals(ipAddress, returnedNode.getIpAddress());
      assertEquals(hostName, returnedNode.getHostName());
      assertTrue(returnedNode.getNetworkLocation()
              .startsWith("/rack1/ng"));
      assertNotNull(returnedNode.getParent());

      // test updating ip address and host name
      String updatedIpAddress = "2.3.4.5";
      String updatedHostName = "host2";
      DatanodeDetails updatedNode = createDatanodeDetails(
          nodeId, updatedHostName, updatedIpAddress, null);
      nodeManager.register(updatedNode, null, null);

      assertEquals(1, nodeManager.getNodeCount(NodeStatus.inServiceHealthy()));
      assertEquals(1, clusterMap.getNumOfLeafNode(""));
      assertEquals(4, clusterMap.getMaxLevel());
      final List<DatanodeInfo> updatedNodeList = nodeManager.getAllNodes();
      assertEquals(1, updatedNodeList.size());

      DatanodeDetails returnedUpdatedNode = updatedNodeList.get(0);
      assertEquals(updatedIpAddress, returnedUpdatedNode.getIpAddress());
      assertEquals(updatedHostName, returnedUpdatedNode.getHostName());
      assertTrue(returnedUpdatedNode.getNetworkLocation()
              .startsWith("/rack1/ng"));
      assertNotNull(returnedUpdatedNode.getParent());

      assertEquals(emptyList(), nodeManager.getNodesByAddress(hostName));
      assertEquals(emptyList(), nodeManager.getNodesByAddress(ipAddress));
    }
  }

  /**
   * Test that pipelineLimit() uses the default value when the config is not set.
   */
  @Test
  public void testUsesDefaultPipelineLimitWhenUnset()
      throws IOException, AuthenticationException {

    // Creates node manager with config without limit set
    OzoneConfiguration conf = getConf();
    conf.unset(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {

      // Registers datanode with healthy volumes
      DatanodeDetails dn = registerWithCapacity(nodeManager);

      // Calls pipelineLimit() and verifies returns default value
      int limit = nodeManager.pipelineLimit(dn);
      assertEquals(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT, limit);
    }
  }

  private static Stream<Arguments> nodeStateTransitions() {
    return Stream.of(
        // start decommissioning or entering maintenance
        Arguments.of(HddsProtos.NodeOperationalState.IN_SERVICE,
                    HddsProtos.NodeOperationalState.DECOMMISSIONING, true),
        Arguments.of(HddsProtos.NodeOperationalState.IN_SERVICE,
                    HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE, true),
        // back to service (DataNodeAdminMonitor abort workflow, maintenance end time expired or node is dead)
        Arguments.of(HddsProtos.NodeOperationalState.DECOMMISSIONING,
                    HddsProtos.NodeOperationalState.IN_SERVICE, true),
        Arguments.of(HddsProtos.NodeOperationalState.DECOMMISSIONED,
                    HddsProtos.NodeOperationalState.IN_SERVICE, true),
        Arguments.of(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
                    HddsProtos.NodeOperationalState.IN_SERVICE, true),
        Arguments.of(HddsProtos.NodeOperationalState.IN_MAINTENANCE,
                    HddsProtos.NodeOperationalState.IN_SERVICE, true),
        // there is no under/over replicated containers on the node, completed the admin workflow
        Arguments.of(HddsProtos.NodeOperationalState.DECOMMISSIONING,
                    HddsProtos.NodeOperationalState.DECOMMISSIONED, false),
        Arguments.of(HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE,
                    HddsProtos.NodeOperationalState.IN_MAINTENANCE, false)
    );
  }

  @ParameterizedTest
  @MethodSource("nodeStateTransitions")
  public void testNodeOperationalStateChange(
      HddsProtos.NodeOperationalState oldState,
      HddsProtos.NodeOperationalState newState,
      boolean shouldNotify)
      throws IOException, NodeNotFoundException, AuthenticationException {
    OzoneConfiguration conf = new OzoneConfiguration();
    SCMStorageConfig scmStorageConfig = mock(SCMStorageConfig.class);
    when(scmStorageConfig.getClusterID()).thenReturn("xyz111");
    EventPublisher eventPublisher = mock(EventPublisher.class);
    HDDSLayoutVersionManager lvm = new HDDSLayoutVersionManager(scmStorageConfig.getLayoutVersion());
    createNodeManager(getConf());
    SCMNodeManager nodeManager = new SCMNodeManager(conf,
        scmStorageConfig, eventPublisher, new NetworkTopologyImpl(conf),
        scmContext, lvm);

    DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
    datanode.setPersistedOpState(oldState);
    nodeManager.register(datanode, null, HddsTestUtils.getRandomPipelineReports());

    nodeManager.setNodeOperationalState(datanode, newState, 0);
    verify(eventPublisher, times(0)).fireEvent(SCMEvents.REPLICATION_MANAGER_NOTIFY, datanode);

    DatanodeDetails reportedDatanode = MockDatanodeDetails.createDatanodeDetails(
        datanode.getID());
    reportedDatanode.setPersistedOpState(newState);

    nodeManager.processHeartbeat(reportedDatanode);

    verify(eventPublisher, times(shouldNotify ? 1 : 0)).fireEvent(
        SCMEvents.REPLICATION_MANAGER_NOTIFY, reportedDatanode);

    nodeManager.close();
  }
}
