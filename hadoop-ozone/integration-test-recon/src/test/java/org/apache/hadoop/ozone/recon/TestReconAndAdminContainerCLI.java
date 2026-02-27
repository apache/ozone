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

package org.apache.hadoop.ozone.recon;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_RECON_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.TestNodeUtil;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersResponse;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Integration tests for ensuring Recon's consistency
 * with the "ozone admin container" CLI.
 */
class TestReconAndAdminContainerCLI {

  private static final Logger LOG = LoggerFactory.getLogger(TestReconAndAdminContainerCLI.class);

  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static ScmClient scmClient;
  private static MiniOzoneCluster cluster;
  private static NodeManager scmNodeManager;
  private static long containerIdR3;
  private static OzoneBucket ozoneBucket;
  private static ContainerManager scmContainerManager;
  private static ContainerManager reconContainerManager;
  private static ReconService recon;

  private static Stream<Arguments> outOfServiceNodeStateArgs() {
    return Stream.of(
        Arguments.of(NodeOperationalState.ENTERING_MAINTENANCE,
            NodeOperationalState.IN_MAINTENANCE, true),
        Arguments.of(NodeOperationalState.DECOMMISSIONING,
            NodeOperationalState.DECOMMISSIONED, false)
    );
  }

  @BeforeAll
  static void init() throws Exception {
    setupConfigKeys();
    recon = new ReconService(CONF);
    cluster = MiniOzoneCluster.newBuilder(CONF)
        .setNumDatanodes(5)
        .addService(recon)
        .build();
    cluster.waitForClusterToBeReady();
    GenericTestUtils.setLogLevel(ReconNodeManager.class, Level.DEBUG);

    scmClient = new ContainerOperationClient(CONF);
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();
    scmContainerManager = scm.getContainerManager();
    scmNodeManager = scm.getScmNodeManager();

    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            recon.getReconServer().getReconStorageContainerManager();
    PipelineManager reconPipelineManager = reconScm.getPipelineManager();
    reconContainerManager = reconScm.getContainerManager();

    LambdaTestUtils.await(60000, 5000,
        () -> (reconPipelineManager.getPipelines().size() >= scmPipelineManager.getPipelines().size()));

    // Verify that Recon has all the pipelines from SCM.
    scmPipelineManager.getPipelines().forEach(p -> {
      Pipeline pipeline = assertDoesNotThrow(() -> reconPipelineManager.getPipeline(p.getId()));
      assertNotNull(pipeline);
    });

    assertThat(scmContainerManager.getContainers()).isEmpty();

    // Verify that all nodes are registered with Recon.
    NodeManager reconNodeManager = reconScm.getScmNodeManager();
    assertEquals(scmNodeManager.getAllNodes().size(),
        reconNodeManager.getAllNodes().size());

    OzoneClient client = cluster.newClient();
    String volumeName = "vol1";
    String bucketName = "bucket1";

    ozoneBucket = TestDataUtil.createVolumeAndBucket(
        client, volumeName, bucketName, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    String keyNameR3 = "key1";
    containerIdR3 = setupRatisKey(recon, keyNameR3,
        HddsProtos.ReplicationFactor.THREE);
  }

  @AfterAll
  static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * It's the same regardless of the ReplicationConfig,
   * but it's easier to test with Ratis ONE.
   */
  @Test
  void testMissingContainer() throws Exception {
    String keyNameR1 = "key2";
    long containerID = setupRatisKey(recon, keyNameR1,
        HddsProtos.ReplicationFactor.ONE);

    Pipeline pipeline =
        scmClient.getContainerWithPipeline(containerID).getPipeline();

    for (DatanodeDetails details : pipeline.getNodes()) {
      cluster.shutdownHddsDatanode(details);
    }
    TestHelper.waitForReplicaCount(containerID, 0, cluster);

    GenericTestUtils.waitFor(() -> {
      try {
        return scmClient.getReplicationManagerReport()
                   .getStat(ContainerHealthState.MISSING) == 1;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 1000, 20000);

    UnHealthyContainerStates containerStateForTesting =
        UnHealthyContainerStates.MISSING;
    compareRMReportToReconResponse(containerStateForTesting);

    for (DatanodeDetails details : pipeline.getNodes()) {
      cluster.restartHddsDatanode(details, false);
      TestNodeUtil.waitForDnToReachOpState(scmNodeManager, details, IN_SERVICE);
    }
  }

  @ParameterizedTest
  @MethodSource("outOfServiceNodeStateArgs")
  @Flaky("HDDS-11128")
  void testNodesInDecommissionOrMaintenance(
      NodeOperationalState initialState, NodeOperationalState finalState,
      boolean isMaintenance) throws Exception {
    Pipeline pipeline =
        scmClient.getContainerWithPipeline(containerIdR3).getPipeline();

    List<DatanodeDetails> details =
        pipeline.getNodes().stream()
            .filter(d -> d.getPersistedOpState().equals(IN_SERVICE))
            .collect(Collectors.toList());

    final DatanodeDetails nodeToGoOffline1 = details.get(0);
    final DatanodeDetails nodeToGoOffline2 = details.get(1);

    UnHealthyContainerStates underReplicatedState =
        UnHealthyContainerStates.UNDER_REPLICATED;
    UnHealthyContainerStates overReplicatedState =
        UnHealthyContainerStates.OVER_REPLICATED;

    // First node goes offline.
    if (isMaintenance) {
      scmClient.startMaintenanceNodes(Collections.singletonList(
          TestNodeUtil.getDNHostAndPort(nodeToGoOffline1)), 0, true);
    } else {
      scmClient.decommissionNodes(Collections.singletonList(
          TestNodeUtil.getDNHostAndPort(nodeToGoOffline1)), false);
    }

    TestNodeUtil.waitForDnToReachOpState(scmNodeManager,
        nodeToGoOffline1, initialState);

    compareRMReportToReconResponse(underReplicatedState);
    compareRMReportToReconResponse(overReplicatedState);

    TestNodeUtil.waitForDnToReachOpState(scmNodeManager,
        nodeToGoOffline1, finalState);
    // Every time a node goes into decommission,
    // a new replica-copy is made to another node.
    // For maintenance, there is no replica-copy in this case.
    if (!isMaintenance) {
      TestHelper.waitForReplicaCount(containerIdR3, 4, cluster);
    }

    compareRMReportToReconResponse(underReplicatedState);
    compareRMReportToReconResponse(overReplicatedState);

    // Second node goes offline.
    if (isMaintenance) {
      scmClient.startMaintenanceNodes(Collections.singletonList(
          TestNodeUtil.getDNHostAndPort(nodeToGoOffline2)), 0, true);
    } else {
      scmClient.decommissionNodes(Collections.singletonList(
          TestNodeUtil.getDNHostAndPort(nodeToGoOffline2)), false);
    }

    TestNodeUtil.waitForDnToReachOpState(scmNodeManager,
        nodeToGoOffline2, initialState);

    compareRMReportToReconResponse(underReplicatedState);
    compareRMReportToReconResponse(overReplicatedState);

    TestNodeUtil.waitForDnToReachOpState(scmNodeManager,
        nodeToGoOffline2, finalState);

    // There will be a replica copy for both maintenance and decommission.
    // maintenance 3 -> 4, decommission 4 -> 5.
    int expectedReplicaNum = isMaintenance ? 4 : 5;
    TestHelper.waitForReplicaCount(containerIdR3, expectedReplicaNum, cluster);

    compareRMReportToReconResponse(underReplicatedState);
    compareRMReportToReconResponse(overReplicatedState);

    scmClient.recommissionNodes(Arrays.asList(
        TestNodeUtil.getDNHostAndPort(nodeToGoOffline1),
        TestNodeUtil.getDNHostAndPort(nodeToGoOffline2)));

    TestNodeUtil.waitForDnToReachOpState(scmNodeManager,
        nodeToGoOffline1, IN_SERVICE);
    TestNodeUtil.waitForDnToReachOpState(scmNodeManager,
        nodeToGoOffline2, IN_SERVICE);

    TestNodeUtil.waitForDnToReachPersistedOpState(nodeToGoOffline1, IN_SERVICE);
    TestNodeUtil.waitForDnToReachPersistedOpState(nodeToGoOffline2, IN_SERVICE);

    compareRMReportToReconResponse(underReplicatedState);
    compareRMReportToReconResponse(overReplicatedState);
  }

  /**
   * The purpose of this method, isn't to validate the numbers
   * but to make sure that they are consistent between
   * Recon and the ReplicationManager.
   */
  private static void compareRMReportToReconResponse(UnHealthyContainerStates containerState)
      throws Exception {
    assertNotNull(containerState);

    // Both threads are running every 1 second.
    // Wait until all values are equal.
    GenericTestUtils.waitFor(() -> assertReportsMatch(containerState),
        1000, 40000);
  }

  private static boolean assertReportsMatch(UnHealthyContainerStates state) {
    ReplicationManagerReport rmReport;
    UnhealthyContainersResponse reconResponse;

    try {
      rmReport = scmClient.getReplicationManagerReport();
      reconResponse = TestReconEndpointUtil
          .getUnhealthyContainersFromRecon(CONF, state);

      assertEquals(rmReport.getStat(ContainerHealthState.MISSING), reconResponse.getMissingCount());
      assertEquals(rmReport.getStat(ContainerHealthState.UNDER_REPLICATED), reconResponse.getUnderReplicatedCount());
      assertEquals(rmReport.getStat(ContainerHealthState.OVER_REPLICATED), reconResponse.getOverReplicatedCount());
      assertEquals(rmReport.getStat(ContainerHealthState.MIS_REPLICATED), reconResponse.getMisReplicatedCount());
    } catch (IOException e) {
      LOG.info("Error getting report", e);
      return false;
    } catch (AssertionError e) {
      LOG.info("Reports do not match (yet): {}", e.getMessage());
      return false;
    }

    // Recon's UnhealthyContainerResponse contains a list of containers
    // for a particular state. Check if RMs sample of containers can be
    // found in Recon's list of containers for a particular state.
    ContainerHealthState rmState = ContainerHealthState.UNHEALTHY;

    if (state.equals(UnHealthyContainerStates.MISSING) &&
        reconResponse.getMissingCount() > 0) {
      rmState = ContainerHealthState.MISSING;
    } else if (state.equals(UnHealthyContainerStates.UNDER_REPLICATED) &&
               reconResponse.getUnderReplicatedCount() > 0) {
      rmState = ContainerHealthState.UNDER_REPLICATED;
    } else if (state.equals(UnHealthyContainerStates.OVER_REPLICATED) &&
               reconResponse.getOverReplicatedCount() > 0) {
      rmState = ContainerHealthState.OVER_REPLICATED;
    } else if (state.equals(UnHealthyContainerStates.MIS_REPLICATED) &&
               reconResponse.getMisReplicatedCount() > 0) {
      rmState = ContainerHealthState.MIS_REPLICATED;
    }

    List<ContainerID> rmContainerIDs = rmReport.getSample(rmState);
    List<Long> rmIDsToLong = new ArrayList<>();
    for (ContainerID id : rmContainerIDs) {
      rmIDsToLong.add(id.getId());
    }
    List<Long> reconContainerIDs =
        reconResponse.getContainers()
            .stream()
            .map(UnhealthyContainerMetadata::getContainerID)
            .collect(Collectors.toList());
    assertThat(reconContainerIDs).containsAll(rmIDsToLong);

    return true;
  }

  private static long setupRatisKey(ReconService reconService, String keyName,
      HddsProtos.ReplicationFactor replicationFactor) throws Exception {
    OmKeyInfo omKeyInfo = createTestKey(keyName,
        RatisReplicationConfig.getInstance(replicationFactor));

    // Sync Recon with OM, to force it to get the new key entries.
    TestReconEndpointUtil.triggerReconDbSyncWithOm(CONF);

    List<Long> containerIDs = getContainerIdsForKey(omKeyInfo);
    // The list has only 1 containerID.
    assertEquals(1, containerIDs.size());
    long containerID = containerIDs.get(0);

    // Verify Recon picked up the new container.
    assertEquals(scmContainerManager.getContainers(),
        reconContainerManager.getContainers());

    ReconContainerMetadataManager reconContainerMetadataManager =
        reconService.getReconServer().getReconContainerMetadataManager();

    // Verify Recon picked up the new keys and
    // updated its container key mappings.
    GenericTestUtils.waitFor(() -> {
      try {
        return reconContainerMetadataManager
                   .getKeyCountForContainer(containerID) > 0;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 1000, 20000);

    return containerID;
  }

  private static OmKeyInfo createTestKey(String keyName,
      ReplicationConfig replicationConfig)
      throws IOException {
    byte[] textBytes = "Testing".getBytes(UTF_8);
    TestDataUtil.createKey(ozoneBucket, keyName, replicationConfig, textBytes);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
                            .setVolumeName(ozoneBucket.getVolumeName())
                            .setBucketName(ozoneBucket.getName())
                            .setKeyName(keyName)
                            .build();
    return cluster.getOzoneManager().lookupKey(keyArgs);
  }

  private static List<Long> getContainerIdsForKey(OmKeyInfo omKeyInfo) {
    assertNotNull(omKeyInfo.getLatestVersionLocations());
    List<OmKeyLocationInfo> locations =
        omKeyInfo.getLatestVersionLocations().getLocationList();

    List<Long> ids = new ArrayList<>();
    for (OmKeyLocationInfo location : locations) {
      ids.add(location.getContainerID());
    }
    return ids;
  }

  private static void setupConfigKeys() {
    CONF.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    CONF.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    CONF.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    CONF.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    CONF.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    CONF.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    CONF.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, SECONDS);
    CONF.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    CONF.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);
    CONF.setTimeDuration(OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
        1, SECONDS);
    CONF.setTimeDuration(
        ScmConfigKeys.OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL,
        1, SECONDS);
    CONF.setTimeDuration(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, SECONDS);
    // Configure multiple task threads for concurrent task execution
    CONF.setInt("ozone.recon.task.thread.count", 6);
    CONF.set(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, "2s");
    CONF.set(ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL, "2s");
    CONF.set(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, "5s");

    CONF.setTimeDuration(HDDS_RECON_HEARTBEAT_INTERVAL,
        1, TimeUnit.SECONDS);
    CONF.setTimeDuration(OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY,
        1, TimeUnit.SECONDS);

    CONF.set(ScmUtils.getContainerReportConfPrefix() +
             ".queue.wait.threshold", "1");
    CONF.set(ScmUtils.getContainerReportConfPrefix() +
             ".execute.wait.threshold", "1");

    ReconTaskConfig reconTaskConfig = CONF.getObject(ReconTaskConfig.class);
    reconTaskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(1));
    CONF.setFromObject(reconTaskConfig);

    ReplicationManager.ReplicationManagerConfiguration replicationConf =
        CONF.getObject(ReplicationManager
                           .ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofSeconds(1));
    replicationConf.setUnderReplicatedInterval(Duration.ofSeconds(1));
    replicationConf.setOverReplicatedInterval(Duration.ofSeconds(1));
    CONF.setFromObject(replicationConf);
  }
}
