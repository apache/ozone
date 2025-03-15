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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.getDNHostAndPort;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.waitForDnToReachHealthState;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.waitForDnToReachOpState;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.waitForDnToReachPersistedOpState;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneClusterProvider;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for ReplicationManager.
 */
public class TestReplicationManagerIntegration {
  private static final int DATANODE_COUNT = 5;
  private static final int HEALTHY_REPLICA_NUM = 3;
  private static String bucketName = "bucket1";
  private static String volName = "vol1";
  private static RatisReplicationConfig ratisRepConfig = RatisReplicationConfig
      .getInstance(HddsProtos.ReplicationFactor.THREE);
  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationManagerIntegration.class);

  private MiniOzoneCluster cluster;
  private NodeManager nodeManager;
  private ContainerManager containerManager;
  private ReplicationManager replicationManager;
  private StorageContainerManager scm;
  private OzoneClient client;
  private ContainerOperationClient scmClient;
  private OzoneBucket bucket;

  private static MiniOzoneClusterProvider clusterProvider;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
        1, SECONDS);
    conf.setTimeDuration(
        ScmConfigKeys.OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL,
        1, SECONDS);
    conf.setTimeDuration(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, SECONDS);
    conf.set(OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, "2s");
    conf.set(OZONE_SCM_PIPELINE_SCRUB_INTERVAL, "2s");
    conf.set(OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, "5s");

    ReplicationManagerConfiguration replicationConf = conf.getObject(ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofSeconds(1));
    replicationConf.setUnderReplicatedInterval(Duration.ofSeconds(1));
    replicationConf.setOverReplicatedInterval(Duration.ofSeconds(1));
    conf.setFromObject(replicationConf);

    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(DATANODE_COUNT);

    clusterProvider = new MiniOzoneClusterProvider(builder, 9);
  }

  @AfterAll
  public static void shutdown() throws InterruptedException {
    if (clusterProvider != null) {
      clusterProvider.shutdown();
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    cluster = clusterProvider.provide();
    cluster.getConf().setTimeDuration(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, 0, SECONDS);
    cluster.waitForClusterToBeReady();

    scm = cluster.getStorageContainerManager();
    nodeManager = scm.getScmNodeManager();
    containerManager = scm.getContainerManager();
    replicationManager = scm.getReplicationManager();

    client = cluster.newClient();
    scmClient = new ContainerOperationClient(cluster.getConf());
    bucket = TestDataUtil.createVolumeAndBucket(client, volName, bucketName);
  }

  @AfterEach
  public void tearDown() throws InterruptedException, IOException {
    IOUtils.close(LOG, client, scmClient);
    if (cluster != null) {
      clusterProvider.destroy(cluster);
    }
  }

  @Test
  public void testClosedContainerReplicationWhenNodeDies()
      throws Exception {
    // Test if RM notify works
    replicationManager.getConfig().setInterval(Duration.ofSeconds(300));
    GenericTestUtils.waitFor(() -> {
      return replicationManager.isThreadWaiting();
    }, 1000, 30000);

    String keyName = "key-" + UUID.randomUUID();
    TestDataUtil.createKey(bucket, keyName, ratisRepConfig,
        "this is the content".getBytes(StandardCharsets.UTF_8));

    // Get the container ID for the key
    OzoneKey key = bucket.getKey(keyName);
    OzoneKeyDetails keyDetails = (OzoneKeyDetails) key;
    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    long containerID = keyLocations.get(0).getContainerID();
    ContainerID containerId = ContainerID.valueOf(containerID);
    // open container would not be handled to do any further processing in RM
    OzoneTestUtils.closeContainer(scm, containerManager.getContainer(containerId));

    assertEquals(HEALTHY_REPLICA_NUM, containerManager.getContainerReplicas(containerId).size());

    // Find a datanode that has a replica of this container
    final DatanodeDetails targetDatanode = containerManager.getContainerReplicas(containerId).stream().findFirst().get()
        .getDatanodeDetails();

    cluster.shutdownHddsDatanode(targetDatanode);
    waitForDnToReachHealthState(nodeManager, targetDatanode, DEAD);

    // Check if the replicas nodes don't contain dead one
    // and the replica of container replica num is considered to be healthy
    GenericTestUtils.waitFor(() -> {
      try {
        Set<ContainerReplica> replicas = containerManager.getContainerReplicas(containerId);
        boolean deadNodeNotInContainerReplica = replicas.stream()
            .noneMatch(r -> r.getDatanodeDetails().equals(targetDatanode));
        boolean hasHealthyReplicaNum = replicas.size() == HEALTHY_REPLICA_NUM;
        return deadNodeNotInContainerReplica && hasHealthyReplicaNum;
      } catch (ContainerNotFoundException e) {
        return false;
      }
    }, 100, 30000);
  }

  private static Stream<Arguments> decommissionTransitionTestCases() {
    return Stream.of(
        // Test case 1: Node enters maintenance
        Arguments.of(
            "Node enters maintenance",
            (BiConsumer<ContainerOperationClient, DatanodeDetails>) (client, dn) -> {
              try {
                client.startMaintenanceNodes(Arrays.asList(getDNHostAndPort(dn)), 0, false);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            },
            IN_MAINTENANCE),
        // Test case 2: Node is decommissioned
        Arguments.of(
            "Node is decommissioned",
            (BiConsumer<ContainerOperationClient, DatanodeDetails>) (client, dn) -> {
              try {
                client.decommissionNodes(Arrays.asList(getDNHostAndPort(dn)), false);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            },
            DECOMMISSIONED));
  }

  @ParameterizedTest
  @MethodSource("decommissionTransitionTestCases")
  public void testClosedContainerReplicationWhenNodeDecommissionAndBackToInService(
      String testName,
      BiConsumer<ContainerOperationClient, DatanodeDetails> action,
      NodeOperationalState expectedOpState)
      throws Exception {

    // Test if RM notify works
    replicationManager.getConfig().setInterval(Duration.ofSeconds(300));
    GenericTestUtils.waitFor(() -> {
      return replicationManager.isThreadWaiting();
    }, 1000, 30000);

    String keyName = "key-" + UUID.randomUUID();
    TestDataUtil.createKey(bucket, keyName, ratisRepConfig,
        "this is the content".getBytes(StandardCharsets.UTF_8));

    OzoneKey key = bucket.getKey(keyName);
    OzoneKeyDetails keyDetails = (OzoneKeyDetails) key;
    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();

    long containerID = keyLocations.get(0).getContainerID();
    ContainerID containerId = ContainerID.valueOf(containerID);
    ContainerInfo containerInfo = containerManager.getContainer(containerId);
    OzoneTestUtils.closeContainer(scm, containerInfo);

    assertEquals(containerManager.getContainerReplicas(containerId).size(), HEALTHY_REPLICA_NUM);

    DatanodeDetails datanode = containerManager.getContainerReplicas(containerId).stream().findFirst().get()
        .getDatanodeDetails();

    action.accept(scmClient, datanode);

    waitForDnToReachOpState(nodeManager, datanode, expectedOpState);

    if (expectedOpState == DECOMMISSIONED) {
      // decommissioning node would be excluded
      assertEquals(containerManager.getContainerReplicas(containerId).size(),
          HEALTHY_REPLICA_NUM + 1);
    } else {
      assertEquals(containerManager.getContainerReplicas(containerId).size(),
          HEALTHY_REPLICA_NUM);
    }

    // bring the node back to service
    scmClient.recommissionNodes(Arrays.asList(getDNHostAndPort(datanode)));

    waitForDnToReachOpState(nodeManager, datanode, IN_SERVICE);
    waitForDnToReachPersistedOpState(datanode, IN_SERVICE);

    GenericTestUtils.waitFor(() -> {
      try {
        return containerManager.getContainerReplicas(containerId).size() == HEALTHY_REPLICA_NUM;
      } catch (Exception e) {
        return false;
      }
    }, 1000, 30000);
  }
}
