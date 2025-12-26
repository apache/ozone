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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for ReplicationManager.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestReplicationManagerIntegration {
  private static final int DATANODE_COUNT = 5;
  private static final int HEALTHY_REPLICA_NUM = 3;
  private static final RatisReplicationConfig RATIS_REPLICATION_CONFIG = RatisReplicationConfig
      .getInstance(HddsProtos.ReplicationFactor.THREE);
  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationManagerIntegration.class);

  private MiniOzoneCluster cluster;
  private NodeManager nodeManager;
  private ContainerManager containerManager;
  private ReplicationManager replicationManager;
  private ReplicationManagerConfiguration replicationConf;
  private StorageContainerManager scm;
  private OzoneClient client;
  private ContainerOperationClient scmClient;
  private OzoneBucket bucket;

  @BeforeEach
  void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 100, MILLISECONDS);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 100, MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 100, MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 100, MILLISECONDS);
    conf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 100, MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 2, SECONDS);
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

    replicationConf = conf.getObject(ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofSeconds(1));
    replicationConf.setUnderReplicatedInterval(Duration.ofMillis(100));
    replicationConf.setOverReplicatedInterval(Duration.ofMillis(100));
    conf.setFromObject(replicationConf);

    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(DATANODE_COUNT);

    cluster = builder.build();
    cluster.getConf().setTimeDuration(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, 0, SECONDS);
    cluster.waitForClusterToBeReady();

    scm = cluster.getStorageContainerManager();
    nodeManager = scm.getScmNodeManager();
    containerManager = scm.getContainerManager();
    replicationManager = scm.getReplicationManager();

    client = cluster.newClient();
    scmClient = new ContainerOperationClient(cluster.getConf());
    bucket = TestDataUtil.createVolumeAndBucket(client);
  }

  @AfterEach
  void shutdown() {
    IOUtils.close(LOG, client, scmClient, cluster);
  }

  @Order(1)
  @Test
  void testReplicationManagerNotify() throws Exception {
    // Test if RM notify works
    replicationManager.getConfig().setInterval(Duration.ofSeconds(300));
    GenericTestUtils.waitFor(() -> replicationManager.isThreadWaiting(), 200, 30000);
  }

  @Order(Integer.MAX_VALUE)
  @Test
  public void testClosedContainerReplicationWhenNodeDies()
      throws Exception {
    String keyName = "key-" + UUID.randomUUID();
    TestDataUtil.createKey(bucket, keyName, RATIS_REPLICATION_CONFIG,
        "this is the content".getBytes(StandardCharsets.UTF_8));

    // Get the container ID for the key
    OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    long containerID = keyLocations.get(0).getContainerID();
    ContainerID containerId = ContainerID.valueOf(containerID);
    // open container would not be handled to do any further processing in RM
    OzoneTestUtils.closeContainer(scm, containerManager.getContainer(containerId));

    assertEquals(HEALTHY_REPLICA_NUM, containerManager.getContainerReplicas(containerId).size());

    final DatanodeDetails targetDatanode = findReplica(containerId);

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

  private DatanodeDetails findReplica(ContainerID containerId) throws ContainerNotFoundException {
    // Find a datanode that has a replica of this container
    return containerManager.getContainerReplicas(containerId).stream()
        .findFirst()
        .orElseThrow(() -> new AssertionError("Replica not found for " + containerId))
        .getDatanodeDetails();
  }

  @ParameterizedTest
  @EnumSource(value = NodeOperationalState.class, names = {"IN_MAINTENANCE", "DECOMMISSIONED"})
  void testClosedContainerReplicationWhenNodeDecommissionAndBackToInService(
      NodeOperationalState expectedOpState)
      throws Exception {

    String keyName = "key-" + UUID.randomUUID();
    TestDataUtil.createKey(bucket, keyName, RATIS_REPLICATION_CONFIG,
        "this is the content".getBytes(StandardCharsets.UTF_8));

    OzoneKeyDetails key = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocations = key.getOzoneKeyLocations();

    long containerID = keyLocations.get(0).getContainerID();
    ContainerID containerId = ContainerID.valueOf(containerID);
    ContainerInfo containerInfo = containerManager.getContainer(containerId);
    OzoneTestUtils.closeContainer(scm, containerInfo);

    assertEquals(containerManager.getContainerReplicas(containerId).size(), HEALTHY_REPLICA_NUM);

    DatanodeDetails datanode = findReplica(containerId);

    if (expectedOpState == IN_MAINTENANCE) {
      scmClient.startMaintenanceNodes(Collections.singletonList(getDNHostAndPort(datanode)), 0, false);
      waitForDnToReachOpState(nodeManager, datanode, expectedOpState);
      assertEquals(containerManager.getContainerReplicas(containerId).size(),
          HEALTHY_REPLICA_NUM);
    } else {
      scmClient.decommissionNodes(Collections.singletonList(getDNHostAndPort(datanode)), false);
      waitForDnToReachOpState(nodeManager, datanode, expectedOpState);
      // decommissioning node would be excluded
      assertEquals(containerManager.getContainerReplicas(containerId).size(),
          HEALTHY_REPLICA_NUM + 1);
    }

    // bring the node back to service
    scmClient.recommissionNodes(Collections.singletonList(getDNHostAndPort(datanode)));

    waitForDnToReachOpState(nodeManager, datanode, IN_SERVICE);
    waitForDnToReachPersistedOpState(datanode, IN_SERVICE);

    GenericTestUtils.waitFor(() -> {
      try {
        return containerManager.getContainerReplicas(containerId).size() == HEALTHY_REPLICA_NUM;
      } catch (Exception e) {
        return false;
      }
    }, 200, 30000);
  }

  /**
   * A node containing a replica of a Ratis container is put into maintenance with no expiry. When it is maintenance, it
   * is shutdown and the test waits until it's handled as dead. Then another node containing this container's replica is
   * decommissioned. The expectation is that it should successfully decommission.
   */
  @Test
  public void testDeadMaintenanceNodeAndDecommission() throws Exception {
    String keyName = "key-" + UUID.randomUUID();
    TestDataUtil.createKey(bucket, keyName, RATIS_REPLICATION_CONFIG,
        "this is the content".getBytes(StandardCharsets.UTF_8));

    OzoneKeyDetails key = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocations = key.getOzoneKeyLocations();

    long iD = keyLocations.get(0).getContainerID();
    ContainerID containerId = ContainerID.valueOf(iD);
    ContainerInfo containerInfo = containerManager.getContainer(containerId);
    OzoneTestUtils.closeContainer(scm, containerInfo);

    assertEquals(HEALTHY_REPLICA_NUM,
        containerManager.getContainerReplicas(containerId).size());

    List<DatanodeDetails> dns = containerManager
        .getContainerReplicas(containerId)
        .stream().map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());

    DatanodeDetails maintenanceDn = dns.get(0);
    DatanodeDetails decomDn = dns.get(1);

    scmClient.startMaintenanceNodes(Collections.singletonList(getDNHostAndPort(maintenanceDn)), 0, false);
    waitForDnToReachOpState(nodeManager, maintenanceDn, IN_MAINTENANCE);
    cluster.shutdownHddsDatanode(maintenanceDn);
    waitForDnToReachHealthState(nodeManager, maintenanceDn, DEAD);

    ContainerReplicaCount containerReplicaCount = replicationManager.getContainerReplicaCount(containerId);
    assertTrue(containerReplicaCount.isSufficientlyReplicated());

    scmClient.decommissionNodes(Collections.singletonList(getDNHostAndPort(decomDn)), false);
    waitForDnToReachOpState(nodeManager, decomDn, DECOMMISSIONED);

    assertEquals(HEALTHY_REPLICA_NUM + 1, containerManager.getContainerReplicas(containerId).size());
    ReplicationManagerReport report = new ReplicationManagerReport(replicationConf.getContainerSampleLimit());
    replicationManager.checkContainerStatus(containerInfo, report);
    assertEquals(0, report.getStat(ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(ReplicationManagerReport.HealthState.MIS_REPLICATED));
    assertEquals(0, report.getStat(ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testOneDeadMaintenanceNodeAndOneLiveMaintenanceNodeAndOneDecommissionNode() throws Exception {
    String keyName = "key-" + UUID.randomUUID();
    TestDataUtil.createKey(bucket, keyName, RATIS_REPLICATION_CONFIG,
        "this is the content".getBytes(StandardCharsets.UTF_8));

    OzoneKeyDetails key = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocations = key.getOzoneKeyLocations();

    long iD = keyLocations.get(0).getContainerID();
    ContainerID containerId = ContainerID.valueOf(iD);
    ContainerInfo containerInfo = containerManager.getContainer(containerId);
    OzoneTestUtils.closeContainer(scm, containerInfo);

    assertEquals(HEALTHY_REPLICA_NUM,
        containerManager.getContainerReplicas(containerId).size());

    List<DatanodeDetails> dns = containerManager
        .getContainerReplicas(containerId)
        .stream().map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());

    DatanodeDetails maintenanceDn = dns.get(0);
    DatanodeDetails decomDn = dns.get(1);
    DatanodeDetails secondMaintenanceDn = dns.get(2);

    scmClient.startMaintenanceNodes(Collections.singletonList(getDNHostAndPort(maintenanceDn)), 0, false);
    waitForDnToReachOpState(nodeManager, maintenanceDn, IN_MAINTENANCE);
    cluster.shutdownHddsDatanode(maintenanceDn);
    waitForDnToReachHealthState(nodeManager, maintenanceDn, DEAD);

    ContainerReplicaCount containerReplicaCount = replicationManager.getContainerReplicaCount(containerId);
    assertTrue(containerReplicaCount.isSufficientlyReplicated());

    scmClient.decommissionNodes(Collections.singletonList(getDNHostAndPort(decomDn)), false);
    scmClient.startMaintenanceNodes(Collections.singletonList(getDNHostAndPort(secondMaintenanceDn)), 0, false);
    waitForDnToReachOpState(nodeManager, decomDn, DECOMMISSIONED);
    waitForDnToReachOpState(nodeManager, secondMaintenanceDn, IN_MAINTENANCE);

    assertEquals(HEALTHY_REPLICA_NUM + 2, containerManager.getContainerReplicas(containerId).size());
    ReplicationManagerReport report = new ReplicationManagerReport(replicationConf.getContainerSampleLimit());
    replicationManager.checkContainerStatus(containerInfo, report);
    assertEquals(0, report.getStat(ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(0, report.getStat(ReplicationManagerReport.HealthState.MIS_REPLICATED));
    assertEquals(0, report.getStat(ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }
}
