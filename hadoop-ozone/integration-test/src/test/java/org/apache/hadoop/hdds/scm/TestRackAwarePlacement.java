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

package org.apache.hadoop.hdds.scm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests that verify rack/host topology is correctly propagated
 * to SCM and that pipeline and container placement respect rack boundaries.
 *
 */
public class TestRackAwarePlacement {

  private static final String RACK0 = "/rack0";
  private static final String RACK1 = "/rack1";

  private static final String[] RACKS = {
      RACK0, RACK0, RACK0,
      RACK1, RACK1, RACK1
  };

  private static final String[] HOSTS = {
      "host0.test", "host1.test", "host2.test",
      "host3.test", "host4.test", "host5.test"
  };

  private static void applyReplicationSpeedupConfig(OzoneConfiguration conf) {
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL,
        3, TimeUnit.SECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL,
        6, TimeUnit.SECONDS);
    conf.setTimeDuration("hdds.scm.replication.thread.interval",
        1, TimeUnit.SECONDS);
    conf.setTimeDuration("hdds.scm.replication.under.replicated.interval",
        5, TimeUnit.SECONDS);
    conf.setTimeDuration("hdds.scm.replication.over.replicated.interval",
        5, TimeUnit.SECONDS);
  }

  static Stream<Arguments> rackAwarePolicies() {
    return Stream.of(
        Arguments.of(
            "org.apache.hadoop.hdds.scm.container.placement.algorithms"
                + ".SCMContainerPlacementRackAware"),
        Arguments.of(
            "org.apache.hadoop.hdds.scm.container.placement.algorithms"
                + ".SCMContainerPlacementRackScatter")
    );
  }

  @ParameterizedTest
  @MethodSource("rackAwarePolicies")
  void testContainerPlacementWithPolicy(
      String placementClassName) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        placementClassName);
    applyReplicationSpeedupConfig(conf);

    try (MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setRacks(RACKS)
        .setHosts(HOSTS)
        .build()) {
      cluster.waitForClusterToBeReady();
      cluster.waitForPipelineTobeReady(ReplicationFactor.THREE, 60_000);

      StorageContainerManager scm = cluster.getStorageContainerManager();
      PlacementPolicy actualPolicy = scm.getContainerPlacementPolicy();
      assertEquals(placementClassName, actualPolicy.getClass().getName(),
          "Placement policy was not set correctly");

      assertPipelinesSpanMultipleRacks(cluster);
      assertContainerReplicationIsRackAware(cluster);
    }
  }

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class WithRacksAndHosts {

    private MiniOzoneCluster cluster;

    @BeforeAll
    void init() throws Exception {
      OzoneConfiguration conf = new OzoneConfiguration();
      applyReplicationSpeedupConfig(conf);
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setRacks(RACKS)
          .setHosts(HOSTS)
          .build();
      cluster.waitForClusterToBeReady();
      cluster.waitForPipelineTobeReady(ReplicationFactor.THREE, 60_000);
    }

    @AfterAll
    void tearDown() {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    @Test
    void testDatanodesHaveCorrectRack() {
      assertRackAssignments(cluster, RACKS);
    }

    @Test
    void testDatanodesHaveCorrectHostname() {
      assertHostnameAssignments(cluster, HOSTS);
    }

    @Test
    void testRatisPipelineSpansMultipleRacks() {
      assertPipelinesSpanMultipleRacks(cluster);
    }

    @Test
    void testContainerReplicationIsRackAware() throws Exception {
      assertContainerReplicationIsRackAware(cluster);
    }
  }

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class WithRacksOnly {

    private MiniOzoneCluster cluster;

    @BeforeAll
    void init() throws Exception {
      OzoneConfiguration conf = new OzoneConfiguration();
      applyReplicationSpeedupConfig(conf);
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setRacks(RACKS)
          .build();
      cluster.waitForClusterToBeReady();
      cluster.waitForPipelineTobeReady(ReplicationFactor.THREE, 60_000);
    }

    @AfterAll
    void tearDown() {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    @Test
    void testDatanodesHaveCorrectRack() {
      assertRackAssignments(cluster, RACKS);
    }

    @Test
    void testRatisPipelineSpansMultipleRacks() {
      assertPipelinesSpanMultipleRacks(cluster);
    }

    @Test
    void testContainerReplicationIsRackAware() throws Exception {
      assertContainerReplicationIsRackAware(cluster);
    }
  }

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class WithHostsOnly {

    private MiniOzoneCluster cluster;

    @BeforeAll
    void init() throws Exception {
      OzoneConfiguration conf = new OzoneConfiguration();
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setHosts(HOSTS)
          .build();
      cluster.waitForClusterToBeReady();
      cluster.waitForPipelineTobeReady(ReplicationFactor.THREE, 60_000);
    }

    @AfterAll
    void tearDown() {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    @Test
    void testDatanodesHaveCorrectHostname() {
      assertHostnameAssignments(cluster, HOSTS);
    }

    @Test
    void testDatanodesAllInDefaultRack() {
      NodeManager nodeManager =
          cluster.getStorageContainerManager().getScmNodeManager();
      List<? extends DatanodeDetails> allNodes = nodeManager.getAllNodes();

      for (DatanodeDetails dn : allNodes) {
        assertEquals(NetworkTopology.DEFAULT_RACK, dn.getNetworkLocation(),
            "Datanode " + dn.getHostName()
                + " should be in default rack when no racks are configured");
      }
    }
  }

  private static void assertContainerReplicationIsRackAware(
      MiniOzoneCluster cluster) throws Exception {
    StorageContainerManager scm = cluster.getStorageContainerManager();

    try (OzoneClient client = cluster.newClient()) {
      ObjectStore store = client.getObjectStore();
      store.createVolume("testvol");
      OzoneVolume volume = store.getVolume("testvol");
      volume.createBucket("testbucket");
      OzoneBucket bucket = volume.getBucket("testbucket");

      byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);
      try (OzoneOutputStream out = bucket.createKey(
          "testkey", data.length,
          RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
          new HashMap<>())) {
        out.write(data);
      }
    }

    ContainerInfo targetContainer = null;
    Set<ContainerReplica> replicas = null;
    for (ContainerInfo c : scm.getContainerManager().getContainers()) {
      Set<ContainerReplica> r =
          scm.getContainerManager().getContainerReplicas(c.containerID());
      if (r.size() >= 3) {
        targetContainer = c;
        replicas = r;
        break;
      }
    }
    assertNotNull(targetContainer,
        "Should find a container with 3 replicas");
    ContainerID containerID = targetContainer.containerID();

    DatanodeDetails stoppedDn =
        replicas.iterator().next().getDatanodeDetails();
    cluster.shutdownHddsDatanode(stoppedDn);

    GenericTestUtils.waitFor(() -> {
      try {
        return scm.getScmNodeManager()
            .getNodeStatus(stoppedDn)
            .getHealth() == HddsProtos.NodeState.DEAD;
      } catch (Exception e) {
        return false;
      }
    }, 500, 30_000);

    GenericTestUtils.waitFor(() -> {
      try {
        return scm.getContainerManager()
            .getContainerReplicas(containerID)
            .size() >= 3;
      } catch (Exception e) {
        return false;
      }
    }, 1_000, 60_000);

    Set<String> racks = scm.getContainerManager()
        .getContainerReplicas(containerID)
        .stream()
        .map(r -> r.getDatanodeDetails().getNetworkLocation())
        .collect(Collectors.toSet());

    assertTrue(racks.size() >= 2,
        "Container replicas after re-replication should span at least "
            + "2 racks, but were on: " + racks);
  }

  private static void assertRackAssignments(MiniOzoneCluster cluster,
                                            String[] expectedRacks) {
    NodeManager nodeManager =
        cluster.getStorageContainerManager().getScmNodeManager();
    List<? extends DatanodeDetails> allNodes = nodeManager.getAllNodes();

    assertEquals(expectedRacks.length, allNodes.size(),
        "Number of registered datanodes should match number of configured racks");

    long actualRack0 = allNodes.stream()
        .filter(dn -> RACK0.equals(dn.getNetworkLocation()))
        .count();
    long actualRack1 = allNodes.stream()
        .filter(dn -> RACK1.equals(dn.getNetworkLocation()))
        .count();

    long expectedRack0 =
        Arrays.stream(expectedRacks).filter(RACK0::equals).count();
    long expectedRack1 =
        Arrays.stream(expectedRacks).filter(RACK1::equals).count();

    assertEquals(expectedRack0, actualRack0,
        "Expected " + expectedRack0 + " datanodes on " + RACK0);
    assertEquals(expectedRack1, actualRack1,
        "Expected " + expectedRack1 + " datanodes on " + RACK1);

    for (DatanodeDetails dn : allNodes) {
      String location = dn.getNetworkLocation();
      assertNotNull(location,
          "Network location must not be null for " + dn.getHostName());
      assertTrue(location.equals(RACK0) || location.equals(RACK1),
          "Unexpected rack for datanode " + dn.getHostName()
              + ": " + location);
    }
  }

  private static void assertHostnameAssignments(MiniOzoneCluster cluster,
                                                String[] expectedHosts) {
    NodeManager nodeManager =
        cluster.getStorageContainerManager().getScmNodeManager();
    List<? extends DatanodeDetails> allNodes = nodeManager.getAllNodes();

    assertEquals(expectedHosts.length, allNodes.size(),
        "Number of registered datanodes should match number of configured hosts");

    Set<String> actual = allNodes.stream()
        .map(DatanodeDetails::getHostName)
        .collect(Collectors.toSet());

    Set<String> expected = Arrays.stream(expectedHosts)
        .collect(Collectors.toSet());

    assertEquals(expected, actual,
        "Registered datanode hostnames should match configured hosts");
  }

  private static void assertPipelinesSpanMultipleRacks(
      MiniOzoneCluster cluster) {
    List<Pipeline> pipelines = cluster.getStorageContainerManager()
        .getPipelineManager()
        .getPipelines(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.OPEN);

    assertFalse(pipelines.isEmpty(),
        "There should be at least one open RATIS THREE pipeline");

    for (Pipeline pipeline : pipelines) {
      Set<String> racks = pipeline.getNodes().stream()
          .map(DatanodeDetails::getNetworkLocation)
          .collect(Collectors.toSet());

      assertTrue(racks.size() >= 2,
          "Pipeline " + pipeline.getId()
              + " should span at least 2 racks, but spans: " + racks);
    }
  }
}
