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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for ContainerStateManager.
 */
public class TestContainerStateManagerIntegration {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerStateManagerIntegration.class);

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;
  private StorageContainerManager scm;
  private ContainerManager containerManager;
  private ContainerStateManager containerStateManager;
  private int numContainerPerOwnerInPipeline;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 1);
    numContainerPerOwnerInPipeline =
        conf.getInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
            ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();
    scm = cluster.getStorageContainerManager();
    containerManager = scm.getContainerManager();
    containerStateManager = containerManager
        .getContainerStateManager();
  }

  @AfterEach
  public void cleanUp() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAllocateContainer() throws IOException {
    // Allocate a container and verify the container info
    ContainerWithPipeline container1 = scm.getClientProtocolServer()
        .allocateContainer(SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf), OzoneConsts.OZONE);
    ContainerInfo info = containerManager
        .getMatchingContainer(OzoneConsts.GB * 3, OzoneConsts.OZONE,
            container1.getPipeline());
    assertNotEquals(container1.getContainerInfo().getContainerID(),
        info.getContainerID());
    assertEquals(OzoneConsts.OZONE, info.getOwner());
    assertEquals(SCMTestUtils.getReplicationType(conf),
        info.getReplicationType());
    assertEquals(SCMTestUtils.getReplicationFactor(conf),
        ReplicationConfig.getLegacyFactor(info.getReplicationConfig()));
    assertEquals(HddsProtos.LifeCycleState.OPEN, info.getState());

    // Check there are two containers in ALLOCATED state after allocation
    ContainerWithPipeline container2 = scm.getClientProtocolServer()
        .allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf), OzoneConsts.OZONE);
    assertNotEquals(container1.getContainerInfo().getContainerID(),
        container2.getContainerInfo().getContainerID());
  }

  @Test
  public void testAllocateContainerWithDifferentOwner() throws IOException {

    // Allocate a container and verify the container info
    ContainerWithPipeline container1 = scm.getClientProtocolServer()
        .allocateContainer(SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf), OzoneConsts.OZONE);
    ContainerInfo info = containerManager
        .getMatchingContainer(OzoneConsts.GB * 3, OzoneConsts.OZONE,
            container1.getPipeline());
    assertNotNull(info);

    String newContainerOwner = "OZONE_NEW";
    ContainerInfo info2 = containerManager
        .getMatchingContainer(OzoneConsts.GB * 3, newContainerOwner,
            container1.getPipeline());
    assertNotNull(info2);

    assertNotEquals(info.containerID(), info2.containerID());
  }

  @Test
  public void testContainerStateManagerRestart() throws IOException,
      TimeoutException, InterruptedException, AuthenticationException,
      InvalidStateTransitionException {
    // Allocate 5 containers in ALLOCATED state and 5 in CREATING state

    for (int i = 0; i < 10; i++) {

      ContainerWithPipeline container = scm.getClientProtocolServer()
          .allocateContainer(
              SCMTestUtils.getReplicationType(conf),
              SCMTestUtils.getReplicationFactor(conf), OzoneConsts.OZONE);
      if (i >= 5) {
        scm.getContainerManager().updateContainerState(container
                .getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.FINALIZE);
      }
    }

    // Restart SCM will not trigger container report to satisfy the safe mode
    // exit rule.
    cluster.restartStorageContainerManager(false);

    List<ContainerInfo> result = cluster.getStorageContainerManager()
        .getContainerManager().getContainers();

    long matchCount = result.stream()
        .filter(info ->
            info.getOwner().equals(OzoneConsts.OZONE))
        .filter(info ->
            info.getReplicationType() == SCMTestUtils.getReplicationType(conf))
        .filter(info ->
            ReplicationConfig.getLegacyFactor(info.getReplicationConfig()) ==
                SCMTestUtils.getReplicationFactor(conf))
        .filter(info ->
            info.getState() == HddsProtos.LifeCycleState.OPEN)
        .count();
    assertEquals(5, matchCount);
    matchCount = result.stream()
        .filter(info ->
            info.getOwner().equals(OzoneConsts.OZONE))
        .filter(info ->
            info.getReplicationType() == SCMTestUtils.getReplicationType(conf))
        .filter(info ->
            ReplicationConfig.getLegacyFactor(info.getReplicationConfig()) ==
                SCMTestUtils.getReplicationFactor(conf))
        .filter(info ->
            info.getState() == HddsProtos.LifeCycleState.CLOSING)
        .count();
    assertEquals(5, matchCount);
  }

  @Test
  public void testGetMatchingContainer() throws IOException {
    long cid;
    ContainerWithPipeline container1 = scm.getClientProtocolServer().
        allocateContainer(SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf), OzoneConsts.OZONE);
    cid = container1.getContainerInfo().getContainerID();

    // each getMatchingContainer call allocates a container in the
    // pipeline till the pipeline has numContainerPerOwnerInPipeline number of
    // containers.
    for (int i = 1; i < numContainerPerOwnerInPipeline; i++) {
      ContainerInfo info = containerManager
          .getMatchingContainer(OzoneConsts.GB * 3, OzoneConsts.OZONE,
              container1.getPipeline());
      assertThat(info.getContainerID()).isGreaterThan(cid);
      cid = info.getContainerID();
    }

    // At this point there are already three containers in the pipeline.
    // next container should be the same as first container
    ContainerInfo info = containerManager
        .getMatchingContainer(OzoneConsts.GB * 3, OzoneConsts.OZONE,
            container1.getPipeline());
    assertEquals(container1.getContainerInfo().getContainerID(),
        info.getContainerID());
  }

  @Test
  @Flaky("HDDS-1159")
  public void testGetMatchingContainerMultipleThreads()
      throws IOException, InterruptedException {
    ContainerWithPipeline container1 = scm.getClientProtocolServer().
        allocateContainer(SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf), OzoneConsts.OZONE);
    Map<Long, Long> container2MatchedCount = new ConcurrentHashMap<>();

    Executor executor = Executors.newFixedThreadPool(4);

    // allocate blocks using multiple threads
    int numBlockAllocates = 100000;
    for (int i = 0; i < numBlockAllocates; i++) {
      CompletableFuture.supplyAsync(() -> {
        ContainerInfo info = containerManager
            .getMatchingContainer(OzoneConsts.GB * 3, OzoneConsts.OZONE,
                container1.getPipeline());
        container2MatchedCount
            .compute(info.getContainerID(), (k, v) -> v == null ? 1L : v + 1);
        return null;
      }, executor);
    }

    // make sure pipeline has has numContainerPerOwnerInPipeline number of
    // containers.
    assertEquals(numContainerPerOwnerInPipeline, scm.getPipelineManager()
            .getNumberOfContainers(container1.getPipeline().getId()));
    Thread.sleep(5000);
    long threshold = 2000;
    // check the way the block allocations are distributed in the different
    // containers.
    for (Long matchedCount : container2MatchedCount.values()) {
      // TODO: #CLUTIL Look at the division of block allocations in different
      // containers.
      LOG.error("Total allocated block = " + matchedCount);
      assertThat(matchedCount)
          .isLessThanOrEqualTo(numBlockAllocates / container2MatchedCount.size() + threshold)
          .isGreaterThanOrEqualTo(numBlockAllocates / container2MatchedCount.size() - threshold);
    }
  }

  void assertContainerCount(LifeCycleState state, int expected) {
    final int computed = containerStateManager.getContainerCount(state);
    assertEquals(expected, computed);
  }

  @Test
  public void testUpdateContainerState() throws IOException,
      InvalidStateTransitionException {
    assertContainerCount(LifeCycleState.OPEN, 0);

    // Allocate container1 and update its state from
    // OPEN -> CLOSING -> CLOSED -> DELETING -> DELETED
    ContainerWithPipeline container1 = scm.getClientProtocolServer()
        .allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf), OzoneConsts.OZONE);
    final ContainerID id1 = container1.getContainerInfo().containerID();
    assertContainerCount(LifeCycleState.OPEN, 1);

    containerManager.updateContainerState(id1, LifeCycleEvent.FINALIZE);
    assertContainerCount(LifeCycleState.CLOSING, 1);

    containerManager.updateContainerState(id1, LifeCycleEvent.CLOSE);
    assertContainerCount(LifeCycleState.CLOSED, 1);

    containerManager.updateContainerState(id1, LifeCycleEvent.DELETE);
    assertContainerCount(LifeCycleState.DELETING, 1);

    containerManager.updateContainerState(id1, LifeCycleEvent.CLEANUP);
    assertContainerCount(LifeCycleState.DELETED, 1);

    // Allocate container1 and update its state from
    // OPEN -> CLOSING -> CLOSED
    ContainerWithPipeline container3 = scm.getClientProtocolServer()
        .allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf), OzoneConsts.OZONE);
    containerManager
        .updateContainerState(container3.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager
        .updateContainerState(container3.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.CLOSE);
    containerManager
        .updateContainerState(container1.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.CLOSE);
    assertContainerCount(LifeCycleState.CLOSED, 1);
  }

  @Test
  public void testReplicaMap() throws Exception {
    DatanodeDetails dn1 = DatanodeDetails.newBuilder().setHostName("host1")
        .setIpAddress("1.1.1.1")
        .setUuid(UUID.randomUUID()).build();
    DatanodeDetails dn2 = DatanodeDetails.newBuilder().setHostName("host2")
        .setIpAddress("2.2.2.2")
        .setUuid(UUID.randomUUID()).build();

    // Test 1: no replica's exist
    ContainerID containerID = ContainerID.valueOf(RandomUtils.secure().randomLong());
    Set<ContainerReplica> replicaSet =
        containerStateManager.getContainerReplicas(containerID);
    assertNull(replicaSet);

    ContainerWithPipeline container = scm.getClientProtocolServer()
        .allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf), OzoneConsts.OZONE);

    ContainerID id = container.getContainerInfo().containerID();

    // Test 2: Add replica nodes and then test
    ContainerReplica replicaOne = ContainerReplica.newBuilder()
        .setContainerID(id)
        .setContainerState(ContainerReplicaProto.State.OPEN)
        .setDatanodeDetails(dn1)
        .build();
    ContainerReplica replicaTwo = ContainerReplica.newBuilder()
        .setContainerID(id)
        .setContainerState(ContainerReplicaProto.State.OPEN)
        .setDatanodeDetails(dn2)
        .build();
    containerStateManager.updateContainerReplica(replicaOne);
    containerStateManager.updateContainerReplica(replicaTwo);
    replicaSet = containerStateManager.getContainerReplicas(id);
    assertEquals(2, replicaSet.size());
    assertThat(replicaSet).contains(replicaOne);
    assertThat(replicaSet).contains(replicaTwo);

    // Test 3: Remove one replica node and then test
    containerStateManager.removeContainerReplica(replicaOne);
    replicaSet = containerStateManager.getContainerReplicas(id);
    assertEquals(1, replicaSet.size());
    assertThat(replicaSet).doesNotContain(replicaOne);
    assertThat(replicaSet).contains(replicaTwo);

    // Test 3: Remove second replica node and then test
    containerStateManager.removeContainerReplica(replicaTwo);
    replicaSet = containerStateManager.getContainerReplicas(id);
    assertEquals(0, replicaSet.size());
    assertThat(replicaSet).doesNotContain(replicaOne);
    assertThat(replicaSet).doesNotContain(replicaTwo);

    // Test 4: Re-insert dn1
    containerStateManager.updateContainerReplica(replicaOne);
    replicaSet = containerStateManager.getContainerReplicas(id);
    assertEquals(1, replicaSet.size());
    assertThat(replicaSet).contains(replicaOne);
    assertThat(replicaSet).doesNotContain(replicaTwo);

    // Re-insert dn2
    containerStateManager.updateContainerReplica(replicaTwo);
    replicaSet = containerStateManager.getContainerReplicas(id);
    assertEquals(2, replicaSet.size());
    assertThat(replicaSet).contains(replicaOne);
    assertThat(replicaSet).contains(replicaTwo);

    // Re-insert dn1
    containerStateManager.updateContainerReplica(replicaOne);
    replicaSet = containerStateManager.getContainerReplicas(id);
    assertEquals(2, replicaSet.size());
    assertThat(replicaSet).contains(replicaOne);
    assertThat(replicaSet).contains(replicaTwo);
  }

}
