/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;

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
    Assert.assertNotEquals(container1.getContainerInfo().getContainerID(),
        info.getContainerID());
    Assert.assertEquals(OzoneConsts.OZONE, info.getOwner());
    Assert.assertEquals(SCMTestUtils.getReplicationType(conf),
        info.getReplicationType());
    Assert.assertEquals(SCMTestUtils.getReplicationFactor(conf),
        ReplicationConfig.getLegacyFactor(info.getReplicationConfig()));
    Assert.assertEquals(HddsProtos.LifeCycleState.OPEN, info.getState());

    // Check there are two containers in ALLOCATED state after allocation
    ContainerWithPipeline container2 = scm.getClientProtocolServer()
        .allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf), OzoneConsts.OZONE);
    Assert.assertNotEquals(container1.getContainerInfo().getContainerID(),
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
    Assert.assertNotNull(info);

    String newContainerOwner = "OZONE_NEW";
    ContainerWithPipeline container2 = scm.getClientProtocolServer()
        .allocateContainer(SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf), newContainerOwner);
    ContainerInfo info2 = containerManager
        .getMatchingContainer(OzoneConsts.GB * 3, newContainerOwner,
            container1.getPipeline());
    Assert.assertNotNull(info2);

    Assert.assertNotEquals(info.containerID(), info2.containerID());
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
    Assert.assertEquals(5, matchCount);
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
    Assert.assertEquals(5, matchCount);
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
      Assert.assertTrue(info.getContainerID() > cid);
      cid = info.getContainerID();
    }

    // At this point there are already three containers in the pipeline.
    // next container should be the same as first container
    ContainerInfo info = containerManager
        .getMatchingContainer(OzoneConsts.GB * 3, OzoneConsts.OZONE,
            container1.getPipeline());
    Assert.assertEquals(container1.getContainerInfo().getContainerID(),
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
    Assert.assertEquals(scm.getPipelineManager()
            .getNumberOfContainers(container1.getPipeline().getId()),
        numContainerPerOwnerInPipeline);
    Thread.sleep(5000);
    long threshold = 2000;
    // check the way the block allocations are distributed in the different
    // containers.
    for (Long matchedCount : container2MatchedCount.values()) {
      // TODO: #CLUTIL Look at the division of block allocations in different
      // containers.
      LOG.error("Total allocated block = " + matchedCount);
      Assert.assertTrue(matchedCount <=
          numBlockAllocates / container2MatchedCount.size() + threshold
          && matchedCount >=
          numBlockAllocates / container2MatchedCount.size() - threshold);
    }
  }

  @Test
  public void testUpdateContainerState() throws IOException,
      InvalidStateTransitionException, TimeoutException {
    Set<ContainerID> containerList = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.OPEN);
    int containers = containerList == null ? 0 : containerList.size();
    Assert.assertEquals(0, containers);

    // Allocate container1 and update its state from
    // OPEN -> CLOSING -> CLOSED -> DELETING -> DELETED
    ContainerWithPipeline container1 = scm.getClientProtocolServer()
        .allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf), OzoneConsts.OZONE);
    containerList = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.OPEN);
    Assert.assertEquals(1, containerList.size());

    containerManager
        .updateContainerState(container1.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.FINALIZE);
    containerList = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.CLOSING);
    Assert.assertEquals(1, containerList.size());

    containerManager
        .updateContainerState(container1.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.CLOSE);
    containerList = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.CLOSED);
    Assert.assertEquals(1, containerList.size());

    containerManager
        .updateContainerState(container1.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.DELETE);
    containerList = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.DELETING);
    Assert.assertEquals(1, containerList.size());

    containerManager
        .updateContainerState(container1.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.CLEANUP);
    containerList = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.DELETED);
    Assert.assertEquals(1, containerList.size());

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
    containerList = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.CLOSED);
    Assert.assertEquals(1, containerList.size());
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
    ContainerID containerID = ContainerID.valueOf(RandomUtils.nextLong());
    Set<ContainerReplica> replicaSet =
        containerStateManager.getContainerReplicas(containerID);
    Assert.assertNull(replicaSet);

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
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    replicaSet = containerStateManager.getContainerReplicas(id);
    Assert.assertEquals(2, replicaSet.size());
    Assert.assertTrue(replicaSet.contains(replicaOne));
    Assert.assertTrue(replicaSet.contains(replicaTwo));

    // Test 3: Remove one replica node and then test
    containerStateManager.removeContainerReplica(id, replicaOne);
    replicaSet = containerStateManager.getContainerReplicas(id);
    Assert.assertEquals(1, replicaSet.size());
    Assert.assertFalse(replicaSet.contains(replicaOne));
    Assert.assertTrue(replicaSet.contains(replicaTwo));

    // Test 3: Remove second replica node and then test
    containerStateManager.removeContainerReplica(id, replicaTwo);
    replicaSet = containerStateManager.getContainerReplicas(id);
    Assert.assertEquals(0, replicaSet.size());
    Assert.assertFalse(replicaSet.contains(replicaOne));
    Assert.assertFalse(replicaSet.contains(replicaTwo));

    // Test 4: Re-insert dn1
    containerStateManager.updateContainerReplica(id, replicaOne);
    replicaSet = containerStateManager.getContainerReplicas(id);
    Assert.assertEquals(1, replicaSet.size());
    Assert.assertTrue(replicaSet.contains(replicaOne));
    Assert.assertFalse(replicaSet.contains(replicaTwo));

    // Re-insert dn2
    containerStateManager.updateContainerReplica(id, replicaTwo);
    replicaSet = containerStateManager.getContainerReplicas(id);
    Assert.assertEquals(2, replicaSet.size());
    Assert.assertTrue(replicaSet.contains(replicaOne));
    Assert.assertTrue(replicaSet.contains(replicaTwo));

    // Re-insert dn1
    containerStateManager.updateContainerReplica(id, replicaOne);
    replicaSet = containerStateManager.getContainerReplicas(id);
    Assert.assertEquals(2, replicaSet.size());
    Assert.assertTrue(replicaSet.contains(replicaOne));
    Assert.assertTrue(replicaSet.contains(replicaTwo));
  }

}
