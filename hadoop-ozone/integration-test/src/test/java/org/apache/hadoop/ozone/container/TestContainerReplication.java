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

package org.apache.hadoop.ozone.container;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType.KeyValueContainer;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_EC_IMPL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.TestHelper.isContainerClosed;
import static org.apache.hadoop.ozone.container.TestHelper.waitForContainerClose;
import static org.apache.hadoop.ozone.container.TestHelper.waitForReplicaCount;
import static org.apache.ozone.test.GenericTestUtils.setLogLevel;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.any;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackAware;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackScatter;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRandom;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.event.Level;

/**
 * Tests ozone containers replication.
 */
class TestContainerReplication {

  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final String KEY = "key1";
  private static final List<Class<? extends PlacementPolicy>> POLICIES = asList(
      SCMContainerPlacementCapacity.class,
      SCMContainerPlacementRackAware.class,
      SCMContainerPlacementRandom.class
  );

  static List<Arguments> containerReplicationArguments() {
    List<Arguments> arguments = new LinkedList<>();
    for (Class<? extends PlacementPolicy> policyClass : POLICIES) {
      String canonicalName = policyClass.getCanonicalName();
      arguments.add(Arguments.arguments(canonicalName, true));
      arguments.add(Arguments.arguments(canonicalName, false));
    }
    return arguments;
  }

  @BeforeAll
  static void setUp() {
    setLogLevel(SCMContainerPlacementCapacity.class, Level.DEBUG);
    setLogLevel(SCMContainerPlacementRackAware.class, Level.DEBUG);
    setLogLevel(SCMContainerPlacementRandom.class, Level.DEBUG);
  }

  @ParameterizedTest
  @MethodSource("containerReplicationArguments")
  void testContainerReplication(
      String placementPolicyClass, boolean legacyEnabled) throws Exception {

    OzoneConfiguration conf = createConfiguration(legacyEnabled);
    conf.set(OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY, placementPolicyClass);
    try (MiniOzoneCluster cluster = newCluster(conf)) {
      cluster.waitForClusterToBeReady();
      SCMContainerPlacementMetrics metrics = cluster.getStorageContainerManager().getPlacementMetrics();
      try (OzoneClient client = cluster.newClient()) {
        createTestData(client);

        List<OmKeyLocationInfo> keyLocations = lookupKey(cluster);
        assertThat(keyLocations).isNotEmpty();
        long datanodeChooseAttemptCount = metrics.getDatanodeChooseAttemptCount();
        long datanodeChooseSuccessCount = metrics.getDatanodeChooseSuccessCount();
        long datanodeChooseFallbackCount = metrics.getDatanodeChooseFallbackCount();
        long datanodeRequestCount = metrics.getDatanodeRequestCount();

        OmKeyLocationInfo keyLocation = keyLocations.get(0);
        long containerID = keyLocation.getContainerID();
        waitForContainerClose(cluster, containerID);

        cluster.shutdownHddsDatanode(keyLocation.getPipeline().getFirstNode());
        waitForReplicaCount(containerID, 2, cluster);

        waitForReplicaCount(containerID, 3, cluster);

        Supplier<String> messageSupplier = () -> "policy=" + placementPolicyClass + " legacy=" + legacyEnabled;
        assertEquals(datanodeRequestCount + 1, metrics.getDatanodeRequestCount(), messageSupplier);
        assertThat(metrics.getDatanodeChooseAttemptCount()).isGreaterThan(datanodeChooseAttemptCount);
        assertEquals(datanodeChooseSuccessCount + 1, metrics.getDatanodeChooseSuccessCount(), messageSupplier);
        assertThat(metrics.getDatanodeChooseFallbackCount()).isGreaterThanOrEqualTo(datanodeChooseFallbackCount);
      }
    }
  }

  private static MiniOzoneCluster newCluster(OzoneConfiguration conf)
      throws IOException {
    return MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
  }

  private static OzoneConfiguration createConfiguration(boolean enableLegacy) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);

    ReplicationManagerConfiguration repConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    repConf.setInterval(Duration.ofSeconds(1));
    repConf.setUnderReplicatedInterval(Duration.ofSeconds(1));
    conf.setFromObject(repConf);

    return conf;
  }

  // TODO use common helper to create test data
  private void createTestData(OzoneClient client) throws IOException {
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(VOLUME);
    OzoneVolume volume = objectStore.getVolume(VOLUME);
    volume.createBucket(BUCKET);

    OzoneBucket bucket = volume.getBucket(BUCKET);

    TestDataUtil.createKey(bucket, KEY,
        RatisReplicationConfig.getInstance(THREE),
        "Hello".getBytes(UTF_8));
  }

  private byte[] createTestData(OzoneClient client, int size) throws IOException {
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(VOLUME);
    OzoneVolume volume = objectStore.getVolume(VOLUME);
    volume.createBucket(BUCKET);
    OzoneBucket bucket = volume.getBucket(BUCKET);

    byte[] b = new byte[size];
    b = RandomUtils.secure().randomBytes(b.length);
    TestDataUtil.createKey(bucket, KEY,
        new ECReplicationConfig("RS-3-2-1k"), b);
    return b;
  }

  private static List<OmKeyLocationInfo> lookupKey(MiniOzoneCluster cluster)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(KEY)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    assertNotNull(locations);
    return locations.getLocationList();
  }

  private static OmKeyLocationInfo lookupKeyFirstLocation(MiniOzoneCluster cluster)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(KEY)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    Assertions.assertNotNull(locations);
    return locations.getLocationList().get(0);
  }

  public void assertState(MiniOzoneCluster cluster, Map<Integer, DatanodeDetails> expectedReplicaMap)
      throws IOException {
    OmKeyLocationInfo keyLocation = lookupKeyFirstLocation(cluster);
    Map<Integer, DatanodeDetails> replicaMap =
        keyLocation.getPipeline().getNodes().stream().collect(Collectors.toMap(
            dn -> keyLocation.getPipeline().getReplicaIndex(dn), Functions.identity()));
    Assertions.assertEquals(expectedReplicaMap, replicaMap);
  }

  private OzoneInputStream createInputStream(OzoneClient client) throws IOException {
    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume volume = objectStore.getVolume(VOLUME);
    OzoneBucket bucket = volume.getBucket(BUCKET);
    return bucket.readKey(KEY);
  }

  private void mockContainerProtocolCalls(final MockedStatic<ContainerProtocolCalls> mockedContainerProtocolCalls,
                                          final Map<Integer, Integer> failedReadChunkCountMap) {
    mockedContainerProtocolCalls.when(() -> ContainerProtocolCalls.readChunk(any(), any(), any(), anyList(), any()))
        .thenAnswer(invocation -> {
          int replicaIndex = ((ContainerProtos.DatanodeBlockID) invocation.getArgument(2)).getReplicaIndex();
          try {
            return invocation.callRealMethod();
          } catch (Throwable e) {
            failedReadChunkCountMap.compute(replicaIndex,
                (replicaIdx, totalCount) -> totalCount == null ? 1 : (totalCount + 1));
            throw e;
          }
        });
  }

  private static void deleteContainer(MiniOzoneCluster cluster, DatanodeDetails dn, long containerId)
      throws IOException {
    OzoneContainer container = cluster.getHddsDatanode(dn).getDatanodeStateMachine().getContainer();
    Container<?> containerData = container.getContainerSet().getContainer(containerId);
    if (containerData != null) {
      container.getDispatcher().getHandler(KeyValueContainer).deleteContainer(containerData, true);
    }
    cluster.getHddsDatanode(dn).getDatanodeStateMachine().triggerHeartbeat();
  }

  @Test
  public void testImportedContainerIsClosed() throws Exception {
    OzoneConfiguration conf = createConfiguration(false);
    // create a 4 node cluster
    try (MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(4).build()) {
      cluster.waitForClusterToBeReady();

      try (OzoneClient client = OzoneClientFactory.getRpcClient(conf)) {
        List<DatanodeDetails> allNodes =
            cluster.getHddsDatanodes().stream()
                .map(HddsDatanodeService::getDatanodeDetails)
                .collect(Collectors.toList());
        // shutdown 4th node (node 3 is down now)
        cluster.shutdownHddsDatanode(allNodes.get(allNodes.size() - 1));

        createTestData(client);
        final OmKeyLocationInfo keyLocation = lookupKeyFirstLocation(cluster);
        long containerID = keyLocation.getContainerID();
        waitForContainerClose(cluster, containerID);

        // shutdown nodes 0 and 1. only node 2 is up now
        for (int i = 0; i < 2; i++) {
          cluster.shutdownHddsDatanode(allNodes.get(i));
        }
        waitForReplicaCount(containerID, 1, cluster);

        // bring back up the 4th node
        cluster.restartHddsDatanode(allNodes.get(allNodes.size() - 1), false);

        // the container should have been imported on the 4th node
        waitForReplicaCount(containerID, 2, cluster);
        assertTrue(isContainerClosed(cluster, containerID, allNodes.get(allNodes.size() - 1)));
      }
    }
  }

  @Test
  @Flaky("HDDS-11087")
  public void testECContainerReplication() throws Exception {
    OzoneConfiguration conf = createConfiguration(false);
    final Map<Integer, Integer> failedReadChunkCountMap = new ConcurrentHashMap<>();
    // Overiding Config to support 1k Chunk size
    conf.set("ozone.replication.allowed-configs", "(^((STANDALONE|RATIS)/(ONE|THREE))|(EC/(3-2|6-3|10-4)-" +
        "(512|1024|2048|4096|1)k)$)");
    conf.set(OZONE_SCM_CONTAINER_PLACEMENT_EC_IMPL_KEY, SCMContainerPlacementRackScatter.class.getCanonicalName());
    try (MockedStatic<ContainerProtocolCalls> mockedContainerProtocolCalls =
             Mockito.mockStatic(ContainerProtocolCalls.class, Mockito.CALLS_REAL_METHODS);) {
      mockContainerProtocolCalls(mockedContainerProtocolCalls, failedReadChunkCountMap);
      // Creating Cluster with 5 Nodes
      try (MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build()) {
        cluster.waitForClusterToBeReady();
        try (OzoneClient client = OzoneClientFactory.getRpcClient(conf)) {
          Set<DatanodeDetails> allNodes =
              cluster.getHddsDatanodes().stream().map(HddsDatanodeService::getDatanodeDetails).collect(
                  Collectors.toSet());
          List<DatanodeDetails> initialNodesWithData = new ArrayList<>();
          // Keeping 5 DNs and stopping the 6th Node here it is kept in the var extraNodes
          for (DatanodeDetails dn : allNodes) {
            if (initialNodesWithData.size() < 5) {
              initialNodesWithData.add(dn);
            } else {
              cluster.shutdownHddsDatanode(dn);
            }
          }

          // Creating 2 stripes with Chunk Size 1k
          int size = 6 * 1024;
          byte[] originalData = createTestData(client, size);

          // Getting latest location of the key
          final OmKeyLocationInfo keyLocation = lookupKeyFirstLocation(cluster);
          long containerID = keyLocation.getContainerID();
          waitForContainerClose(cluster, containerID);

          // Forming Replica Index Map
          Map<Integer, DatanodeDetails> replicaIndexMap =
              initialNodesWithData.stream().map(dn -> new Object[]{dn, keyLocation.getPipeline().getReplicaIndex(dn)})
                  .collect(
                      Collectors.toMap(x -> (Integer) x[1], x -> (DatanodeDetails) x[0]));

          //Reading through file and comparing with input data.
          byte[] readData = new byte[size];
          try (OzoneInputStream inputStream = createInputStream(client)) {
            IOUtils.readFully(inputStream, readData);
            Assertions.assertArrayEquals(readData, originalData);
          }
          Assertions.assertEquals(0, failedReadChunkCountMap.size());
          //Opening a new stream before we make changes to the blocks.
          try (OzoneInputStream inputStream = createInputStream(client)) {
            int firstReadLen = 1024 * 3;
            Arrays.fill(readData, (byte) 0);
            //Reading first stripe.
            IOUtils.readFully(inputStream, readData, 0, firstReadLen);
            Assertions.assertEquals(0, failedReadChunkCountMap.size());
            //Checking the initial state as per the latest location.
            assertState(cluster, ImmutableMap.of(1, replicaIndexMap.get(1), 2, replicaIndexMap.get(2),
                3, replicaIndexMap.get(3), 4, replicaIndexMap.get(4), 5, replicaIndexMap.get(5)));

            // Stopping replication manager
            cluster.getStorageContainerManager().getReplicationManager().stop();
            // Deleting the container from DN1 & DN3
            deleteContainer(cluster, replicaIndexMap.get(1), containerID);
            deleteContainer(cluster, replicaIndexMap.get(3), containerID);
            // Waiting for replica count of container to come down to 3.
            waitForReplicaCount(containerID, 3, cluster);
            // Shutting down DN1
            cluster.shutdownHddsDatanode(replicaIndexMap.get(1));
            // Starting replication manager which should process under replication & write replica 1 to DN3.
            cluster.getStorageContainerManager().getReplicationManager().start();
            waitForReplicaCount(containerID, 4, cluster);
            // Asserting Replica 1 has been written to DN3.
            assertState(cluster, ImmutableMap.of(1, replicaIndexMap.get(3), 2, replicaIndexMap.get(2),
                4, replicaIndexMap.get(4), 5, replicaIndexMap.get(5)));
            // Starting DN1.
            cluster.restartHddsDatanode(replicaIndexMap.get(1), false);
            // Waiting for underreplication to get resolved.
            waitForReplicaCount(containerID, 5, cluster);
            // Asserting Replica 1 & Replica 3 has been swapped b/w DN1 & DN3.
            assertState(cluster, ImmutableMap.of(1, replicaIndexMap.get(3), 2, replicaIndexMap.get(2),
                3, replicaIndexMap.get(1), 4, replicaIndexMap.get(4), 5, replicaIndexMap.get(5)));
            // Reading the Stripe 2 from the pre initialized inputStream
            IOUtils.readFully(inputStream, readData, firstReadLen, size - firstReadLen);
            // Asserting there was a failure in the first read chunk.
            Assertions.assertEquals(ImmutableMap.of(1, 1, 3, 1), failedReadChunkCountMap);
            Assertions.assertArrayEquals(readData, originalData);
          }
        }
      }
    }
  }

}
