/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.TestHelper.waitForContainerClose;
import static org.apache.hadoop.ozone.container.TestHelper.waitForReplicaCount;
import static org.apache.ozone.test.GenericTestUtils.setLogLevel;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackAware;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRandom;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.event.Level;

/**
 * Tests ozone containers replication.
 */
@Timeout(300)
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
    setLogLevel(SCMContainerPlacementCapacity.LOG, Level.DEBUG);
    setLogLevel(SCMContainerPlacementRackAware.LOG, Level.DEBUG);
    setLogLevel(SCMContainerPlacementRandom.LOG, Level.DEBUG);
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
    repConf.setEnableLegacy(enableLegacy);
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

    try (OutputStream out = bucket.createKey(KEY, 0,
        RatisReplicationConfig.getInstance(THREE), emptyMap())) {
      out.write("Hello".getBytes(UTF_8));
    }
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

}
