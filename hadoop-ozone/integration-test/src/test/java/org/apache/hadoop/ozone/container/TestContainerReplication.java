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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.TestHelper.waitForContainerClose;
import static org.apache.hadoop.ozone.container.TestHelper.waitForReplicaCount;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackAware;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRandom;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;

import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.event.Level;

/**
 * Tests ozone containers replication.
 */
@RunWith(Parameterized.class)
public class TestContainerReplication {
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = Timeout.seconds(300);

  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final String KEY = "key1";

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private String placementPolicyClass;

  @Parameterized.Parameters
  public static List<String> parameters() {
    List<String> classes = new ArrayList<>();
    classes.add(SCMContainerPlacementRackAware.class.getCanonicalName());
    classes.add(SCMContainerPlacementCapacity.class.getCanonicalName());
    classes.add(SCMContainerPlacementRandom.class.getCanonicalName());
    return classes;
  }

  public TestContainerReplication(String placementPolicy) {
    this.placementPolicyClass = placementPolicy;
  }

  @Before
  public void setUp() throws Exception {
    GenericTestUtils.setLogLevel(SCMContainerPlacementRandom.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(SCMContainerPlacementCapacity.LOG,
        Level.DEBUG);
    GenericTestUtils.setLogLevel(SCMContainerPlacementRackAware.LOG,
        Level.DEBUG);
    OzoneConfiguration conf = createConfiguration();
    conf.set(OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY, placementPolicyClass);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();

    client = OzoneClientFactory.getRpcClient(conf);
    createTestData();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testContainerReplication() throws Exception {
    List<OmKeyLocationInfo> keyLocations = lookupKey(cluster);
    assertFalse(keyLocations.isEmpty());

    OmKeyLocationInfo keyLocation = keyLocations.get(0);
    long containerID = keyLocation.getContainerID();
    waitForContainerClose(cluster, containerID);

    cluster.shutdownHddsDatanode(keyLocation.getPipeline().getFirstNode());

    waitForReplicaCount(containerID, 3, cluster);
  }

  private static OzoneConfiguration createConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);

    ReplicationManagerConfiguration repConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    repConf.setInterval(Duration.ofSeconds(1));
    conf.setFromObject(repConf);
    return conf;
  }

  private void createTestData() throws IOException {
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(VOLUME);
    OzoneVolume volume = objectStore.getVolume(VOLUME);
    volume.createBucket(BUCKET);

    OzoneBucket bucket = volume.getBucket(BUCKET);
    try (OutputStream out = bucket.createKey(KEY, 0)) {
      out.write("Hello".getBytes(UTF_8));
    }
  }

  private static List<OmKeyLocationInfo> lookupKey(MiniOzoneCluster cluster)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(KEY)
        .setReplicationConfig(new RatisReplicationConfig(THREE))
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    Assert.assertNotNull(locations);
    return locations.getLocationList();
  }

}
