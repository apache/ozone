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

package org.apache.hadoop.ozone.container.replication;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.TestHelper.waitForContainerClose;
import static org.apache.hadoop.ozone.container.TestHelper.waitForReplicaCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;

import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests ozone containers replication.
 */
@Timeout(300)
class TestContainerReplication {

  private static final int DATANODE_COUNT = 3;
  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static OzoneBucket bucket;

  @BeforeAll
  static void setup() throws Exception {
    OzoneConfiguration conf = createConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(DATANODE_COUNT)
        .build();
    cluster.waitForClusterToBeReady();

    client = OzoneClientFactory.getRpcClient(conf);

    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(VOLUME);
    OzoneVolume volume = objectStore.getVolume(VOLUME);
    volume.createBucket(BUCKET);
    bucket = volume.getBucket(BUCKET);
  }

  @AfterAll
  static void tearDown() throws IOException {
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void testPush() throws Exception {
    OmKeyLocationInfo location = createNewClosedContainer("push");
    final long containerID = location.getContainerID();
    DatanodeDetails source = location.getPipeline().getFirstNode();
    DatanodeDetails target = selectOtherNode(source);
    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.toTarget(containerID, target);
    addDatanodeCommand(source, cmd);
    waitForReplicaCount(containerID, 2, cluster);
  }

  @Test
  void testPull() throws Exception {
    OmKeyLocationInfo location = createNewClosedContainer("pull");
    final long containerID = location.getContainerID();
    DatanodeDetails source = location.getPipeline().getFirstNode();
    DatanodeDetails target = selectOtherNode(source);
    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.fromSources(containerID,
            ImmutableList.of(source));
    addDatanodeCommand(target, cmd);
    waitForReplicaCount(containerID, 2, cluster);
  }

  private static void addDatanodeCommand(DatanodeDetails dn, SCMCommand<?> cmd)
      throws IOException {
    StateContext context = cluster.getHddsDatanode(dn).getDatanodeStateMachine()
        .getContext();
    context.getTermOfLeaderSCM().ifPresent(cmd::setTerm);
    context.addCommand(cmd);
  }

  private DatanodeDetails selectOtherNode(DatanodeDetails source)
      throws IOException {
    int sourceIndex = cluster.getHddsDatanodeIndex(source);
    int targetIndex = IntStream.range(0, DATANODE_COUNT)
        .filter(index -> index != sourceIndex)
        .findAny()
        .orElseThrow(() -> new AssertionError("no target datanode found"));
    return cluster.getHddsDatanodes().get(targetIndex).getDatanodeDetails();
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

  private static OmKeyLocationInfo createNewClosedContainer(String key)
      throws Exception {
    ReplicationConfig one = RatisReplicationConfig.getInstance(ONE);
    try (OutputStream out = bucket.createKey(key, 0, one, emptyMap())) {
      out.write("Hello".getBytes(UTF_8));
    }
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(key)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    assertNotNull(locations);
    List<OmKeyLocationInfo> locationList = locations.getLocationList();
    assertEquals(1, locationList.size());
    OmKeyLocationInfo location = locationList.iterator().next();
    waitForContainerClose(cluster, location.getContainerID());
    return location;
  }

}
