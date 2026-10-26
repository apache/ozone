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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * When a datanode loses the replica of an open EC container (here: its volume fails), SCM must close the container,
 * so that it is no longer allocated for writes that would fail. For Ratis, the pipeline is closed by the failed write;
 * for EC, the datanode asks SCM to close the container.
 */
public class TestECContainerCloseOnVolumeFailure {

  private static final ECReplicationConfig EC = new ECReplicationConfig(3, 2);
  private static final Set<Integer> ALL_INDEXES = IntStream.rangeClosed(1, EC.getRequiredNodes())
      .boxed().collect(Collectors.toSet());

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static OzoneBucket bucket;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, 500, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, 1, TimeUnit.SECONDS);
    // close commands go out right after the container is moved to CLOSING
    conf.setTimeDuration(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, 1, TimeUnit.SECONDS);
    ReplicationManager.ReplicationManagerConfiguration rmConfig =
        conf.getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    rmConfig.setInterval(Duration.ofSeconds(1));
    rmConfig.setUnderReplicatedInterval(Duration.ofSeconds(1));
    conf.setFromObject(rmConfig);
    cluster = MiniOzoneCluster.newBuilder(conf)
        // RS(3,2) needs 5; a spare datanode keeps the reconstruction target from depending on the failed one
        .setNumDatanodes(6)
        // a datanode keeps a working volume after one fails
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder().setNumDataVolumes(2).build())
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    client.getObjectStore().createVolume("vol");
    client.getObjectStore().getVolume("vol").createBucket("bucket");
    bucket = client.getObjectStore().getVolume("vol").getBucket("bucket");
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void openEcContainerIsClosedWhenAReplicaIsLost() throws Exception {
    byte[] data = "data written before the volume failure".getBytes(UTF_8);
    writeKey("before", data);
    long containerID = bucket.getKey("before").getOzoneKeyLocations().get(0).getContainerID();
    ContainerManager containerManager = cluster.getStorageContainerManager().getContainerManager();
    assertEquals(HddsProtos.LifeCycleState.OPEN, state(containerManager, containerID));

    failVolumeOfReplica(containerManager, containerID);

    GenericTestUtils.waitFor(() -> state(containerManager, containerID) != HddsProtos.LifeCycleState.OPEN,
        200, 30_000);

    // new writes get another container, and succeed
    writeKey("after", data);
    assertNotEquals(containerID, bucket.getKey("after").getOzoneKeyLocations().get(0).getContainerID());
    // the data written before is still readable, from the surviving replicas
    assertKeyData("before", data);

    // the close completes
    GenericTestUtils.waitFor(() -> state(containerManager, containerID) == HddsProtos.LifeCycleState.CLOSED,
        200, 60_000);
    // and the lost replica is reconstructed: every index has a CLOSED replica again
    GenericTestUtils.waitFor(() -> closedReplicaIndexes(containerManager, containerID).equals(ALL_INDEXES),
        500, 120_000);
    assertKeyData("before", data);
  }

  private static void assertKeyData(String name, byte[] data) throws IOException {
    try (OzoneInputStream in = bucket.readKey(name)) {
      assertArrayEquals(data, IOUtils.toByteArray(in));
    }
  }

  private static Set<Integer> closedReplicaIndexes(ContainerManager containerManager, long containerID) {
    try {
      return containerManager.getContainerReplicas(ContainerID.valueOf(containerID)).stream()
          .filter(r -> r.getState() == ContainerReplicaProto.State.CLOSED)
          .map(ContainerReplica::getReplicaIndex)
          .collect(Collectors.toSet());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void writeKey(String name, byte[] data) throws IOException {
    try (OzoneOutputStream out = bucket.createKey(name, data.length, EC, new HashMap<>())) {
      out.write(data);
    }
  }

  private static HddsProtos.LifeCycleState state(ContainerManager containerManager, long containerID) {
    try {
      return containerManager.getContainer(ContainerID.valueOf(containerID)).getState();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Fails the volume holding the container on one of the datanodes of its pipeline, as a disk failure would. */
  private static void failVolumeOfReplica(ContainerManager containerManager, long containerID) throws Exception {
    Pipeline pipeline = cluster.getStorageContainerManager().getPipelineManager()
        .getPipeline(containerManager.getContainer(ContainerID.valueOf(containerID)).getPipelineID());
    DatanodeDetails dn = pipeline.getNodes().get(0);
    HddsDatanodeService datanode = cluster.getHddsDatanode(dn);
    OzoneContainer ozoneContainer = datanode.getDatanodeStateMachine().getContainer();
    Container<?> replica = ozoneContainer.getContainerSet().getContainer(containerID);
    assertNotNull(replica, "replica of " + containerID + " on " + dn);
    ozoneContainer.getVolumeSet().failVolume(replica.getContainerData().getVolume().getStorageDir().getPath());
    ozoneContainer.handleVolumeFailures();
  }
}
