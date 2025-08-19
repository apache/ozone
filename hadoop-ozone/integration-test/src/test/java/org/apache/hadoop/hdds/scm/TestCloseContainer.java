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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.containerChecksumFileExists;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test to ensure a container can be closed and its replicas
 * reported back correctly after a SCM restart.
 */
public class TestCloseContainer {

  private static int numOfDatanodes = 3;
  private static String bucketName = "bucket1";
  private static String volName = "vol1";
  private OzoneBucket bucket;
  private MiniOzoneCluster cluster;
  private OzoneClient client;
  public static final Logger LOG = LoggerFactory.getLogger(TestCloseContainer.class);

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    final int interval = 100;

    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        interval, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);

    ReplicationManagerConfiguration replicationConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofSeconds(1));
    conf.setFromObject(replicationConf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numOfDatanodes)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    bucket = TestDataUtil.createVolumeAndBucket(client, volName, bucketName);
  }

  @AfterEach
  public void cleanup() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReplicasAreReportedForClosedContainerAfterRestart()
      throws Exception {
    // Create some keys to write data into the open containers
    for (int i = 0; i < 10; i++) {
      TestDataUtil.createKey(bucket, "key" + i, "this is the content".getBytes(UTF_8));
    }
    StorageContainerManager scm = cluster.getStorageContainerManager();

    // Pick any container on the cluster, get its pipeline, close it and then
    // wait for the container to close
    ContainerInfo container = scm.getContainerManager().getContainers().get(0);
    // Checksum file doesn't exist before container close
    List<HddsDatanodeService> hddsDatanodes = cluster.getHddsDatanodes();
    for (HddsDatanodeService hddsDatanode: hddsDatanodes) {
      assertFalse(containerChecksumFileExists(hddsDatanode, container.getContainerID()));
    }
    OzoneTestUtils.closeContainer(scm, container);

    // Checksum file exists after container close
    for (HddsDatanodeService hddsDatanode: hddsDatanodes) {
      GenericTestUtils.waitFor(() -> checkContainerCloseInDatanode(hddsDatanode, container), 100, 5000);
      GenericTestUtils.waitFor(() -> containerChecksumFileExists(hddsDatanode, container.getContainerID()), 100, 5000);
    }

    long originalSeq = container.getSequenceId();

    cluster.restartStorageContainerManager(true);

    scm = cluster.getStorageContainerManager();
    ContainerInfo newContainer
        = scm.getContainerManager().getContainer(container.containerID());

    // After restarting SCM, ensure the sequenceId for the container is the
    // same as before.
    assertEquals(originalSeq, newContainer.getSequenceId());

    // Ensure 3 replicas are reported successfully as expected.
    GenericTestUtils.waitFor(() ->
            getContainerReplicas(newContainer).size() == 3, 200, 30000);
    for (ContainerReplica replica : getContainerReplicas(newContainer)) {
      assertNotEquals(0, replica.getDataChecksum());
    }
  }

  /**
   * Retrieves the containerReplica set for a given container or fails the test
   * if the container cannot be found. This is a helper method to allow the
   * container replica count to be checked in a lambda expression.
   * @param c The container for which to retrieve replicas
   * @return
   */
  private Set<ContainerReplica> getContainerReplicas(ContainerInfo c) {
    return assertDoesNotThrow(() -> cluster.getStorageContainerManager()
        .getContainerManager().getContainerReplicas(c.containerID()),
        "Unexpected exception while retrieving container replicas");
  }

  @Test
  public void testCloseClosedContainer()
      throws Exception {
    // Create some keys to write data into the open containers
    for (int i = 0; i < 10; i++) {
      TestDataUtil.createKey(bucket, "key" + i, "this is the content".getBytes(UTF_8));
    }
    StorageContainerManager scm = cluster.getStorageContainerManager();
    // Pick any container on the cluster and close it via client
    ContainerInfo container = scm.getContainerManager().getContainers().get(0);
    // Checksum file doesn't exist before container close
    List<HddsDatanodeService> hddsDatanodes = cluster.getHddsDatanodes();
    for (HddsDatanodeService hddsDatanode: hddsDatanodes) {
      assertFalse(containerChecksumFileExists(hddsDatanode, container.getContainerID()));
    }
    // Close container
    OzoneTestUtils.closeContainer(scm, container);

    // Checksum file exists after container close
    for (HddsDatanodeService hddsDatanode: hddsDatanodes) {
      GenericTestUtils.waitFor(() -> checkContainerCloseInDatanode(hddsDatanode, container), 100, 5000);
      GenericTestUtils.waitFor(() -> containerChecksumFileExists(hddsDatanode, container.getContainerID()), 100, 5000);
    }

    for (ContainerReplica replica : getContainerReplicas(container)) {
      assertNotEquals(0, replica.getDataChecksum());
    }

    assertThrows(IOException.class,
        () -> cluster.getStorageContainerLocationClient()
            .closeContainer(container.getContainerID()),
        "Container " + container.getContainerID() + " already closed");
  }

  @Test
  public void testContainerChecksumForClosedContainer() throws Exception {
    // Create some keys to write data into the open containers
    ReplicationConfig repConfig = RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
    TestDataUtil.createKey(bucket, "key1", repConfig, "this is the content".getBytes(UTF_8));
    StorageContainerManager scm = cluster.getStorageContainerManager();

    ContainerInfo containerInfo1 = scm.getContainerManager().getContainers().get(0);
    // Checksum file doesn't exist before container close
    List<HddsDatanodeService> hddsDatanodes = cluster.getHddsDatanodes();
    for (HddsDatanodeService hddsDatanode : hddsDatanodes) {
      assertFalse(containerChecksumFileExists(hddsDatanode, containerInfo1.getContainerID()));
    }
    // Close container.
    OzoneTestUtils.closeContainer(scm, containerInfo1);
    ContainerProtos.ContainerChecksumInfo prevExpectedChecksumInfo1 = null;
    // Checksum file exists after container close and matches the expected container
    // merkle tree for all the datanodes
    for (HddsDatanodeService hddsDatanode : hddsDatanodes) {
      GenericTestUtils.waitFor(() -> checkContainerCloseInDatanode(hddsDatanode, containerInfo1) &&
          containerChecksumFileExists(hddsDatanode, containerInfo1.getContainerID()), 100, 5000);
      OzoneContainer ozoneContainer = hddsDatanode.getDatanodeStateMachine().getContainer();
      Container<?> container1 = ozoneContainer.getController().getContainer(containerInfo1.getContainerID());
      ContainerProtos.ContainerChecksumInfo containerChecksumInfo = ContainerMerkleTreeTestUtils.readChecksumFile(
              container1.getContainerData());
      assertNotNull(containerChecksumInfo);
      if (prevExpectedChecksumInfo1 != null) {
        ContainerMerkleTreeTestUtils.assertTreesSortedAndMatch(prevExpectedChecksumInfo1.getContainerMerkleTree(),
            containerChecksumInfo.getContainerMerkleTree());
      }
      prevExpectedChecksumInfo1 = containerChecksumInfo;
    }

    // Create 2nd container and check the checksum doesn't match with 1st container
    TestDataUtil.createKey(bucket, "key2", repConfig, "this is the different content".getBytes(UTF_8));
    ContainerInfo containerInfo2 = scm.getContainerManager().getContainers().get(1);
    for (HddsDatanodeService hddsDatanode : hddsDatanodes) {
      assertFalse(containerChecksumFileExists(hddsDatanode, containerInfo2.getContainerID()));
    }

    // Close container.
    OzoneTestUtils.closeContainer(scm, containerInfo2);
    ContainerProtos.ContainerChecksumInfo prevExpectedChecksumInfo2 = null;
    // Checksum file exists after container close and matches the expected container
    // merkle tree for all the datanodes
    for (HddsDatanodeService hddsDatanode : hddsDatanodes) {
      GenericTestUtils.waitFor(() -> checkContainerCloseInDatanode(hddsDatanode, containerInfo2) &&
          containerChecksumFileExists(hddsDatanode, containerInfo2.getContainerID()), 100, 5000);
      OzoneContainer ozoneContainer = hddsDatanode.getDatanodeStateMachine().getContainer();
      Container<?> container2 = ozoneContainer.getController().getContainer(containerInfo2.getContainerID());
      ContainerProtos.ContainerChecksumInfo containerChecksumInfo = ContainerMerkleTreeTestUtils.readChecksumFile(
          container2.getContainerData());
      assertNotNull(containerChecksumInfo);
      if (prevExpectedChecksumInfo2 != null) {
        ContainerMerkleTreeTestUtils.assertTreesSortedAndMatch(prevExpectedChecksumInfo2.getContainerMerkleTree(),
            containerChecksumInfo.getContainerMerkleTree());
      }
      prevExpectedChecksumInfo2 = containerChecksumInfo;
    }

    // Container merkle tree for different container should not match.
    assertNotEquals(prevExpectedChecksumInfo1.getContainerID(), prevExpectedChecksumInfo2.getContainerID());
    assertNotEquals(prevExpectedChecksumInfo1.getContainerMerkleTree().getDataChecksum(),
        prevExpectedChecksumInfo2.getContainerMerkleTree().getDataChecksum());

    // Wait for SCM to receive container reports with non-zero checksums for all replicas
    GenericTestUtils.waitFor(() -> getContainerReplicas(containerInfo1).stream()
            .allMatch(r -> r.getDataChecksum() != 0), 200, 5000);
    GenericTestUtils.waitFor(() -> getContainerReplicas(containerInfo2).stream()
            .allMatch(r -> r.getDataChecksum() != 0), 200, 5000);
  }

  private boolean checkContainerCloseInDatanode(HddsDatanodeService hddsDatanode,
                                                ContainerInfo containerInfo) {
    Container container = hddsDatanode.getDatanodeStateMachine().getContainer().getController()
            .getContainer(containerInfo.getContainerID());
    return container.getContainerState() == ContainerProtos.ContainerDataProto.State.CLOSED;
  }
}
