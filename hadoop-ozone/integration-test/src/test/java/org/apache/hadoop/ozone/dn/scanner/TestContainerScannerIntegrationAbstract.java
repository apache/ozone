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

package org.apache.hadoop.ozone.dn.scanner;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterAll;

/**
 * This class tests the data scanner functionality.
 */
public abstract class TestContainerScannerIntegrationAbstract {

  private static MiniOzoneCluster cluster;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  protected static final Duration SCAN_INTERVAL = Duration.ofSeconds(1);
  private static String volumeName;
  private static String bucketName;
  private static OzoneBucket bucket;
  // Log4j 2 capturer currently doesn't support capturing specific logs.
  // We must use one capturer for both the container and application logs.
  private final GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer.log4j2("");

  public static void buildCluster(OzoneConfiguration ozoneConfig)
      throws Exception {
    // Allow SCM to quickly learn about the unhealthy container.
    ozoneConfig.set(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, "1s");
    // Speed up corruption detection by allowing scans of the same container to
    // run back to back.
    ozoneConfig.setTimeDuration(
        ContainerScannerConfiguration.CONTAINER_SCAN_MIN_GAP,
        0, TimeUnit.SECONDS);

    // Build a one datanode cluster.
    cluster = MiniOzoneCluster.newBuilder(ozoneConfig).setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 30000);
    ozClient = OzoneClientFactory.getRpcClient(ozoneConfig);
    store = ozClient.getObjectStore();

    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    bucket = volume.getBucket(bucketName);
  }

  void pauseScanner() {
    getOzoneContainer().pauseContainerScrub();
  }

  void resumeScanner() {
    getOzoneContainer().resumeContainerScrub();
  }

  @AfterAll
  static void shutdown() throws IOException {
    if (ozClient != null) {
      ozClient.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  protected void waitForScmToSeeReplicaState(long containerID, State state)
      throws Exception {
    LambdaTestUtils.await(5000, 500,
        () -> getContainerReplica(containerID).getState() == state);
  }

  protected void waitForScmToCloseContainer(long containerID) throws Exception {
    ContainerManager cm = cluster.getStorageContainerManager()
        .getContainerManager();
    LambdaTestUtils.await(5000, 500,
        () -> cm.getContainer(ContainerID.valueOf(containerID)).getState()
            != HddsProtos.LifeCycleState.OPEN);
  }

  private static OzoneContainer getOzoneContainer() {
    assertEquals(1, cluster.getHddsDatanodes().size());
    HddsDatanodeService dn = cluster.getHddsDatanodes().get(0);
    return dn.getDatanodeStateMachine().getContainer();
  }

  protected Container<?> getDnContainer(long containerID) {
    return getOzoneContainer().getContainerSet().getContainer(containerID);
  }

  protected boolean containerChecksumFileExists(long containerID) {
    assertEquals(1, cluster.getHddsDatanodes().size());
    HddsDatanodeService dn = cluster.getHddsDatanodes().get(0);
    return ContainerMerkleTreeTestUtils.containerChecksumFileExists(dn, containerID);
  }

  protected long writeDataThenCloseContainer() throws Exception {
    return writeDataThenCloseContainer("keyName");
  }

  protected long writeDataThenCloseContainer(String keyName) throws Exception {
    OzoneOutputStream key = createKey(keyName);
    key.write(getTestData());
    key.flush();
    key.close();

    long containerID = bucket.getKey(keyName).getOzoneKeyLocations().stream()
        .findFirst().get().getContainerID();
    closeContainerAndWait(containerID);
    return containerID;
  }

  protected void closeContainerAndWait(long containerID) throws Exception {
    cluster.getStorageContainerLocationClient().closeContainer(containerID);

    GenericTestUtils.waitFor(
        () -> TestHelper.isContainerClosed(cluster, containerID,
            cluster.getHddsDatanodes().get(0).getDatanodeDetails()),
        1000, 5000);

    // After the container is marked as closed in the datanode, we must wait for the checksum generation from metadata
    // to finish.
    LambdaTestUtils.await(5000, 1000, () ->
            getContainerReplica(containerID).getDataChecksum() != 0);
    long closedChecksum = getContainerReplica(containerID).getDataChecksum();
    assertNotEquals(0, closedChecksum);
  }

  protected long writeDataToOpenContainer() throws Exception {
    String keyName = "keyName";
    OzoneOutputStream key = createKey(keyName);
    key.write(getTestData());
    key.close();

    return bucket.getKey(keyName).getOzoneKeyLocations().stream()
        .findFirst().get().getContainerID();
  }

  protected byte[] getTestData() {
    int sizeLargerThanOneChunk = (int) (OzoneConsts.MB + OzoneConsts.MB / 2);
    return ContainerTestHelper
        .getFixedLengthString("sample value", sizeLargerThanOneChunk)
        .getBytes(UTF_8);
  }

  protected ContainerReplica getContainerReplica(long containerId) throws ContainerNotFoundException {
    ContainerManager cm = cluster.getStorageContainerManager().getContainerManager();
    Set<ContainerReplica> containerReplicas = cm.getContainerReplicas(ContainerID.valueOf(containerId));
    // Only using a single datanode cluster.
    assertEquals(1, containerReplicas.size());
    return containerReplicas.iterator().next();
  }

  //ignore the result of the key read because it is expected to fail
  @SuppressWarnings("ResultOfMethodCallIgnored")
  protected void readFromCorruptedKey(String keyName) throws IOException {
    try (OzoneInputStream key = bucket.readKey(keyName)) {
      assertThrows(IOException.class, key::read);
    }
  }

  protected GenericTestUtils.LogCapturer getContainerLogCapturer() {
    return logCapturer;
  }

  private OzoneOutputStream createKey(String keyName) throws Exception {
    return TestHelper.createKey(
        keyName, RATIS, ONE, 0, store, volumeName, bucketName);
  }

  protected OzoneConfiguration getConf() {
    return cluster.getConf();
  }

  protected HddsDatanodeService getDatanode() {
    assertEquals(1, cluster.getHddsDatanodes().size());
    return cluster.getHddsDatanodes().get(0);
  }
}
