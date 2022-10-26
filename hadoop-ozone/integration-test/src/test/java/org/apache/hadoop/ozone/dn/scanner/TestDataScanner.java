/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.dn.scanner;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerMetadataScanner;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;

/**
 * This class tests the data scanner functionality.
 */
public class TestDataScanner {

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConfig;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  @BeforeClass
  public static void init() throws Exception {
    ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, "1s");
    ozoneConfig.set(ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_ENABLED,
        String.valueOf(true));
    ozoneConfig.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    ozoneConfig.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION,
        false);
    cluster = MiniOzoneCluster.newBuilder(ozoneConfig).setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 30000);
    ozClient = OzoneClientFactory.getRpcClient(ozoneConfig);
    store = ozClient.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    if (ozClient != null) {
      ozClient.close();
    }
    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  //This test performs 2 separate tests because creating
  // and running a cluster is expensive.
  @Test
  public void testScannersMarkContainerUnhealthy() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String value = "sample key value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String keyNameInClosedContainer = "keyNameInClosedContainer";
    OzoneOutputStream key = createKey(volumeName, bucketName,
        keyNameInClosedContainer);
    // write data more than 1 chunk
    int sizeLargerThanOneChunk = (int) (OzoneConsts.MB + OzoneConsts.MB / 2);
    byte[] data = ContainerTestHelper
        .getFixedLengthString(value, sizeLargerThanOneChunk)
        .getBytes(UTF_8);
    key.write(data);

    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    key.flush();
    TestHelper.waitForContainerClose(key, cluster);
    key.close();
    String keyNameInOpenContainer = "keyNameInOpenContainer";
    OzoneOutputStream key2 = createKey(volumeName, bucketName,
        keyNameInOpenContainer);
    key2.write(data);
    key2.close();
    // wait for the container report to propagate to SCM
    Thread.sleep(5000);

    Assert.assertEquals(1, cluster.getHddsDatanodes().size());

    HddsDatanodeService dn = cluster.getHddsDatanodes().get(0);
    OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();
    ContainerSet containerSet = oc.getContainerSet();
    //Given an open and a closed container
    Assert.assertTrue(containerSet.containerCount() > 1);
    Container<?> openContainer = getContainerInState(containerSet, OPEN);
    Container<?> closedContainer = getContainerInState(containerSet, CLOSED);

    //When deleting their metadata to make them unhealthy and scanning them
    deleteChunksDirForContainer(openContainer);
    deleteChunksDirForContainer(closedContainer);

    ContainerScannerConfiguration conf = ozoneConfig.getObject(
        ContainerScannerConfiguration.class);
    ContainerMetadataScanner sb = new ContainerMetadataScanner(conf,
        oc.getController());
    //Scan the open container and trigger on-demand scan for the closed one
    sb.scanContainer(openContainer);
    tryReadKeyWithMissingChunksDir(bucket, keyNameInClosedContainer);
    // wait for the incremental container report to propagate to SCM
    Thread.sleep(5000);

    ContainerManager cm = cluster.getStorageContainerManager()
        .getContainerManager();
    ContainerReplica openContainerReplica = getContainerReplica(
        cm, openContainer.getContainerData().getContainerID());
    ContainerReplica closedContainerReplica = getContainerReplica(
        cm, closedContainer.getContainerData().getContainerID());
    //Then both containers are marked unhealthy
    Assert.assertEquals(State.UNHEALTHY, openContainerReplica.getState());
    Assert.assertEquals(State.UNHEALTHY, closedContainerReplica.getState());
  }

  private ContainerReplica getContainerReplica(
      ContainerManager cm, long containerId) throws ContainerNotFoundException {
    Set<ContainerReplica> containerReplicas = cm.getContainerReplicas(
        ContainerID.valueOf(
            containerId));
    Assert.assertEquals(1, containerReplicas.size());
    return containerReplicas.iterator().next();
  }

  //ignore the result of the key read because it is expected to fail
  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void tryReadKeyWithMissingChunksDir(
      OzoneBucket bucket, String keyNameInClosedContainer) throws IOException {
    try (OzoneInputStream key = bucket.readKey(keyNameInClosedContainer)) {
      Assert.assertThrows(IOException.class, key::read);
    }
  }

  private void deleteChunksDirForContainer(Container<?> container) {
    File chunksDir = new File(container.getContainerData().getContainerPath(),
        "chunks");
    deleteDirectory(chunksDir);
    Assert.assertFalse(chunksDir.exists());
  }

  private Container<?> getContainerInState(
      ContainerSet cs, ContainerProtos.ContainerDataProto.State state) {
    return cs.getContainerMap().values().stream()
        .filter(c -> state ==
            c.getContainerState())
        .findAny()
        .orElseThrow(() ->
            new RuntimeException("No Open container found for testing"));
  }

  private OzoneOutputStream createKey(String volumeName, String bucketName,
                                      String keyName) throws Exception {
    return TestHelper.createKey(
        keyName, RATIS, ONE, 0, store, volumeName, bucketName);
  }

  void deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    directoryToBeDeleted.delete();
  }
}
