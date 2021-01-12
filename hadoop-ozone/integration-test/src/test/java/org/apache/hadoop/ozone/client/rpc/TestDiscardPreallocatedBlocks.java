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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.BlockOutputStreamEntry;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Tests Close Container Exception handling by Ozone Client.
 */
public class TestDiscardPreallocatedBlocks{

  /**
   * Set a timeout for each test.
   */

  @Rule
  public Timeout timeout = new Timeout(300000);
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf = new OzoneConfiguration();
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static int chunkSize;
  private static int blockSize;
  private static String volumeName;
  private static String bucketName;
  private static String keyString;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */

  @BeforeClass
  public static void init() throws Exception {
    chunkSize = (int) OzoneConsts.MB;
    blockSize = 4 * chunkSize;

    OzoneClientConfig config = new OzoneClientConfig();
    config.setChecksumType(ChecksumType.NONE);
    conf.setFromObject(config);

    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4,
        StorageUnit.MB);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = "closecontainerexceptionhandlingtest";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  private String getKeyName() {
    return UUID.randomUUID().toString();
  }

  /**
  * Shutdown MiniDFSCluster.
  */

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDiscardPreallocatedBlocks() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.RATIS, 2 * blockSize);
    KeyOutputStream keyOutputStream =
        (KeyOutputStream) key.getOutputStream();
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    // With the initial size provided, it should have pre allocated 2 blocks
    Assert.assertEquals(2, keyOutputStream.getStreamEntries().size());
    long containerID1 = keyOutputStream.getStreamEntries().get(0)
            .getBlockID().getContainerID();
    long containerID2 = keyOutputStream.getStreamEntries().get(1)
            .getBlockID().getContainerID();
    Assert.assertEquals(containerID1, containerID2);
    String dataString =
        ContainerTestHelper.getFixedLengthString(keyString, (1 * blockSize));
    byte[] data = dataString.getBytes(UTF_8);
    key.write(data);
    List<OmKeyLocationInfo> locationInfos =
        new ArrayList<>(keyOutputStream.getLocationInfoList());
    List<BlockOutputStreamEntry> locationStreamInfos =
        new ArrayList<>(keyOutputStream.getStreamEntries());
    long containerID = locationInfos.get(0).getContainerID();
    ContainerInfo container =
        cluster.getStorageContainerManager().getContainerManager()
            .getContainer(ContainerID.valueof(containerID));
    Pipeline pipeline =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipeline(container.getPipelineID());
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    Assert.assertEquals(3, datanodes.size());
    waitForContainerClose(key);
    dataString =
        ContainerTestHelper.getFixedLengthString(keyString, (1 * blockSize));
    data = dataString.getBytes(UTF_8);
    key.write(data);
    Assert.assertEquals(3, keyOutputStream.getStreamEntries().size());
    // the 1st block got written. Now all the containers are closed, so the 2nd
    // pre allocated block will be removed from the list and new block should
    // have been allocated
    Assert.assertTrue(
        keyOutputStream.getLocationInfoList().get(0).getBlockID()
            .equals(locationInfos.get(0).getBlockID()));
    Assert.assertFalse(
        locationStreamInfos.get(1).getBlockID()
            .equals(keyOutputStream.getLocationInfoList().get(1).getBlockID()));
    key.close();

  }

  private OzoneOutputStream createKey(String keyName, ReplicationType type,
      long size) throws Exception {
    return TestHelper
        .createKey(keyName, type, size, objectStore, volumeName, bucketName);
  }

  private void waitForContainerClose(OzoneOutputStream outputStream)
      throws Exception {
    TestHelper
        .waitForContainerClose(outputStream, cluster);
  }

}