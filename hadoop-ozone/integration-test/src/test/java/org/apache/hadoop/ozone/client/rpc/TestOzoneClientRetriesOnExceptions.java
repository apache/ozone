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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.BlockOutputStreamEntry;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Tests failure detection and handling in BlockOutputStream Class.
 */
public class TestOzoneClientRetriesOnExceptions {

  private static final int MAX_RETRIES = 3;

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private OzoneClient client;
  private ObjectStore objectStore;
  private int chunkSize;
  private int flushSize;
  private int maxFlushSize;
  private int blockSize;
  private String volumeName;
  private String bucketName;
  private String keyString;
  private XceiverClientManager xceiverClientManager;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    chunkSize = 100;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setMaxRetryCount(MAX_RETRIES);
    clientConfig.setChecksumType(ChecksumType.NONE);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 3);
    conf.setQuietMode(false);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(7)
        .setTotalPipelineNumLimit(10)
        .setBlockSize(blockSize)
        .setChunkSize(chunkSize)
        .setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .setStreamBufferSizeUnit(StorageUnit.BYTES)
        .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    xceiverClientManager = new XceiverClientManager(conf);
    keyString = UUID.randomUUID().toString();
    volumeName = "testblockoutputstreamwithretries";
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
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGroupMismatchExceptionHandling() throws Exception {
    String keyName = getKeyName();
    int dataLength = maxFlushSize + 50;
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS,
            dataLength);
    // write data more than 1 chunk
    byte[] data1 =
            ContainerTestHelper.getFixedLengthString(keyString, dataLength)
                    .getBytes(UTF_8);
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream) key.getOutputStream();
    long containerID =
            keyOutputStream.getStreamEntries().get(0).
                    getBlockID().getContainerID();
    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    ContainerInfo container =
            cluster.getStorageContainerManager().getContainerManager()
                    .getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline =
            cluster.getStorageContainerManager().getPipelineManager()
                    .getPipeline(container.getPipelineID());
    XceiverClientSpi xceiverClient =
            xceiverClientManager.acquireClient(pipeline);
    xceiverClient.sendCommand(ContainerTestHelper
            .getCreateContainerRequest(containerID, pipeline).build());
    xceiverClientManager.releaseClient(xceiverClient, false);
    key.write(data1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
            .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;
    TestHelper.waitForPipelineClose(key, cluster, false);
    key.flush();
    Assert.assertTrue(HddsClientUtils.checkForException(blockOutputStream
            .getIoException()) instanceof GroupMismatchException);
    Assert.assertTrue(keyOutputStream.getExcludeList().getPipelineIds()
            .contains(pipeline.getId()));
    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 2);
    key.close();
    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 0);
    validateData(keyName, data1);
  }

  @Test
  public void testMaxRetriesByOzoneClient() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(
        keyName, ReplicationType.RATIS, (MAX_RETRIES + 1) * blockSize);
    Assert.assertTrue(key.getOutputStream() instanceof KeyOutputStream);
    KeyOutputStream keyOutputStream = (KeyOutputStream) key.getOutputStream();
    List<BlockOutputStreamEntry> entries = keyOutputStream.getStreamEntries();
    Assert.assertEquals((MAX_RETRIES + 1),
        keyOutputStream.getStreamEntries().size());
    int dataLength = maxFlushSize + 50;
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    long containerID;
    List<Long> containerList = new ArrayList<>();
    for (BlockOutputStreamEntry entry : entries) {
      containerID = entry.getBlockID().getContainerID();
      ContainerInfo container =
          cluster.getStorageContainerManager().getContainerManager()
              .getContainer(ContainerID.valueOf(containerID));
      Pipeline pipeline =
          cluster.getStorageContainerManager().getPipelineManager()
              .getPipeline(container.getPipelineID());
      XceiverClientSpi xceiverClient =
          xceiverClientManager.acquireClient(pipeline);
      Assume.assumeFalse(containerList.contains(containerID));
      containerList.add(containerID);
      xceiverClient.sendCommand(ContainerTestHelper
          .getCreateContainerRequest(containerID, pipeline).build());
      xceiverClientManager.releaseClient(xceiverClient, false);
    }
    key.write(data1);
    OutputStream stream = entries.get(0).getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;
    TestHelper.waitForContainerClose(key, cluster);
    // Ensure that blocks for the key have been allocated to at least N+1
    // containers so that write request will be tried on N+1 different blocks
    // of N+1 different containers and it will finally fail as it will hit
    // the max retry count of N.
    Assume.assumeTrue(containerList.size() + " <= " + MAX_RETRIES,
        containerList.size() > MAX_RETRIES);
    try {
      key.write(data1);
      // ensure that write is flushed to dn
      key.flush();
      Assert.fail("Expected exception not thrown");
    } catch (IOException ioe) {
      Assert.assertTrue(HddsClientUtils.checkForException(blockOutputStream
              .getIoException()) instanceof ContainerNotOpenException);
      Assert.assertTrue(ioe.
              getMessage().contains(
              "Retry request failed. " +
                      "retries get failed due to exceeded maximum " +
                      "allowed retries number: " + MAX_RETRIES));
    }
    try {
      key.flush();
      Assert.fail("Expected exception not thrown");
    } catch (IOException ioe) {
      Assert.assertTrue(ioe.getMessage().contains("Stream is closed"));
    }
    try {
      key.close();
    } catch (IOException ioe) {
      Assert.fail("Expected should not be thrown");
    }
  }

  private OzoneOutputStream createKey(String keyName, ReplicationType type,
      long size) throws Exception {
    return TestHelper
            .createKey(keyName, type, ReplicationFactor.ONE,
                    size, objectStore, volumeName, bucketName);
  }

  private void validateData(String keyName, byte[] data) throws Exception {
    TestHelper
        .validateData(keyName, data, objectStore, volumeName, bucketName);
  }
}
