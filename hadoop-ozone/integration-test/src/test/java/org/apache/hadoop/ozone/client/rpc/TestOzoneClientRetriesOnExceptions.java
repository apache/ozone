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

package org.apache.hadoop.ozone.client.rpc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
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
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RoundRobinPipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.BlockOutputStreamEntry;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests failure detection and handling in BlockOutputStream Class.
 */
public class TestOzoneClientRetriesOnExceptions {

  private static final int MAX_RETRIES = 3;

  private static final int CHUNK_SIZE = 100;
  private static final int FLUSH_SIZE = 2 * CHUNK_SIZE;
  private static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;
  private static final int BLOCK_SIZE = 2 * MAX_FLUSH_SIZE;

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private OzoneClient client;
  private ObjectStore objectStore;
  private String volumeName;
  private String bucketName;
  private String keyString;
  private XceiverClientManager xceiverClientManager;

  @BeforeEach
  public void init() throws Exception {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setMaxRetryCount(MAX_RETRIES);
    clientConfig.setChecksumType(ChecksumType.NONE);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 3);
    conf.set(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, "5s");
    conf.set("hdds.scm.pipeline.choose.policy.impl", RoundRobinPipelineChoosePolicy.class.getName());
    conf.setQuietMode(false);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .applyTo(conf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
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

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGroupMismatchExceptionHandling() throws Exception {
    String keyName = getKeyName();
    int dataLength = MAX_FLUSH_SIZE + 50;
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS,
        dataLength);
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    KeyOutputStream keyOutputStream =
        assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    long containerID =
        keyOutputStream.getStreamEntries().get(0).
            getBlockID().getContainerID();
    assertEquals(1, keyOutputStream.getStreamEntries().size());
    ContainerInfo container =
        cluster.getStorageContainerManager().getContainerManager()
            .getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipeline(container.getPipelineID());
    XceiverClientSpi xceiverClient =
        xceiverClientManager.acquireClient(pipeline);
    xceiverClient.sendCommand(ContainerTestHelper
        .getCreateContainerRequest(containerID, pipeline));
    xceiverClientManager.releaseClient(xceiverClient, false);
    key.write(data1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    BlockOutputStream blockOutputStream = assertInstanceOf(BlockOutputStream.class, stream);
    TestHelper.waitForPipelineClose(key, cluster, false);
    key.flush();
    assertInstanceOf(GroupMismatchException.class,
        HddsClientUtils.checkForException(blockOutputStream.getIoException()));
    assertThat(keyOutputStream.getExcludeList().getPipelineIds())
        .contains(pipeline.getId());
    assertEquals(2, keyOutputStream.getStreamEntries().size());
    key.close();
    assertEquals(0, keyOutputStream.getStreamEntries().size());
    validateData(keyName, data1);
  }

  @Test
  void testMaxRetriesByOzoneClient() throws Exception {
    String keyName = getKeyName();
    try (OzoneOutputStream key = createKey(
        keyName, ReplicationType.RATIS, (MAX_RETRIES + 1) * BLOCK_SIZE)) {
      KeyOutputStream keyOutputStream =
          assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
      List<BlockOutputStreamEntry> entries = keyOutputStream.getStreamEntries();
      assertEquals((MAX_RETRIES + 1),
          keyOutputStream.getStreamEntries().size());
      int dataLength = MAX_FLUSH_SIZE + 50;
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
        assertThat(containerList.contains(containerID));
        containerList.add(containerID);
        xceiverClient.sendCommand(ContainerTestHelper
            .getCreateContainerRequest(containerID, pipeline));
        xceiverClientManager.releaseClient(xceiverClient, false);
      }
      key.write(data1);
      OutputStream stream = entries.get(0).getOutputStream();
      BlockOutputStream blockOutputStream = assertInstanceOf(BlockOutputStream.class, stream);
      TestHelper.waitForContainerClose(key, cluster);
      // Ensure that blocks for the key have been allocated to at least N+1
      // containers so that write request will be tried on N+1 different blocks
      // of N+1 different containers and it will finally fail as it will hit
      // the max retry count of N.
      Assumptions.assumeTrue(containerList.size() > MAX_RETRIES,
          containerList.size() + " <= " + MAX_RETRIES);
      IOException ioe = assertThrows(IOException.class, () -> {
        key.write(data1);
        // ensure that write is flushed to dn
        key.flush();
      });
      assertInstanceOf(ContainerNotOpenException.class,
          HddsClientUtils.checkForException(blockOutputStream.getIoException()));
      assertThat(ioe.getMessage()).contains(
          "Retry request failed. " +
              "retries get failed due to exceeded maximum " +
              "allowed retries number: " + MAX_RETRIES);

      ioe = assertThrows(IOException.class, () -> key.flush());
      assertThat(ioe.getMessage()).contains("Stream is closed");
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
