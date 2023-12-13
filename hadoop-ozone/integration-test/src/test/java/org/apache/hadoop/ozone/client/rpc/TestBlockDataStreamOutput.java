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

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.hdds.scm.storage.BlockDataStreamOutput;
import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.BlockDataStreamOutputEntry;
import org.apache.hadoop.ozone.client.io.KeyDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.apache.ozone.test.JUnit5AwareTimeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests BlockDataStreamOutput class.
 */
public class TestBlockDataStreamOutput {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public TestRule timeout = new JUnit5AwareTimeout(Timeout.seconds(300));
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf = new OzoneConfiguration();
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static int chunkSize;
  private static int flushSize;
  private static int maxFlushSize;
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
    chunkSize = 100;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    conf.setFromObject(clientConfig);

    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4,
        StorageUnit.MB);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .setTotalPipelineNumLimit(3)
        .setBlockSize(blockSize)
        .setChunkSize(chunkSize)
        .setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .setDataStreamBufferFlushize(maxFlushSize)
        .setStreamBufferSizeUnit(StorageUnit.BYTES)
        .setDataStreamMinPacketSize(chunkSize)
        .setDataStreamStreamWindowSize(5 * chunkSize)
        .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = "testblockoutputstream";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  static String getKeyName() {
    return UUID.randomUUID().toString();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testHalfChunkWrite() throws Exception {
    testWrite(chunkSize / 2);
    testWriteWithFailure(chunkSize / 2);
  }

  @Test
  public void testSingleChunkWrite() throws Exception {
    testWrite(chunkSize);
    testWriteWithFailure(chunkSize);
  }

  @Test
  public void testMultiChunkWrite() throws Exception {
    testWrite(chunkSize + 50);
    testWriteWithFailure(chunkSize + 50);
  }

  @Test
  public void testMultiBlockWrite() throws Exception {
    testWrite(blockSize + 50);
    testWriteWithFailure(blockSize + 50);
  }

  static void testWrite(int dataLength) throws Exception {
    String keyName = getKeyName();
    OzoneDataStreamOutput key = createKey(
        keyName, ReplicationType.RATIS, dataLength);
    final byte[] data = ContainerTestHelper.generateData(dataLength, false);
    key.write(ByteBuffer.wrap(data));
    // now close the stream, It will update the key length.
    key.close();
    validateData(keyName, data);
  }

  private void testWriteWithFailure(int dataLength) throws Exception {
    String keyName = getKeyName();
    OzoneDataStreamOutput key = createKey(
        keyName, ReplicationType.RATIS, dataLength);
    byte[] data =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    ByteBuffer b = ByteBuffer.wrap(data);
    key.write(b);
    KeyDataStreamOutput keyDataStreamOutput =
        (KeyDataStreamOutput) key.getByteBufStreamOutput();
    ByteBufferStreamOutput stream =
        keyDataStreamOutput.getStreamEntries().get(0).getByteBufStreamOutput();
    Assert.assertTrue(stream instanceof BlockDataStreamOutput);
    TestHelper.waitForContainerClose(key, cluster);
    key.write(b);
    key.close();
    String dataString = new String(data, UTF_8);
    validateData(keyName, dataString.concat(dataString).getBytes(UTF_8));
  }

  @Test
  public void testPutBlockAtBoundary() throws Exception {
    int dataLength = 500;
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    long putBlockCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.PutBlock);
    long pendingPutBlockCount = metrics.getPendingContainerOpCountMetrics(
        ContainerProtos.Type.PutBlock);
    String keyName = getKeyName();
    OzoneDataStreamOutput key = createKey(
        keyName, ReplicationType.RATIS, 0);
    byte[] data =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(ByteBuffer.wrap(data));
    Assert.assertTrue(
        metrics.getPendingContainerOpCountMetrics(ContainerProtos.Type.PutBlock)
            <= pendingPutBlockCount + 1);
    key.close();
    // Since data length is 500 , first putBlock will be at 400(flush boundary)
    // and the other at 500
    Assert.assertTrue(
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock)
            == putBlockCount + 2);
    validateData(keyName, data);
  }


  static OzoneDataStreamOutput createKey(String keyName, ReplicationType type,
      long size) throws Exception {
    return TestHelper.createStreamKey(
        keyName, type, size, objectStore, volumeName, bucketName);
  }
  static void validateData(String keyName, byte[] data) throws Exception {
    TestHelper.validateData(
        keyName, data, objectStore, volumeName, bucketName);
  }


  @Test
  public void testMinPacketSize() throws Exception {
    String keyName = getKeyName();
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    OzoneDataStreamOutput key = createKey(keyName, ReplicationType.RATIS, 0);
    long writeChunkCount =
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk);
    byte[] data =
        ContainerTestHelper.getFixedLengthString(keyString, chunkSize / 2)
            .getBytes(UTF_8);
    key.write(ByteBuffer.wrap(data));
    // minPacketSize= 100, so first write of 50 wont trigger a writeChunk
    Assert.assertEquals(writeChunkCount,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    key.write(ByteBuffer.wrap(data));
    Assert.assertEquals(writeChunkCount + 1,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
    // now close the stream, It will update the key length.
    key.close();
    String dataString = new String(data, UTF_8);
    validateData(keyName, dataString.concat(dataString).getBytes(UTF_8));
  }

  @Test
  public void testTotalAckDataLength() throws Exception {
    int dataLength = 400;
    String keyName = getKeyName();
    OzoneDataStreamOutput key = createKey(
        keyName, ReplicationType.RATIS, 0);
    byte[] data =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    KeyDataStreamOutput keyDataStreamOutput =
        (KeyDataStreamOutput) key.getByteBufStreamOutput();
    BlockDataStreamOutputEntry stream =
        keyDataStreamOutput.getStreamEntries().get(0);
    key.write(ByteBuffer.wrap(data));
    key.close();
    Assert.assertEquals(dataLength, stream.getTotalAckDataLength());
  }

}
