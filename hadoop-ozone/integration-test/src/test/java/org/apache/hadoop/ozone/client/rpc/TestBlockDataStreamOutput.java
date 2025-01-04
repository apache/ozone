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

import org.apache.hadoop.hdds.DatanodeVersion;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.hdds.scm.storage.BlockDataStreamOutput;
import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.BlockDataStreamOutputEntry;
import org.apache.hadoop.ozone.client.io.KeyDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.PutBlock;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.WriteChunk;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_CHUNK_READ_NETTY_CHUNKED_NIO_FILE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Tests BlockDataStreamOutput class.
 */
@Timeout(300)
public class TestBlockDataStreamOutput {
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
  private static final DatanodeVersion DN_OLD_VERSION = DatanodeVersion.SEPARATE_RATIS_PORTS_AVAILABLE;

  @BeforeAll
  public static void init() throws Exception {
    chunkSize = 100;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    conf.setFromObject(clientConfig);

    conf.setBoolean(OZONE_CHUNK_READ_NETTY_CHUNKED_NIO_FILE_KEY, true);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4,
        StorageUnit.MB);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(blockSize)
        .setChunkSize(chunkSize)
        .setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .setDataStreamBufferFlushSize(maxFlushSize)
        .setDataStreamMinPacketSize(chunkSize)
        .setDataStreamWindowSize(5 * chunkSize)
        .applyTo(conf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setCurrentVersion(DN_OLD_VERSION)
            .build())
        .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = "testblockdatastreamoutput";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  static String getKeyName() {
    return UUID.randomUUID().toString();
  }

  @AfterAll
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
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    long pendingWriteChunkCount = metrics.getPendingContainerOpCountMetrics(WriteChunk);
    long pendingPutBlockCount = metrics.getPendingContainerOpCountMetrics(PutBlock);

    String keyName = getKeyName();
    OzoneDataStreamOutput key = createKey(
        keyName, ReplicationType.RATIS, dataLength);
    final byte[] data = ContainerTestHelper.generateData(dataLength, false);
    key.write(ByteBuffer.wrap(data));
    // now close the stream, It will update the key length.
    key.close();
    validateData(keyName, data);

    assertEquals(pendingPutBlockCount,
        metrics.getPendingContainerOpCountMetrics(PutBlock));
    assertEquals(pendingWriteChunkCount,
        metrics.getPendingContainerOpCountMetrics(WriteChunk));
  }

  private void testWriteWithFailure(int dataLength) throws Exception {
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    long pendingWriteChunkCount = metrics.getPendingContainerOpCountMetrics(WriteChunk);
    long pendingPutBlockCount = metrics.getPendingContainerOpCountMetrics(PutBlock);

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
    assertInstanceOf(BlockDataStreamOutput.class, stream);
    TestHelper.waitForContainerClose(key, cluster);
    key.write(b);
    key.close();
    String dataString = new String(data, UTF_8);
    validateData(keyName, dataString.concat(dataString).getBytes(UTF_8));

    assertEquals(pendingPutBlockCount,
        metrics.getPendingContainerOpCountMetrics(PutBlock));
    assertEquals(pendingWriteChunkCount,
        metrics.getPendingContainerOpCountMetrics(WriteChunk));
  }

  @Test
  public void testPutBlockAtBoundary() throws Exception {
    int dataLength = maxFlushSize + 100;
    XceiverClientMetrics metrics =
        XceiverClientManager.getXceiverClientMetrics();
    long writeChunkCount = metrics.getContainerOpCountMetrics(WriteChunk);
    long putBlockCount = metrics.getContainerOpCountMetrics(PutBlock);
    long pendingWriteChunkCount = metrics.getPendingContainerOpCountMetrics(WriteChunk);
    long pendingPutBlockCount = metrics.getPendingContainerOpCountMetrics(PutBlock);
    long totalOpCount = metrics.getTotalOpCount();

    String keyName = getKeyName();
    OzoneDataStreamOutput key = createKey(
        keyName, ReplicationType.RATIS, 0);
    byte[] data =
        ContainerTestHelper.getFixedLengthString(keyString, dataLength)
            .getBytes(UTF_8);
    key.write(ByteBuffer.wrap(data));
    assertThat(metrics.getPendingContainerOpCountMetrics(PutBlock))
        .isLessThanOrEqualTo(pendingPutBlockCount + 1);
    assertThat(metrics.getPendingContainerOpCountMetrics(WriteChunk))
        .isLessThanOrEqualTo(pendingWriteChunkCount + 5);
    key.close();
    // Since data length is 500 , first putBlock will be at 400(flush boundary)
    // and the other at 500
    assertEquals(putBlockCount + 2,
        metrics.getContainerOpCountMetrics(PutBlock));
    // Each chunk is 100 so there will be 500 / 100 = 5 chunks.
    assertEquals(writeChunkCount + 5,
        metrics.getContainerOpCountMetrics(WriteChunk));
    assertEquals(totalOpCount + 7,
        metrics.getTotalOpCount());
    assertEquals(pendingPutBlockCount,
        metrics.getPendingContainerOpCountMetrics(PutBlock));
    assertEquals(pendingWriteChunkCount,
        metrics.getPendingContainerOpCountMetrics(WriteChunk));

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
    long writeChunkCount = metrics.getContainerOpCountMetrics(WriteChunk);
    long pendingWriteChunkCount = metrics.getPendingContainerOpCountMetrics(WriteChunk);
    byte[] data =
        ContainerTestHelper.getFixedLengthString(keyString, chunkSize / 2)
            .getBytes(UTF_8);
    key.write(ByteBuffer.wrap(data));
    // minPacketSize= 100, so first write of 50 wont trigger a writeChunk
    assertEquals(writeChunkCount,
        metrics.getContainerOpCountMetrics(WriteChunk));
    key.write(ByteBuffer.wrap(data));
    assertEquals(writeChunkCount + 1,
        metrics.getContainerOpCountMetrics(WriteChunk));
    // now close the stream, It will update the key length.
    key.close();
    assertEquals(pendingWriteChunkCount,
        metrics.getPendingContainerOpCountMetrics(WriteChunk));
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
    assertEquals(dataLength, stream.getTotalAckDataLength());
  }

  @Test
  public void testDatanodeVersion() throws Exception {
    // Verify all DNs internally have versions set correctly
    List<HddsDatanodeService> dns = cluster.getHddsDatanodes();
    for (HddsDatanodeService dn : dns) {
      DatanodeDetails details = dn.getDatanodeDetails();
      assertEquals(DN_OLD_VERSION.toProtoValue(), details.getCurrentVersion());
    }

    String keyName = getKeyName();
    OzoneDataStreamOutput key = createKey(keyName, ReplicationType.RATIS, 0);
    KeyDataStreamOutput keyDataStreamOutput = (KeyDataStreamOutput) key.getByteBufStreamOutput();
    BlockDataStreamOutputEntry stream = keyDataStreamOutput.getStreamEntries().get(0);

    // Now check 3 DNs in a random pipeline returns the correct DN versions
    List<DatanodeDetails> streamDnDetails = stream.getPipeline().getNodes();
    for (DatanodeDetails details : streamDnDetails) {
      assertEquals(DN_OLD_VERSION.toProtoValue(), details.getCurrentVersion());
    }
  }

}
