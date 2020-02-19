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
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests {@link KeyInputStream}.
 */
public class TestKeyInputStream {
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
    flushSize = 4 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;
    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4,
        StorageUnit.MB);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setTotalPipelineNumLimit(5)
        .setBlockSize(blockSize)
        .setChunkSize(chunkSize)
        .setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .setStreamBufferSizeUnit(StorageUnit.BYTES)
        .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getClient(conf);
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = "test-key-input-stream-volume";
    bucketName = "test-key-input-stream-bucket";
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
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

  private String getKeyName() {
    return UUID.randomUUID().toString();
  }

  private OzoneOutputStream createKey(String keyName, ReplicationType type,
      long size) throws Exception {
    return TestHelper
        .createKey(keyName, type, size, objectStore, volumeName, bucketName);
  }


  @Test
  public void testSeekRandomly() throws Exception {
    XceiverClientMetrics metrics = XceiverClientManager
        .getXceiverClientMetrics();

    String keyName = getKeyName();
    OzoneOutputStream key = TestHelper.createKey(keyName,
        ReplicationType.RATIS, 0, objectStore, volumeName, bucketName);

    // write data of more than 2 blocks.
    int dataLength = (2 * blockSize) + (chunkSize);

    Random rd = new Random();
    byte[] inputData = new byte[dataLength];
    rd.nextBytes(inputData);
    key.write(inputData);
    key.close();


    KeyInputStream keyInputStream = (KeyInputStream) objectStore
        .getVolume(volumeName).getBucket(bucketName).readKey(keyName)
        .getInputStream();

    // Seek to some where end.
    validate(keyInputStream, inputData, dataLength-200, 100);

    // Now seek to start.
    validate(keyInputStream, inputData, 0, 140);

    validate(keyInputStream, inputData, 200, 300);

    validate(keyInputStream, inputData, 30, 500);

    randomSeek(dataLength, keyInputStream, inputData);

    // Read entire key.
    validate(keyInputStream, inputData, 0, dataLength);

    // Repeat again and check.
    randomSeek(dataLength, keyInputStream, inputData);

    validate(keyInputStream, inputData, 0, dataLength);

    keyInputStream.close();
  }

  /**
   * This method does random seeks and reads and validates the reads are
   * correct or not.
   * @param dataLength
   * @param keyInputStream
   * @param inputData
   * @throws Exception
   */
  private void randomSeek(int dataLength, KeyInputStream keyInputStream,
      byte[] inputData) throws Exception {
    // Do random seek.
    for (int i=0; i<dataLength - 300; i+=20) {
      validate(keyInputStream, inputData, i, 200);
    }

    // Seek to end and read in reverse order. And also this is partial chunks
    // as readLength is 20, chunk length is 100.
    for (int i=dataLength - 100; i>=100; i-=20) {
      validate(keyInputStream, inputData, i, 20);
    }

    // Start from begin and seek such that we read partially chunks.
    for (int i=0; i<dataLength - 300; i+=20) {
      validate(keyInputStream, inputData, i, 90);
    }

  }

  /**
   * This method seeks to specified seek value and read the data specified by
   * readLength and validate the read is correct or not.
   * @param keyInputStream
   * @param inputData
   * @param seek
   * @param readLength
   * @throws Exception
   */
  private void validate(KeyInputStream keyInputStream, byte[] inputData,
      long seek, int readLength) throws Exception {
    keyInputStream.seek(seek);

    byte[] expectedData = new byte[readLength];
    keyInputStream.read(expectedData, 0, readLength);

    byte[] dest = new byte[readLength];

    System.arraycopy(inputData, (int)seek, dest, 0, readLength);

    for (int i=0; i < readLength; i++) {
      Assert.assertEquals(expectedData[i], dest[i]);
    }
  }


  @Test
  public void testSeek() throws Exception {
    XceiverClientMetrics metrics = XceiverClientManager
        .getXceiverClientMetrics();
    long writeChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.WriteChunk);
    long readChunkCount = metrics.getContainerOpCountMetrics(
        ContainerProtos.Type.ReadChunk);

    String keyName = getKeyName();
    OzoneOutputStream key = TestHelper.createKey(keyName,
        ReplicationType.RATIS, 0, objectStore, volumeName, bucketName);

    // write data spanning 3 chunks
    int dataLength = (2 * chunkSize) + (chunkSize / 2);
    byte[] inputData = ContainerTestHelper.getFixedLengthString(
        keyString, dataLength).getBytes(UTF_8);
    key.write(inputData);
    key.close();

    Assert.assertEquals(writeChunkCount + 3,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));

    KeyInputStream keyInputStream = (KeyInputStream) objectStore
        .getVolume(volumeName).getBucket(bucketName).readKey(keyName)
        .getInputStream();

    // Seek to position 150
    keyInputStream.seek(150);

    Assert.assertEquals(150, keyInputStream.getPos());

    // Seek operation should not result in any readChunk operation.
    Assert.assertEquals(readChunkCount, metrics
        .getContainerOpsMetrics(ContainerProtos.Type.ReadChunk));

    byte[] readData = new byte[chunkSize];
    keyInputStream.read(readData, 0, chunkSize);

    // Since we reading data from index 150 to 250 and the chunk boundary is
    // 100 bytes, we need to read 2 chunks.
    Assert.assertEquals(readChunkCount + 2,
        metrics.getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk));

    keyInputStream.close();

    // Verify that the data read matches with the input data at corresponding
    // indices.
    for (int i = 0; i < chunkSize; i++) {
      Assert.assertEquals(inputData[chunkSize + 50 + i], readData[i]);
    }
  }
}
