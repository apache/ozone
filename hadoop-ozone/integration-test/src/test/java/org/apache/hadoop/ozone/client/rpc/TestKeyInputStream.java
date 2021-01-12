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
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.hdds.scm.container.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.keyvalue.ChunkLayoutTestInfo;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.TestHelper.countReplicas;
import static org.junit.Assert.fail;

import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests {@link KeyInputStream}.
 */
@RunWith(Parameterized.class)
public class TestKeyInputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestKeyInputStream.class);

  private static final int TIMEOUT = 300_000;

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
  private static ChunkLayoutTestInfo chunkLayout;

  @Parameterized.Parameters
  public static Collection<Object[]> layouts() {
    return Arrays.asList(new Object[][] {
        {ChunkLayoutTestInfo.FILE_PER_CHUNK},
        {ChunkLayoutTestInfo.FILE_PER_BLOCK}
    });
  }

  public TestKeyInputStream(ChunkLayoutTestInfo layout) {
    this.chunkLayout = layout;
  }
  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    chunkSize = 256 * 1024 * 2;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;

    OzoneClientConfig config = new OzoneClientConfig();
    config.setBytesPerChecksum(256 * 1024 * 1024);
    conf.setFromObject(config);


    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 64,
        StorageUnit.MB);
    conf.set(ScmConfigKeys.OZONE_SCM_CHUNK_LAYOUT_KEY, chunkLayout.name());

    ReplicationManagerConfiguration repConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    repConf.setInterval(Duration.ofSeconds(1));
    conf.setFromObject(repConf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(4)
        .setTotalPipelineNumLimit(5)
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
    keyString = UUID.randomUUID().toString();
    volumeName = "test-key-input-stream-volume";
    bucketName = "test-key-input-stream-bucket";
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @Rule
  public Timeout timeout = new Timeout(TIMEOUT);

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private String getKeyName() {
    return UUID.randomUUID().toString();
  }


  @Test
  public void testSeekRandomly() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = TestHelper.createKey(keyName,
        ReplicationType.RATIS, 0, objectStore, volumeName, bucketName);

    // write data of more than 2 blocks.
    int dataLength = (2 * blockSize) + (chunkSize);

    byte[] inputData = writeRandomBytes(key, dataLength);

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
    XceiverClientManager.resetXceiverClientMetrics();
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
        .getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk));

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

  @Test
  public void testReadChunk() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = TestHelper.createKey(keyName,
        ReplicationType.RATIS, 0, objectStore, volumeName, bucketName);

    // write data spanning multiple blocks/chunks
    int dataLength = 2 * blockSize + (blockSize / 2);
    byte[] data = writeRandomBytes(key, dataLength);

    // read chunk data
    try (KeyInputStream keyInputStream = (KeyInputStream) objectStore
        .getVolume(volumeName).getBucket(bucketName).readKey(keyName)
        .getInputStream()) {

      int[] bufferSizeList = {chunkSize / 4, chunkSize / 2, chunkSize - 1,
          chunkSize, chunkSize + 1, blockSize - 1, blockSize, blockSize + 1,
          blockSize * 2};
      for (int bufferSize : bufferSizeList) {
        assertReadFully(data, keyInputStream, bufferSize, 0);
        keyInputStream.seek(0);
      }
    }
  }

  @Test
  public void testSkip() throws Exception {
    XceiverClientManager.resetXceiverClientMetrics();
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

    // skip 150
    keyInputStream.skip(70);
    Assert.assertEquals(70, keyInputStream.getPos());
    keyInputStream.skip(0);
    Assert.assertEquals(70, keyInputStream.getPos());
    keyInputStream.skip(80);

    Assert.assertEquals(150, keyInputStream.getPos());

    // Skip operation should not result in any readChunk operation.
    Assert.assertEquals(readChunkCount, metrics
        .getContainerOpCountMetrics(ContainerProtos.Type.ReadChunk));

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

  @Test
  public void readAfterReplication() throws Exception {
    testReadAfterReplication(false);
  }

  @Test
  public void readAfterReplicationWithUnbuffering() throws Exception {
    testReadAfterReplication(true);
  }

  private void testReadAfterReplication(boolean doUnbuffer) throws Exception {
    Assume.assumeTrue(cluster.getHddsDatanodes().size() > 3);

    int dataLength = 2 * chunkSize;
    String keyName = getKeyName();
    OzoneOutputStream key = TestHelper.createKey(keyName,
        ReplicationType.RATIS, dataLength, objectStore, volumeName, bucketName);

    byte[] data = writeRandomBytes(key, dataLength);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.THREE)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);

    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    Assert.assertNotNull(locations);
    List<OmKeyLocationInfo> locationInfoList = locations.getLocationList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo loc = locationInfoList.get(0);
    long containerID = loc.getContainerID();
    Assert.assertEquals(3, countReplicas(containerID, cluster));

    TestHelper.waitForContainerClose(cluster, containerID);

    List<DatanodeDetails> pipelineNodes = loc.getPipeline().getNodes();

    // read chunk data
    try (KeyInputStream keyInputStream = (KeyInputStream) objectStore
        .getVolume(volumeName).getBucket(bucketName)
        .readKey(keyName).getInputStream()) {

      int b = keyInputStream.read();
      Assert.assertNotEquals(-1, b);

      if (doUnbuffer) {
        keyInputStream.unbuffer();
      }

      cluster.shutdownHddsDatanode(pipelineNodes.get(0));

      // check that we can still read it
      assertReadFully(data, keyInputStream, dataLength - 1, 1);
    }
  }

  private static void waitForNodeToBecomeDead(
      DatanodeDetails datanode) throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() ->
        HddsProtos.NodeState.DEAD == getNodeHealth(datanode),
        100, 30000);
    LOG.info("Node {} is {}", datanode.getUuidString(),
        getNodeHealth(datanode));
  }

  private static HddsProtos.NodeState getNodeHealth(DatanodeDetails dn) {
    HddsProtos.NodeState health = null;
    try {
      NodeManager nodeManager =
          cluster.getStorageContainerManager().getScmNodeManager();
      health = nodeManager.getNodeStatus(dn).getHealth();
    } catch (NodeNotFoundException e) {
      fail("Unexpected NodeNotFound exception");
    }
    return health;
  }

  private byte[] writeRandomBytes(OutputStream key, int size)
      throws IOException {
    byte[] data = new byte[size];
    Random r = new Random();
    r.nextBytes(data);
    key.write(data);
    key.close();
    return data;
  }

  private static void assertReadFully(byte[] data, InputStream in,
      int bufferSize, int totalRead) throws IOException {

    byte[] buffer = new byte[bufferSize];
    while (totalRead < data.length) {
      int numBytesRead = in.read(buffer);
      if (numBytesRead == -1 || numBytesRead == 0) {
        break;
      }
      byte[] tmp1 =
          Arrays.copyOfRange(data, totalRead, totalRead + numBytesRead);
      byte[] tmp2 =
          Arrays.copyOfRange(buffer, 0, numBytesRead);
      Assert.assertArrayEquals(tmp1, tmp2);
      totalRead += numBytesRead;
    }
    Assert.assertEquals(data.length, totalRead);
  }

}
