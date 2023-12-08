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
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests key output stream.
 */
abstract class AbstractTestECKeyOutputStream {
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
  private static int dataBlocks = 3;
  private static int inputSize = dataBlocks * chunkSize;
  private static byte[][] inputChunks = new byte[dataBlocks][chunkSize];

  /**
   * Create a MiniDFSCluster for testing.
   */
  protected static void init(boolean zeroCopyEnabled) throws Exception {
    chunkSize = 1024 * 1024;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumType(ContainerProtos.ChecksumType.NONE);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    // If SCM detects dead node too quickly, then container would be moved to
    // closed state and all in progress writes will get exception. To avoid
    // that, we are just keeping higher timeout and none of the tests depending
    // on deadnode detection timeout currently.
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 30, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 60, TimeUnit.SECONDS);
    conf.setTimeDuration("hdds.ratis.raft.server.rpc.slowness.timeout", 300,
        TimeUnit.SECONDS);
    conf.setTimeDuration(
        "hdds.ratis.raft.server.notification.no-leader.timeout", 300,
        TimeUnit.SECONDS);
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4,
        StorageUnit.MB);
    conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, 500,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, 1,
        TimeUnit.SECONDS);
    conf.setBoolean(OzoneConfigKeys.OZONE_EC_GRPC_ZERO_COPY_ENABLED,
        zeroCopyEnabled);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(10)
        .setTotalPipelineNumLimit(10).setBlockSize(blockSize)
        .setChunkSize(chunkSize).setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .setStreamBufferSizeUnit(StorageUnit.BYTES).build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = "testeckeyoutputstream";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    initInputChunks();
  }

  @BeforeClass
  public static void init() throws Exception {
    init(false);
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
  public void testCreateKeyWithECReplicationConfig() throws Exception {
    try (OzoneOutputStream key = TestHelper
        .createKey(keyString, new ECReplicationConfig(3, 2,
              ECReplicationConfig.EcCodec.RS, chunkSize), inputSize,
            objectStore, volumeName, bucketName)) {
      Assert.assertTrue(key.getOutputStream() instanceof ECKeyOutputStream);
    }
  }

  @Test
  public void testCreateKeyWithOutBucketDefaults() throws Exception {
    OzoneVolume volume = objectStore.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    try (OzoneOutputStream out = bucket.createKey("myKey", inputSize)) {
      Assert.assertTrue(out.getOutputStream() instanceof KeyOutputStream);
      for (int i = 0; i < inputChunks.length; i++) {
        out.write(inputChunks[i]);
      }
    }
  }

  @Test
  public void testCreateKeyWithBucketDefaults() throws Exception {
    String myBucket = UUID.randomUUID().toString();
    OzoneVolume volume = objectStore.getVolume(volumeName);
    final BucketArgs.Builder bucketArgs = BucketArgs.newBuilder();
    bucketArgs.setDefaultReplicationConfig(
        new DefaultReplicationConfig(
            new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
                chunkSize)));

    volume.createBucket(myBucket, bucketArgs.build());
    OzoneBucket bucket = volume.getBucket(myBucket);

    try (OzoneOutputStream out = bucket.createKey(keyString, inputSize)) {
      Assert.assertTrue(out.getOutputStream() instanceof ECKeyOutputStream);
      for (int i = 0; i < inputChunks.length; i++) {
        out.write(inputChunks[i]);
      }
    }
    byte[] buf = new byte[chunkSize];
    try (OzoneInputStream in = bucket.readKey(keyString)) {
      for (int i = 0; i < inputChunks.length; i++) {
        int read = in.read(buf, 0, chunkSize);
        Assert.assertEquals(chunkSize, read);
        Assert.assertTrue(Arrays.equals(buf, inputChunks[i]));
      }
    }
  }

  @Test
  public void testOverwriteECKeyWithRatisKey() throws Exception {
    String myBucket = UUID.randomUUID().toString();
    OzoneVolume volume = objectStore.getVolume(volumeName);
    final BucketArgs.Builder bucketArgs = BucketArgs.newBuilder();
    volume.createBucket(myBucket, bucketArgs.build());
    OzoneBucket bucket = volume.getBucket(myBucket);
    createKeyAndCheckReplicationConfig(keyString, bucket,
        new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
            chunkSize));

    //Overwrite with RATIS/THREE
    createKeyAndCheckReplicationConfig(keyString, bucket,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));

    //Overwrite with RATIS/ONE
    createKeyAndCheckReplicationConfig(keyString, bucket,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE));
  }

  @Test
  public void testOverwriteRatisKeyWithECKey() throws Exception {
    String myBucket = UUID.randomUUID().toString();
    OzoneVolume volume = objectStore.getVolume(volumeName);
    final BucketArgs.Builder bucketArgs = BucketArgs.newBuilder();
    volume.createBucket(myBucket, bucketArgs.build());
    OzoneBucket bucket = volume.getBucket(myBucket);

    createKeyAndCheckReplicationConfig(keyString, bucket,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    // Overwrite with EC key
    createKeyAndCheckReplicationConfig(keyString, bucket,
        new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
            chunkSize));
  }

  private void createKeyAndCheckReplicationConfig(String keyName,
      OzoneBucket bucket, ReplicationConfig replicationConfig)
      throws IOException {
    try (OzoneOutputStream out = bucket
        .createKey(keyName, inputSize, replicationConfig, new HashMap<>())) {
      for (int i = 0; i < inputChunks.length; i++) {
        out.write(inputChunks[i]);
      }
    }
    OzoneKeyDetails key = bucket.getKey(keyName);
    Assert.assertEquals(replicationConfig, key.getReplicationConfig());
  }

  @Test
  public void testCreateRatisKeyAndWithECBucketDefaults() throws Exception {
    OzoneBucket bucket = getOzoneBucket();
    try (OzoneOutputStream out = bucket.createKey(
        "testCreateRatisKeyAndWithECBucketDefaults", 2000,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        new HashMap<>())) {
      Assert.assertTrue(out.getOutputStream() instanceof KeyOutputStream);
      for (int i = 0; i < inputChunks.length; i++) {
        out.write(inputChunks[i]);
      }
    }
  }

  @Test
  public void test13ChunksInSingleWriteOp() throws IOException {
    testMultipleChunksInSingleWriteOp(13);
  }

  @Test
  public void testChunksInSingleWriteOpWithOffset() throws IOException {
    testMultipleChunksInSingleWriteOp(11, 25, 19);
  }

  @Test
  public void test15ChunksInSingleWriteOp() throws IOException {
    testMultipleChunksInSingleWriteOp(15);
  }

  @Test
  public void test20ChunksInSingleWriteOp() throws IOException {
    testMultipleChunksInSingleWriteOp(20);
  }

  @Test
  public void test21ChunksInSingleWriteOp() throws IOException {
    testMultipleChunksInSingleWriteOp(21);
  }

  private void testMultipleChunksInSingleWriteOp(int offset,
                                                int bufferChunks, int numChunks)
          throws IOException {
    byte[] inputData = getInputBytes(offset, bufferChunks, numChunks);
    final OzoneBucket bucket = getOzoneBucket();
    String keyName =
            String.format("testMultipleChunksInSingleWriteOpOffset" +
                    "%dBufferChunks%dNumChunks", offset, bufferChunks,
                    numChunks);
    try (OzoneOutputStream out = bucket.createKey(keyName, 4096,
        new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
            chunkSize), new HashMap<>())) {
      out.write(inputData, offset, numChunks * chunkSize);
    }

    validateContent(offset, numChunks * chunkSize, inputData, bucket,
            bucket.getKey(keyName));
  }

  private void testMultipleChunksInSingleWriteOp(int numChunks)
      throws IOException {
    testMultipleChunksInSingleWriteOp(0, numChunks, numChunks);
  }

  @Test
  public void testECContainerKeysCountAndNumContainerReplicas()
      throws IOException, InterruptedException, TimeoutException {
    byte[] inputData = getInputBytes(1);
    final OzoneBucket bucket = getOzoneBucket();
    ContainerOperationClient containerOperationClient =
        new ContainerOperationClient(conf);

    ECReplicationConfig repConfig = new ECReplicationConfig(
        3, 2, ECReplicationConfig.EcCodec.RS, chunkSize);
    // Close all EC pipelines so we must get a fresh pipeline and hence
    // container for this test.
    PipelineManager pm =
        cluster.getStorageContainerManager().getPipelineManager();
    for (Pipeline p : pm.getPipelines(repConfig)) {
      pm.closePipeline(p, true);
    }

    String keyName = UUID.randomUUID().toString();
    try (OzoneOutputStream out = bucket.createKey(keyName, 4096,
        repConfig, new HashMap<>())) {
      out.write(inputData);
    }
    OzoneKeyDetails key = bucket.getKey(keyName);
    long currentKeyContainerID =
        key.getOzoneKeyLocations().get(0).getContainerID();

    GenericTestUtils.waitFor(() -> {
      try {
        return (containerOperationClient.getContainer(currentKeyContainerID)
            .getNumberOfKeys() == 1) && (containerOperationClient
            .getContainerReplicas(currentKeyContainerID).size() == 5);
      } catch (IOException exception) {
        Assert.fail("Unexpected exception " + exception);
        return false;
      }
    }, 100, 10000);
    validateContent(inputData, bucket, key);
  }

  private void validateContent(byte[] inputData, OzoneBucket bucket,
                               OzoneKey key) throws IOException {
    validateContent(0, inputData.length, inputData, bucket, key);
  }

  private void validateContent(int offset, int length, byte[] inputData,
                               OzoneBucket bucket,
      OzoneKey key) throws IOException {
    try (OzoneInputStream is = bucket.readKey(key.getName())) {
      byte[] fileContent = new byte[length];
      Assert.assertEquals(length, is.read(fileContent));
      Assert.assertEquals(new String(Arrays.copyOfRange(inputData, offset,
                      offset + length), UTF_8),
          new String(fileContent, UTF_8));
    }
  }

  private OzoneBucket getOzoneBucket() throws IOException {
    String myBucket = UUID.randomUUID().toString();
    OzoneVolume volume = objectStore.getVolume(volumeName);
    final BucketArgs.Builder bucketArgs = BucketArgs.newBuilder();
    bucketArgs.setDefaultReplicationConfig(
        new DefaultReplicationConfig(
            new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
                chunkSize)));

    volume.createBucket(myBucket, bucketArgs.build());
    return volume.getBucket(myBucket);
  }

  private static void initInputChunks() {
    for (int i = 0; i < dataBlocks; i++) {
      inputChunks[i] = getBytesWith(i + 1, chunkSize);
    }
  }

  private static byte[] getBytesWith(int singleDigitNumber, int total) {
    StringBuilder builder = new StringBuilder(singleDigitNumber);
    for (int i = 1; i <= total; i++) {
      builder.append(singleDigitNumber);
    }
    return builder.toString().getBytes(UTF_8);
  }

  @Test
  public void testWriteShouldSucceedWhenDNKilled() throws Exception {
    int numChunks = 3;
    byte[] inputData = getInputBytes(numChunks);
    final OzoneBucket bucket = getOzoneBucket();
    String keyName = "testWriteShouldSucceedWhenDNKilled" + numChunks;
    DatanodeDetails nodeToKill = null;
    try {
      try (OzoneOutputStream out = bucket.createKey(keyName, 1024,
          new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
              chunkSize), new HashMap<>())) {
        ECKeyOutputStream ecOut = (ECKeyOutputStream) out.getOutputStream();
        out.write(inputData);
        // Kill a node from first pipeline
        nodeToKill = ecOut.getStreamEntries()
            .get(0).getPipeline().getFirstNode();
        cluster.shutdownHddsDatanode(nodeToKill);

        out.write(inputData);

        // Wait for flushing thread to finish its work.
        final long checkpoint = System.currentTimeMillis();
        ecOut.insertFlushCheckpoint(checkpoint);
        GenericTestUtils.waitFor(() -> ecOut.getFlushCheckpoint() == checkpoint,
            100, 10000);

        // Check the second blockGroup pipeline to make sure that the failed
        // node is not selected.
        Assert.assertFalse(ecOut.getStreamEntries()
            .get(1).getPipeline().getNodes().contains(nodeToKill));
      }

      try (OzoneInputStream is = bucket.readKey(keyName)) {
        // We wrote "inputData" twice, so do two reads and ensure the correct
        // data comes back.
        for (int i = 0; i < 2; i++) {
          byte[] fileContent = new byte[inputData.length];
          Assert.assertEquals(inputData.length, is.read(fileContent));
          Assert.assertEquals(new String(inputData, UTF_8),
              new String(fileContent, UTF_8));
        }
      }
    } finally {
      cluster.restartHddsDatanode(nodeToKill, true);
    }
  }

  private byte[] getInputBytes(int numChunks) {
    return getInputBytes(0, numChunks, numChunks);
  }

  private byte[] getInputBytes(int offset, int bufferChunks, int numChunks) {
    byte[] inputData = new byte[offset + bufferChunks * chunkSize];
    for (int i = 0; i < numChunks; i++) {
      int start = offset + (i * chunkSize);
      Arrays.fill(inputData, start, start + chunkSize - 1,
          String.valueOf(i % 9).getBytes(UTF_8)[0]);
    }
    return inputData;
  }

}
