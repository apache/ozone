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

import com.google.common.cache.Cache;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.BlockOutputStreamEntry;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests key output stream.
 */
public class TestECKeyOutputStream {
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
  private static int parityBlocks = 2;
  private static int inputSize = dataBlocks * chunkSize;
  private static byte[][] inputChunks = new byte[dataBlocks][chunkSize];

  /**
   * Create a MiniDFSCluster for testing.
   */
  @BeforeClass
  public static void init() throws Exception {
    chunkSize = 1024;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumType(ContainerProtos.ChecksumType.NONE);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4,
        StorageUnit.MB);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(7)
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
        new DefaultReplicationConfig(ReplicationType.EC,
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
      for (int i=0; i< inputChunks.length; i++) {
        int read = in.read(buf, 0, chunkSize);
        Assert.assertEquals(chunkSize, read);
        Assert.assertTrue(Arrays.equals(buf, inputChunks[i]));
      }
    }
  }

  @Test
  public void testCreateRatisKeyAndWithECBucketDefaults() throws Exception {
    OzoneBucket bucket = getOzoneBucket();
    try (OzoneOutputStream out = bucket
        .createKey("testCreateRatisKeyAndWithECBucketDefaults", 2000,
            new RatisReplicationConfig("3"), new HashMap<>())) {
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

  public void testMultipleChunksInSingleWriteOp(int numChunks)
      throws IOException {
    byte[] inputData = new byte[numChunks * chunkSize];
    for (int i = 0; i < numChunks; i++) {
      int start = (i * chunkSize);
      Arrays.fill(inputData, start, start + chunkSize - 1,
          String.valueOf(i % 9).getBytes(UTF_8)[0]);
    }
    final OzoneBucket bucket = getOzoneBucket();
    String keyName = "testMultipleChunksInSingleWriteOp" + numChunks;
    try (OzoneOutputStream out = bucket.createKey(keyName, 4096,
        new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
            chunkSize), new HashMap<>())) {
      out.write(inputData);
    }

    validateContent(inputData, bucket, bucket.getKey(keyName));
  }

  private void validateContent(byte[] inputData, OzoneBucket bucket,
      OzoneKey key) throws IOException {
    try (OzoneInputStream is = bucket.readKey(key.getName())) {
      byte[] fileContent = new byte[inputData.length];
      Assert.assertEquals(inputData.length, is.read(fileContent));
      Assert.assertEquals(new String(inputData, UTF_8),
          new String(fileContent, UTF_8));
    }
  }

  private OzoneBucket getOzoneBucket() throws IOException {
    String myBucket = UUID.randomUUID().toString();
    OzoneVolume volume = objectStore.getVolume(volumeName);
    final BucketArgs.Builder bucketArgs = BucketArgs.newBuilder();
    bucketArgs.setDefaultReplicationConfig(
        new DefaultReplicationConfig(ReplicationType.EC,
            new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
                chunkSize)));

    volume.createBucket(myBucket, bucketArgs.build());
    return volume.getBucket(myBucket);
  }



  @Test
  // Ignoring this test, as the single block pipelines are not exposed anymore.
  // TODO: remove this before committing HDDS-5755
  @Ignore
  public void testECKeyXceiverClientShouldNotUseCachedKeysForDifferentStreams()
      throws Exception {
    int data = 3;
    int parity = 2;
    try (OzoneOutputStream key = TestHelper
        .createKey(keyString, new ECReplicationConfig(data, parity,
        ECReplicationConfig.EcCodec.RS, chunkSize), 4096,
            objectStore, volumeName, bucketName)) {
      final List<BlockOutputStreamEntry> streamEntries =
          ((ECKeyOutputStream) key.getOutputStream()).getStreamEntries();
      Assert.assertEquals(1, streamEntries.size());
      final Cache<String, XceiverClientSpi> clientCache =
          ((XceiverClientManager) ((ECKeyOutputStream) key.getOutputStream())
              .getXceiverClientFactory()).getClientCache();
      clientCache.invalidateAll();
      clientCache.cleanUp();
      final Pipeline firstStreamPipeline = streamEntries.get(0).getPipeline();
      XceiverClientSpi xceiverClientSpi =
          ((ECKeyOutputStream) key.getOutputStream()).getXceiverClientFactory()
              .acquireClient(firstStreamPipeline);
      Assert.assertNotNull(xceiverClientSpi);
      final String firstCacheKey =
          clientCache.asMap().entrySet().iterator().next().getKey();
      List<String> prevVisitedKeys = new ArrayList<>();
      prevVisitedKeys.add(firstCacheKey);
      // Lets look at all underlying EC Block group streams and make sure
      // xceiver client entry is not repeating for all.
      for (int i = 1; i < streamEntries.size(); i++) {
        Pipeline pipeline = streamEntries.get(i).getPipeline();
        Assert.assertEquals(i, clientCache.asMap().size());
        xceiverClientSpi = ((ECKeyOutputStream) key.getOutputStream())
            .getXceiverClientFactory().acquireClient(pipeline);
        Assert.assertNotNull(xceiverClientSpi);
        Assert.assertEquals(i + 1, clientCache.asMap().size());
        final String newCacheKey =
            getNewKey(clientCache.asMap().entrySet().iterator(),
                prevVisitedKeys);
        prevVisitedKeys.add(newCacheKey);
        Assert.assertNotEquals(firstCacheKey, newCacheKey);
      }
    }
  }

  private String getNewKey(
      Iterator<Map.Entry<String, XceiverClientSpi>> iterator,
      List<String> prevVisitedKeys) {
    while (iterator.hasNext()) {
      final String key = iterator.next().getKey();
      if (!prevVisitedKeys.contains(key)) {
        return key;
      }
    }
    return null;
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
}
