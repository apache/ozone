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
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.rules.Timeout;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_FLUSH_DELAY;

/**
 * Tests BlockOutputStream class.
 */
public class Test2BlockOutputStream {

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
    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.set(OzoneConfigKeys.OZONE_CLIENT_CHECKSUM_TYPE, "NONE");
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4,
        StorageUnit.MB);
    conf.setBoolean(OZONE_CLIENT_STREAM_BUFFER_FLUSH_DELAY, true);
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
    keyString = UUID.randomUUID().toString();
    volumeName = "testblockoutputstream";
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
  public void testFlushChunkDelay() throws Exception {
    String keyName1 = getKeyName();
    OzoneOutputStream key1 = createKey(keyName1, ReplicationType.RATIS, 0);

    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, 10)
            .getBytes(UTF_8);
    key1.write(data1);
    key1.flush();
    KeyOutputStream keyOutputStream = (KeyOutputStream)key1.getOutputStream();
    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    BlockOutputStream blockOutputStream = (BlockOutputStream) stream;

    // we have just written data(length 10) less than chunk Size,
    // at this time we call flush will not  sync data.
    Assert.assertEquals(0, blockOutputStream.getTotalDataFlushedLength());
    key1.close();
    validateData(keyName1, data1);

    String keyName2 = getKeyName();
    OzoneOutputStream key2 = createKey(keyName2, ReplicationType.RATIS, 0);
    byte[] data2 =
            ContainerTestHelper.getFixedLengthString(keyString, 110)
                    .getBytes(UTF_8);
    key2.write(data2);
    key2.flush();
    keyOutputStream = (KeyOutputStream)key2.getOutputStream();
    Assert.assertTrue(keyOutputStream.getStreamEntries().size() == 1);
    stream = keyOutputStream.getStreamEntries().get(0)
            .getOutputStream();
    Assert.assertTrue(stream instanceof BlockOutputStream);
    blockOutputStream = (BlockOutputStream) stream;

    // we have just written data(length 110) greater than chunk Size,
    // at this time we call flush will sync data.
    Assert.assertEquals(data2.length,
            blockOutputStream.getTotalDataFlushedLength());
    key2.close();
    validateData(keyName2, data2);
  }

  private OzoneOutputStream createKey(String keyName, ReplicationType type,
      long size) throws Exception {
    return TestHelper
        .createKey(keyName, type, size, objectStore, volumeName, bucketName);
  }
  private void validateData(String keyName, byte[] data) throws Exception {
    TestHelper
        .validateData(keyName, data, objectStore, volumeName, bucketName);
  }

}
