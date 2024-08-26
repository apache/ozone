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
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.BlockDataStreamOutputEntry;
import org.apache.hadoop.ozone.client.io.KeyDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.container.TestHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests DatanodeVersion in client stream.
 */
@Timeout(120)
public class TestDatanodeVersion {
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
  private static final int DN_OLD_VERSION = DatanodeVersion.SEPARATE_RATIS_PORTS_AVAILABLE.toProtoValue();

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   */
  @BeforeAll
  public static void init() throws Exception {
    chunkSize = 100;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    conf.setFromObject(clientConfig);

    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4, StorageUnit.MB);

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
        .setNumDatanodes(3)
        .setDatanodeCurrentVersion(DN_OLD_VERSION)
        .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    volumeName = "testblockoutputstream";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  static OzoneDataStreamOutput createKey(String keyName, ReplicationType type, long size) throws Exception {
    return TestHelper.createStreamKey(keyName, type, size, objectStore, volumeName, bucketName);
  }

  @Test
  public void testStreamDatanodeVersion() throws Exception {
    // Verify all DNs internally have versions set correctly
    List<HddsDatanodeService> dns = cluster.getHddsDatanodes();
    for (HddsDatanodeService dn : dns) {
      DatanodeDetails details = dn.getDatanodeDetails();
      assertEquals(DN_OLD_VERSION, details.getCurrentVersion());
    }

    String keyName = UUID.randomUUID().toString();
    OzoneDataStreamOutput key = createKey(keyName, ReplicationType.RATIS, 0);
    KeyDataStreamOutput keyDataStreamOutput = (KeyDataStreamOutput) key.getByteBufStreamOutput();
    BlockDataStreamOutputEntry stream = keyDataStreamOutput.getStreamEntries().get(0);

    // Now check 3 DNs in a random pipeline returns the correct DN versions
    List<DatanodeDetails> streamDnDetails = stream.getPipeline().getNodes();
    for (DatanodeDetails details : streamDnDetails) {
      assertEquals(DN_OLD_VERSION, details.getCurrentVersion());
    }
  }

}
