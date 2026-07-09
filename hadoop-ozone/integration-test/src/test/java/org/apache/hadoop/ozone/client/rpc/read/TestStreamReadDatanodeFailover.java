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

package org.apache.hadoop.ozone.client.rpc.read;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.client.OzoneClientTestUtils.assertKeyContent;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.StreamingReadResponse;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.storage.StreamBlockInputStream;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.om.TestBucket;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Verifies that streaming block reads fail over to a healthy replica when the
 * datanode serving the stream becomes unavailable, matching legacy
 * {@code BlockInputStream} behavior.
 *
 * <p>With {@code ozone.client.stream.readblock.enable=true}, reads currently
 * do not fail over correctly when the streaming datanode stops responding.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestStreamReadDatanodeFailover {

  private static final Duration STREAM_READ_TIMEOUT = Duration.ofSeconds(3);
  private static final int PIPELINE_READY_TIMEOUT_MS = 30_000;

  private MiniOzoneCluster cluster;
  private final Set<DatanodeDetails> stoppedDatanodes = new HashSet<>();

  @BeforeAll
  void setup() throws Exception {
    cluster = newCluster();
    cluster.waitForClusterToBeReady();
  }

  @BeforeEach
  void waitForPipeline() throws Exception {
    cluster.waitForPipelineTobeReady(THREE, PIPELINE_READY_TIMEOUT_MS);
  }

  @AfterEach
  void resetDatanodes() throws Exception {
    if (stoppedDatanodes.isEmpty()) {
      return;
    }
    List<DatanodeDetails> toRestart = new ArrayList<>(stoppedDatanodes);
    stoppedDatanodes.clear();
    for (DatanodeDetails dn : toRestart) {
      cluster.restartHddsDatanode(dn, false);
    }
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(cluster);
  }

  /**
   * Legacy (Async) chunk reads succeed after stopping one pipeline datanode.
   */
  @Test
  void testAsyncReadFailoverWhenDatanodeStopped() throws Exception {
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore store = client.getObjectStore();
      OzoneBucket bucket = createBucket(store);
      String keyName = newKeyName();
      byte[] content = RandomUtils.secure().randomBytes(32 * 1024);
      TestDataUtil.createKey(bucket, keyName,
          RatisReplicationConfig.getInstance(THREE), content);

      List<DatanodeDetails> datanodes = getPipelineDatanodes(cluster, bucket, keyName);
      assertEquals(3, datanodes.size());

      stopDatanode(datanodes.get(0));
      assertKeyContent(bucket, keyName, content);

      stopDatanode(datanodes.get(1));
      assertKeyContent(bucket, keyName, content);
    }
  }

  /**
   * Streaming reads should succeed after stopping pipeline datanodes when at
   * least one healthy replica remains, same as the legacy path.
   */
  @Test
  void testStreamReadFailoverWhenDatanodesStopped() throws Exception {
    OzoneConfiguration streamConf = streamReadConfig(cluster.getConf());
    try (OzoneClient client = OzoneClientFactory.getRpcClient(streamConf)) {
      ObjectStore store = client.getObjectStore();
      OzoneBucket bucket = createBucket(store);
      String keyName = newKeyName();
      byte[] content = RandomUtils.secure().randomBytes(32 * 1024);
      TestDataUtil.createKey(bucket, keyName,
          RatisReplicationConfig.getInstance(THREE), content);

      List<DatanodeDetails> datanodes = getPipelineDatanodes(cluster, bucket, keyName);
      assertEquals(3, datanodes.size());

      // Sanity check: streaming read works with all datanodes up.
      assertKeyContent(bucket, keyName, content);

      stopDatanode(datanodes.get(0));
      assertKeyContent(bucket, keyName, content);

      stopDatanode(datanodes.get(1));
      assertKeyContent(bucket, keyName, content);
    }
  }

  /**
   * After a streaming read has started, stopping the datanode serving the
   * stream must not break the read. Legacy reads fail over and continue from
   * the current offset; streaming reads currently fail (timeout or wrong data).
   */
  @Test
  void testStreamReadFailoverAfterActiveDatanodeStopped() throws Exception {
    OzoneConfiguration streamConf = streamReadConfig(cluster.getConf());
    try (OzoneClient streamClient = OzoneClientFactory.getRpcClient(streamConf);
         OzoneClient legacyClient = cluster.newClient()) {
      TestBucket streamBucket = TestBucket.newBuilder(streamClient).build();
      OzoneBucket legacyBucket = legacyClient.getObjectStore()
          .getVolume(streamBucket.delegate().getVolumeName())
          .getBucket(streamBucket.delegate().getName());

      String keyName = newKeyName();
      byte[] content = streamBucket.writeRandomBytes(keyName, 32 * 1024);

      List<DatanodeDetails> datanodes =
          getPipelineDatanodes(cluster, streamBucket.delegate(), keyName);
      assertEquals(3, datanodes.size());

      // Legacy control: stopping one pipeline datanode mid-read still succeeds.
      try (InputStream legacyIn = legacyBucket.readKey(keyName)) {
        assertNotEquals(-1, legacyIn.read());
        stopDatanode(datanodes.get(0));
        readRemaining(legacyIn, content, 1);
      }

      // Streaming read: stop the datanode actively serving the stream.
      // This fails today without a proper streaming read failover fix.
      try (KeyInputStream streamIn = streamBucket.getKeyInputStream(keyName)) {
        assertNotEquals(-1, streamIn.read());
        StreamBlockInputStream blockStream =
            (StreamBlockInputStream) streamIn.getPartStreams().get(0);
        DatanodeDetails activeDatanode = getActiveStreamingDatanode(blockStream);
        assertTrue(datanodes.contains(activeDatanode),
            "Active streaming datanode should belong to the key pipeline");
        stopDatanode(activeDatanode);
        readRemaining(streamIn, content, 1);
      }
    }
  }

  private void stopDatanode(DatanodeDetails dn) throws IOException {
    cluster.shutdownHddsDatanode(dn);
    stoppedDatanodes.add(dn);
  }

  private static void readRemaining(InputStream in, byte[] expected, int offset)
      throws IOException {
    byte[] actual = org.apache.commons.io.IOUtils.readFully(in, expected.length - offset);
    assertArrayEquals(
        java.util.Arrays.copyOfRange(expected, offset, expected.length),
        actual);
  }

  private static DatanodeDetails getActiveStreamingDatanode(StreamBlockInputStream blockStream)
      throws ReflectiveOperationException {
    Field streamingReaderField = StreamBlockInputStream.class.getDeclaredField("streamingReader");
    streamingReaderField.setAccessible(true);
    Object streamingReader = streamingReaderField.get(blockStream);
    assertTrue(streamingReader != null, "Streaming reader should be initialized after first read");

    Method getResponse = streamingReader.getClass().getDeclaredMethod("getResponse");
    getResponse.setAccessible(true);
    StreamingReadResponse response = (StreamingReadResponse) getResponse.invoke(streamingReader);
    assertTrue(response != null, "Streaming read response should be initialized");
    return response.getDatanodeDetails();
  }

  private static OzoneConfiguration streamReadConfig(OzoneConfiguration base) {
    OzoneClientConfig clientConfig = base.getObject(OzoneClientConfig.class);
    clientConfig.setStreamReadBlock(true);
    clientConfig.setStreamReadTimeout(STREAM_READ_TIMEOUT);
    OzoneConfiguration conf = new OzoneConfiguration(base);
    conf.setFromObject(clientConfig);
    return conf;
  }

  private static MiniOzoneCluster newCluster() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 5);
    return MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
  }

  private static OzoneBucket createBucket(ObjectStore store) throws IOException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    String bucketName = UUID.randomUUID().toString();
    store.getVolume(volumeName).createBucket(bucketName);
    return store.getVolume(volumeName).getBucket(bucketName);
  }

  private static String newKeyName() {
    return "key-" + UUID.randomUUID();
  }

  private static List<DatanodeDetails> getPipelineDatanodes(MiniOzoneCluster cluster,
      OzoneBucket bucket, String keyName) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(keyName)
        .build();
    OmKeyLocationInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs)
        .getKeyLocationVersions().get(0)
        .getBlocksLatestVersionOnly().get(0);
    long containerID = keyInfo.getContainerID();

    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerInfo container = scm.getContainerManager()
        .getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline = scm.getPipelineManager()
        .getPipeline(container.getPipelineID());
    return pipeline.getNodes();
  }
}
