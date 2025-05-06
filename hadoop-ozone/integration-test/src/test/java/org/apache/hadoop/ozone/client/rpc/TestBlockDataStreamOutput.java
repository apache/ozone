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
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.DatanodeVersion;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.hdds.scm.storage.BlockDataStreamOutput;
import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
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
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests BlockDataStreamOutput class.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestBlockDataStreamOutput {
  private MiniOzoneCluster cluster;
  private static final int CHUNK_SIZE = 100;
  private static final int FLUSH_SIZE = 2 * CHUNK_SIZE;
  private static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;
  private static final int BLOCK_SIZE = 2 * MAX_FLUSH_SIZE;
  private static final String VOLUME_NAME = "testblockoutputstream";
  private static final String BUCKET_NAME = VOLUME_NAME;
  private static String keyString = UUID.randomUUID().toString();
  private static final DatanodeVersion DN_OLD_VERSION = DatanodeVersion.SEPARATE_RATIS_PORTS_AVAILABLE;

  static MiniOzoneCluster createCluster() throws IOException,
      InterruptedException, TimeoutException {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumType(ContainerProtos.ChecksumType.NONE);
    clientConfig.setStreamBufferFlushDelay(false);
    clientConfig.setEnablePutblockPiggybacking(true);
    conf.setFromObject(clientConfig);

    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);
    conf.setQuietMode(false);
    conf.setStorageSize(OZONE_SCM_BLOCK_SIZE, 4, StorageUnit.MB);
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 3);

    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);

    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(3));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(5));
    conf.setFromObject(raftClientConfig);

    RatisClientConfig ratisClientConfig =
        conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWriteRequestTimeout(Duration.ofSeconds(30));
    ratisClientConfig.setWatchRequestTimeout(Duration.ofSeconds(30));
    conf.setFromObject(ratisClientConfig);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .setDataStreamBufferFlushSize(MAX_FLUSH_SIZE)
        .setDataStreamMinPacketSize(CHUNK_SIZE)
        .setDataStreamWindowSize(5 * CHUNK_SIZE)
        .applyTo(conf);

    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setCurrentVersion(DN_OLD_VERSION)
            .build())
        .build();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE,
        180000);
    cluster.waitForClusterToBeReady();

    try (OzoneClient client = cluster.newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      objectStore.createVolume(VOLUME_NAME);
      objectStore.getVolume(VOLUME_NAME).createBucket(BUCKET_NAME);
    }

    return cluster;
  }

  private static Stream<Arguments> clientParameters() {
    return Stream.of(
        Arguments.of(true),
        Arguments.of(false)
    );
  }

  private static Stream<Arguments> dataLengthParameters() {
    return Stream.of(
        Arguments.of(CHUNK_SIZE / 2),
        Arguments.of(CHUNK_SIZE),
        Arguments.of(CHUNK_SIZE + 50),
        Arguments.of(BLOCK_SIZE + 50)
    );
  }

  static OzoneClientConfig newClientConfig(ConfigurationSource source,
                                           boolean flushDelay) {
    OzoneClientConfig clientConfig = source.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumType(ContainerProtos.ChecksumType.NONE);
    clientConfig.setStreamBufferFlushDelay(flushDelay);
    return clientConfig;
  }

  static OzoneClient newClient(OzoneConfiguration conf,
                               OzoneClientConfig config) throws IOException {
    OzoneConfiguration copy = new OzoneConfiguration(conf);
    copy.setFromObject(config);
    return OzoneClientFactory.getRpcClient(copy);
  }

  @BeforeAll
  public void init() throws Exception {
    cluster = createCluster();
  }

  static String getKeyName() {
    return UUID.randomUUID().toString();
  }

  @AfterAll
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @MethodSource("dataLengthParameters")
  @Flaky("HDDS-12027")
  public void testStreamWrite(int dataLength) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), false);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      testWrite(client, dataLength);
      testWriteWithFailure(client, dataLength);
    }
  }

  static void testWrite(OzoneClient client, int dataLength) throws Exception {
    String keyName = getKeyName();
    OzoneDataStreamOutput key = createKey(
        client, keyName, dataLength);
    final byte[] data = ContainerTestHelper.generateData(dataLength, false);
    key.write(ByteBuffer.wrap(data));
    // now close the stream, It will update the key length.
    key.close();
    validateData(client, keyName, data);
  }

  private void testWriteWithFailure(OzoneClient client, int dataLength) throws Exception {
    String keyName = getKeyName();
    OzoneDataStreamOutput key = createKey(
        client, keyName, dataLength);
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
    validateData(client, keyName, dataString.concat(dataString).getBytes(UTF_8));
  }

  static OzoneDataStreamOutput createKey(OzoneClient client, String keyName,
                                         long size) throws Exception {
    return TestHelper.createStreamKey(keyName, ReplicationType.RATIS, size,
        client.getObjectStore(), VOLUME_NAME, BUCKET_NAME);
  }

  static void validateData(OzoneClient client, String keyName, byte[] data) throws Exception {
    TestHelper.validateData(
        keyName, data, client.getObjectStore(), VOLUME_NAME, BUCKET_NAME);
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  public void testPutBlockAtBoundary(boolean flushDelay) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      int dataLength = 500;
      XceiverClientMetrics metrics =
          XceiverClientManager.getXceiverClientMetrics();
      long putBlockCount = metrics.getContainerOpCountMetrics(
          ContainerProtos.Type.PutBlock);
      long pendingPutBlockCount = metrics.getPendingContainerOpCountMetrics(
          ContainerProtos.Type.PutBlock);
      String keyName = getKeyName();
      OzoneDataStreamOutput key = createKey(
          client, keyName, 0);
      byte[] data =
          ContainerTestHelper.getFixedLengthString(keyString, dataLength)
              .getBytes(UTF_8);
      key.write(ByteBuffer.wrap(data));
      assertThat(metrics.getPendingContainerOpCountMetrics(ContainerProtos.Type.PutBlock))
          .isLessThanOrEqualTo(pendingPutBlockCount + 1);
      key.close();
      // Since data length is 500 , first putBlock will be at 400(flush boundary)
      // and the other at 500
      assertEquals(
          metrics.getContainerOpCountMetrics(ContainerProtos.Type.PutBlock),
          putBlockCount + 2);
      validateData(client, keyName, data);
    }
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  public void testMinPacketSize(boolean flushDelay) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      String keyName = getKeyName();
      XceiverClientMetrics metrics =
          XceiverClientManager.getXceiverClientMetrics();
      OzoneDataStreamOutput key = createKey(client, keyName, 0);
      long writeChunkCount =
          metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk);
      byte[] data =
          ContainerTestHelper.getFixedLengthString(keyString, CHUNK_SIZE / 2)
              .getBytes(UTF_8);
      key.write(ByteBuffer.wrap(data));
      // minPacketSize= 100, so first write of 50 won't trigger a writeChunk
      assertEquals(writeChunkCount,
          metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
      key.write(ByteBuffer.wrap(data));
      assertEquals(writeChunkCount + 1,
          metrics.getContainerOpCountMetrics(ContainerProtos.Type.WriteChunk));
      // now close the stream, It will update the key length.
      key.close();
      String dataString = new String(data, UTF_8);
      validateData(client, keyName, dataString.concat(dataString).getBytes(UTF_8));
    }
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  public void testTotalAckDataLength(boolean flushDelay) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      int dataLength = 400;
      String keyName = getKeyName();
      OzoneDataStreamOutput key = createKey(
          client, keyName, 0);
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
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  public void testDatanodeVersion(boolean flushDelay) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      // Verify all DNs internally have versions set correctly
      List<HddsDatanodeService> dns = cluster.getHddsDatanodes();
      for (HddsDatanodeService dn : dns) {
        DatanodeDetails details = dn.getDatanodeDetails();
        assertEquals(DN_OLD_VERSION.toProtoValue(), details.getCurrentVersion());
      }

      String keyName = getKeyName();
      OzoneDataStreamOutput key = createKey(client, keyName, 0);
      KeyDataStreamOutput keyDataStreamOutput = (KeyDataStreamOutput) key.getByteBufStreamOutput();
      BlockDataStreamOutputEntry stream = keyDataStreamOutput.getStreamEntries().get(0);

      // Now check 3 DNs in a random pipeline returns the correct DN versions
      List<DatanodeDetails> streamDnDetails = stream.getPipeline().getNodes();
      for (DatanodeDetails details : streamDnDetails) {
        assertEquals(DN_OLD_VERSION.toProtoValue(), details.getCurrentVersion());
      }
    }
  }
}
