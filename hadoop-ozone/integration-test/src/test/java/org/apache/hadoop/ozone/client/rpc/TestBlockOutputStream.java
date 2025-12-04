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

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.PutBlock;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.WriteChunk;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.container.TestHelper.validateData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.hdds.scm.storage.RatisBlockOutputStream;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestBlockOutputStream {

  static final int CHUNK_SIZE = 100;
  static final int FLUSH_SIZE = 2 * CHUNK_SIZE;
  static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;
  static final int BLOCK_SIZE = 2 * MAX_FLUSH_SIZE;
  static final String VOLUME = "testblockoutputstream";
  static final String BUCKET = VOLUME;

  private MiniOzoneCluster cluster;

  static MiniOzoneCluster createCluster() throws IOException,
      InterruptedException, TimeoutException {
    return createCluster(5);
  }

  static MiniOzoneCluster createCluster(int datanodes) throws IOException,
      InterruptedException, TimeoutException {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumType(ChecksumType.NONE);
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
        .applyTo(conf);

    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(datanodes)
        .build();
    cluster.waitForClusterToBeReady();

    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE,
        180000);

    try (OzoneClient client = cluster.newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      objectStore.createVolume(VOLUME);
      objectStore.getVolume(VOLUME).createBucket(BUCKET);
    }

    return cluster;
  }

  @BeforeAll
  void init() throws Exception {
    cluster = createCluster();
  }

  @AfterAll
  void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static Stream<Arguments> clientParameters() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, true),
        Arguments.of(false, false)
    );
  }

  static OzoneClientConfig newClientConfig(ConfigurationSource source,
      boolean flushDelay, boolean enablePiggybacking) {
    OzoneClientConfig clientConfig = source.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumType(ChecksumType.NONE);
    clientConfig.setStreamBufferFlushDelay(flushDelay);
    clientConfig.setEnablePutblockPiggybacking(enablePiggybacking);
    return clientConfig;
  }

  static OzoneClient newClient(OzoneConfiguration conf,
      OzoneClientConfig config) throws IOException {
    OzoneConfiguration copy = new OzoneConfiguration(conf);
    copy.setFromObject(config);
    return OzoneClientFactory.getRpcClient(copy);
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  void testWriteLessThanChunkSize(boolean flushDelay, boolean enablePiggybacking) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay, enablePiggybacking);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      XceiverClientMetrics metrics =
          XceiverClientManager.getXceiverClientMetrics();
      long writeChunkCount = metrics.getContainerOpCountMetrics(WriteChunk);
      long putBlockCount = metrics.getContainerOpCountMetrics(PutBlock);
      long pendingWriteChunkCount =
          metrics.getPendingContainerOpCountMetrics(WriteChunk);
      long pendingPutBlockCount =
          metrics.getPendingContainerOpCountMetrics(PutBlock);
      long totalOpCount = metrics.getTotalOpCount();
      String keyName = getKeyName();
      int dataLength = 50;
      final int totalWriteLength = dataLength * 2;
      byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
      KeyOutputStream keyOutputStream;
      RatisBlockOutputStream blockOutputStream;
      BufferPool bufferPool;
      try (OzoneOutputStream key = createKey(client, keyName)) {
        key.write(data1);
        keyOutputStream =
            assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

        assertEquals(1, keyOutputStream.getStreamEntries().size());
        blockOutputStream =
            assertInstanceOf(RatisBlockOutputStream.class,
                keyOutputStream.getStreamEntries().get(0).getOutputStream());

        // we have written data less than a chunk size, the data will just sit
        // in the buffer, with only one buffer being allocated in the buffer pool

        bufferPool = blockOutputStream.getBufferPool();
        assertEquals(1, bufferPool.getSize());
        //Just the writtenDataLength will be updated here
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        // no data will be flushed till now
        assertEquals(0, blockOutputStream.getTotalDataFlushedLength());
        assertEquals(0, blockOutputStream.getTotalAckDataLength());
        assertEquals(pendingWriteChunkCount,
            metrics.getPendingContainerOpCountMetrics(WriteChunk));
        assertEquals(pendingPutBlockCount,
            metrics.getPendingContainerOpCountMetrics(PutBlock));

        // commitIndex2FlushedData Map will be empty here
        assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
        // Total write data greater than or equal one chunk
        // size to make sure flush will sync data.
        key.write(data1);
        // This will flush the data and update the flush length and the map.
        key.flush();

        // flush is a sync call, all pending operations will complete
        assertEquals(pendingWriteChunkCount,
            metrics.getPendingContainerOpCountMetrics(WriteChunk));
        assertEquals(pendingPutBlockCount,
            metrics.getPendingContainerOpCountMetrics(PutBlock));
        // we have written data less than a chunk size, the data will just sit
        // in the buffer, with only one buffer being allocated in the buffer pool

        assertEquals(1, bufferPool.getSize());
        assertEquals(totalWriteLength, blockOutputStream.getWrittenDataLength());
        assertEquals(totalWriteLength,
            blockOutputStream.getTotalDataFlushedLength());
        assertEquals(0,
            blockOutputStream.getCommitIndex2flushedDataMap().size());

        // flush ensures watchForCommit updates the total length acknowledged
        assertEquals(totalWriteLength, blockOutputStream.getTotalAckDataLength());

        assertEquals(1, keyOutputStream.getStreamEntries().size());
        // now close the stream, It will update ack length after watchForCommit
      }

      assertEquals(pendingWriteChunkCount,
          metrics.getPendingContainerOpCountMetrics(WriteChunk));
      assertEquals(pendingPutBlockCount,
          metrics.getPendingContainerOpCountMetrics(PutBlock));
      assertEquals(writeChunkCount + 1,
          metrics.getContainerOpCountMetrics(WriteChunk));
      assertEquals(putBlockCount + 2,
          metrics.getContainerOpCountMetrics(PutBlock));
      assertEquals(totalOpCount + 3, metrics.getTotalOpCount());

      // make sure the bufferPool is empty
      assertEquals(0, bufferPool.computeBufferData());
      assertEquals(totalWriteLength, blockOutputStream.getTotalAckDataLength());
      assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
      assertEquals(0, keyOutputStream.getStreamEntries().size());

      validateData(keyName, data1, client.getObjectStore(), VOLUME, BUCKET);
    }
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  @Flaky("HDDS-11564")
  void testWriteExactlyFlushSize(boolean flushDelay, boolean enablePiggybacking) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay, enablePiggybacking);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      XceiverClientMetrics metrics =
          XceiverClientManager.getXceiverClientMetrics();
      final long writeChunkCount =
          metrics.getContainerOpCountMetrics(WriteChunk);
      final long putBlockCount =
          metrics.getContainerOpCountMetrics(PutBlock);
      final long pendingWriteChunkCount =
          metrics.getPendingContainerOpCountMetrics(WriteChunk);
      final long pendingPutBlockCount =
          metrics.getPendingContainerOpCountMetrics(PutBlock);
      final long totalOpCount = metrics.getTotalOpCount();

      String keyName = getKeyName();
      // write data equal to 2 chunks
      int dataLength = FLUSH_SIZE;
      byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
      KeyOutputStream keyOutputStream;
      RatisBlockOutputStream blockOutputStream;
      try (OzoneOutputStream key = createKey(client, keyName)) {
        key.write(data1);

        assertEquals(writeChunkCount + 2,
            metrics.getContainerOpCountMetrics(WriteChunk));
        assertEquals(putBlockCount + 1,
            metrics.getContainerOpCountMetrics(PutBlock));
        // The WriteChunk and PutBlock can be completed soon.
        assertThat(metrics.getPendingContainerOpCountMetrics(WriteChunk))
            .isLessThanOrEqualTo(pendingWriteChunkCount + 2);
        assertThat(metrics.getPendingContainerOpCountMetrics(PutBlock))
            .isLessThanOrEqualTo(pendingPutBlockCount + 1);

        keyOutputStream =
            assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
        assertEquals(1, keyOutputStream.getStreamEntries().size());
        blockOutputStream =
            assertInstanceOf(RatisBlockOutputStream.class,
                keyOutputStream.getStreamEntries().get(0).getOutputStream());

        // we have just written data equal flush Size = 2 chunks, at this time
        // buffer pool will have 2 buffers allocated worth of chunk size

        assertEquals(2, blockOutputStream.getBufferPool().getSize());
        // writtenDataLength as well flushedDataLength will be updated here
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
        assertEquals(0, blockOutputStream.getTotalAckDataLength());

        // Before flush, if there was no pending PutBlock which means it is complete.
        // It put a commit index into commitIndexMap.
        assertEquals((metrics.getPendingContainerOpCountMetrics(PutBlock) == pendingPutBlockCount) ? 1 : 0,
            blockOutputStream.getCommitIndex2flushedDataMap().size());

        // Now do a flush.
        key.flush();

        assertEquals(1, keyOutputStream.getStreamEntries().size());
        // The previously written data is equal to flushSize, so no action is
        // triggered when execute flush, if flushDelay is enabled.
        // If flushDelay is disabled, it will call waitOnFlushFutures to wait all
        // putBlocks finished. It was broken because WriteChunk and PutBlock
        // can be complete regardless of whether the flush executed or not.
        if (flushDelay) {
          assertThat(metrics.getPendingContainerOpCountMetrics(WriteChunk))
              .isLessThanOrEqualTo(pendingWriteChunkCount + 2);
          assertThat(metrics.getPendingContainerOpCountMetrics(PutBlock))
              .isLessThanOrEqualTo(pendingWriteChunkCount + 1);
        } else {
          assertEquals(pendingWriteChunkCount,
              metrics.getPendingContainerOpCountMetrics(WriteChunk));
          assertEquals(pendingPutBlockCount,
              metrics.getPendingContainerOpCountMetrics(PutBlock));
        }

        // Since the data in the buffer is already flushed, flush here will have
        // no impact on the counters and data structures
        assertEquals(2, blockOutputStream.getBufferPool().getSize());

        // No action is triggered when execute flush, BlockOutputStream will not
        // be updated.
        assertEquals(flushDelay ? dataLength : 0,
            blockOutputStream.getBufferPool().computeBufferData());
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());
        assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
        // If the flushDelay feature is enabled, nothing happens.
        // The assertions will be as same as those before flush.
        // If it flushed, the Commit index will be removed.
        assertEquals((flushDelay &&
                (metrics.getPendingContainerOpCountMetrics(PutBlock) == pendingPutBlockCount)) ? 1 : 0,
            blockOutputStream.getCommitIndex2flushedDataMap().size());
        assertEquals(flushDelay ? 0 : dataLength,
            blockOutputStream.getTotalAckDataLength());

        // now close the stream, It will update ack length after watchForCommit
      }

      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      // make sure the bufferPool is empty
      assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
      assertEquals(pendingWriteChunkCount,
          metrics.getPendingContainerOpCountMetrics(WriteChunk));
      assertEquals(pendingPutBlockCount,
          metrics.getPendingContainerOpCountMetrics(PutBlock));
      assertEquals(writeChunkCount + 2,
          metrics.getContainerOpCountMetrics(WriteChunk));
      assertEquals(putBlockCount + 2,
          metrics.getContainerOpCountMetrics(PutBlock));
      assertEquals(totalOpCount + 4, metrics.getTotalOpCount());
      assertEquals(0, keyOutputStream.getStreamEntries().size());

      validateData(keyName, data1, client.getObjectStore(), VOLUME, BUCKET);
    }
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  void testWriteMoreThanChunkSize(boolean flushDelay, boolean enablePiggybacking) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay, enablePiggybacking);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      XceiverClientMetrics metrics =
          XceiverClientManager.getXceiverClientMetrics();
      long writeChunkCount = metrics.getContainerOpCountMetrics(
          WriteChunk);
      long putBlockCount = metrics.getContainerOpCountMetrics(
          PutBlock);
      long pendingWriteChunkCount = metrics.getPendingContainerOpCountMetrics(
          WriteChunk);
      long pendingPutBlockCount = metrics.getPendingContainerOpCountMetrics(
          PutBlock);
      long totalOpCount = metrics.getTotalOpCount();
      String keyName = getKeyName();
      // write data more than 1 chunk
      int dataLength = CHUNK_SIZE + 50;
      byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
      KeyOutputStream keyOutputStream;
      RatisBlockOutputStream blockOutputStream;
      BufferPool bufferPool;
      try (OzoneOutputStream key = createKey(client, keyName)) {
        key.write(data1);
        assertEquals(totalOpCount + 1, metrics.getTotalOpCount());
        keyOutputStream =
            assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

        assertEquals(1, keyOutputStream.getStreamEntries().size());
        blockOutputStream =
            assertInstanceOf(RatisBlockOutputStream.class,
                keyOutputStream.getStreamEntries().get(0).getOutputStream());

        // we have just written data equal flush Size > 1 chunk, at this time
        // buffer pool will have 2 buffers allocated worth of chunk size

        bufferPool = blockOutputStream.getBufferPool();
        assertEquals(2, bufferPool.getSize());
        // writtenDataLength as well flushedDataLength will be updated here
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        // since data written is still less than flushLength, flushLength will
        // still be 0.
        assertEquals(0, blockOutputStream.getTotalDataFlushedLength());
        assertEquals(0, blockOutputStream.getTotalAckDataLength());

        assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());

        // This will flush the data and update the flush length and the map.
        key.flush();
        assertEquals(writeChunkCount + 2,
            metrics.getContainerOpCountMetrics(WriteChunk));
        assertEquals(putBlockCount + ((enablePiggybacking) ? 0 : 1),
            metrics.getContainerOpCountMetrics(PutBlock));
        assertEquals(pendingWriteChunkCount,
            metrics.getPendingContainerOpCountMetrics(WriteChunk));
        assertEquals(pendingPutBlockCount,
            metrics.getPendingContainerOpCountMetrics(PutBlock));

        assertEquals(2, bufferPool.getSize());
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
        assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());

        // flush ensures watchForCommit updates the total length acknowledged
        assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());

        // now close the stream, It will update ack length after watchForCommit
      }
      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      // make sure the bufferPool is empty
      assertEquals(0, bufferPool.computeBufferData());
      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
      assertEquals(pendingWriteChunkCount,
          metrics.getPendingContainerOpCountMetrics(WriteChunk));
      assertEquals(pendingPutBlockCount,
          metrics.getPendingContainerOpCountMetrics(PutBlock));
      assertEquals(writeChunkCount + 2,
          metrics.getContainerOpCountMetrics(WriteChunk));
      assertEquals(putBlockCount + ((enablePiggybacking) ? 1 : 2),
          metrics.getContainerOpCountMetrics(PutBlock));
      assertEquals(totalOpCount  + ((enablePiggybacking) ? 3 : 4), metrics.getTotalOpCount());
      assertEquals(0, keyOutputStream.getStreamEntries().size());

      validateData(keyName, data1, client.getObjectStore(), VOLUME, BUCKET);
    }
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  @Flaky("HDDS-11564")
  void testWriteMoreThanFlushSize(boolean flushDelay, boolean enablePiggybacking) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay, enablePiggybacking);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      XceiverClientMetrics metrics =
          XceiverClientManager.getXceiverClientMetrics();
      long writeChunkCount = metrics.getContainerOpCountMetrics(
          WriteChunk);
      long putBlockCount = metrics.getContainerOpCountMetrics(
          PutBlock);
      long pendingWriteChunkCount = metrics.getPendingContainerOpCountMetrics(
          WriteChunk);
      long pendingPutBlockCount = metrics.getPendingContainerOpCountMetrics(
          PutBlock);
      long totalOpCount = metrics.getTotalOpCount();

      String keyName = getKeyName();
      int dataLength = FLUSH_SIZE + 50;
      byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
      KeyOutputStream keyOutputStream;
      RatisBlockOutputStream blockOutputStream;
      try (OzoneOutputStream key = createKey(client, keyName)) {
        key.write(data1);

        assertEquals(totalOpCount + 3, metrics.getTotalOpCount());
        keyOutputStream =
            assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

        assertEquals(1, keyOutputStream.getStreamEntries().size());
        blockOutputStream =
            assertInstanceOf(RatisBlockOutputStream.class,
                keyOutputStream.getStreamEntries().get(0).getOutputStream());

        // we have just written data more than flush Size(2 chunks), at this time
        // buffer pool will have 3 buffers allocated worth of chunk size

        assertEquals(3, blockOutputStream.getBufferPool().getSize());
        // writtenDataLength as well flushedDataLength will be updated here
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(FLUSH_SIZE, blockOutputStream.getTotalDataFlushedLength());
        assertEquals(0, blockOutputStream.getTotalAckDataLength());

        // Before flush, if there was no pending PutBlock which means it is complete.
        // It put a commit index into commitIndexMap.
        assertEquals((metrics.getPendingContainerOpCountMetrics(PutBlock) == pendingPutBlockCount) ? 1 : 0,
            blockOutputStream.getCommitIndex2flushedDataMap().size());

        key.flush();
        if (flushDelay) {
          // If the flushDelay feature is enabled, nothing happens.
          // The assertions will be as same as those before flush.
          assertEquals(FLUSH_SIZE, blockOutputStream.getTotalDataFlushedLength());
          assertEquals((metrics.getPendingContainerOpCountMetrics(PutBlock) == pendingPutBlockCount) ? 1 : 0,
              blockOutputStream.getCommitIndex2flushedDataMap().size());

          assertEquals(0, blockOutputStream.getTotalAckDataLength());
          assertEquals(1, keyOutputStream.getStreamEntries().size());
        } else {
          assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
          assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());

          assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
          assertEquals(1, keyOutputStream.getStreamEntries().size());
        }
      }

      assertEquals(pendingWriteChunkCount,
          metrics.getPendingContainerOpCountMetrics(WriteChunk));
      assertEquals(pendingPutBlockCount,
          metrics.getPendingContainerOpCountMetrics(PutBlock));
      assertEquals(writeChunkCount + 3,
          metrics.getContainerOpCountMetrics(WriteChunk));
      // If the flushDelay was disabled, it sends PutBlock with the data in the buffer.
      assertEquals(putBlockCount + (flushDelay ? 2 : 3) - (enablePiggybacking ? 1 : 0),
          metrics.getContainerOpCountMetrics(PutBlock));
      assertEquals(totalOpCount + (flushDelay ? 5 : 6) - (enablePiggybacking ? 1 : 0),
          metrics.getTotalOpCount());
      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      // make sure the bufferPool is empty
      assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
      assertEquals(0, keyOutputStream.getStreamEntries().size());

      validateData(keyName, data1, client.getObjectStore(), VOLUME, BUCKET);
    }
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  @Flaky("HDDS-11564")
  void testWriteExactlyMaxFlushSize(boolean flushDelay, boolean enablePiggybacking) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay, enablePiggybacking);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      XceiverClientMetrics metrics =
          XceiverClientManager.getXceiverClientMetrics();
      long writeChunkCount = metrics.getContainerOpCountMetrics(WriteChunk);
      long putBlockCount = metrics.getContainerOpCountMetrics(PutBlock);
      long pendingWriteChunkCount =
          metrics.getPendingContainerOpCountMetrics(WriteChunk);
      long pendingPutBlockCount =
          metrics.getPendingContainerOpCountMetrics(PutBlock);
      long totalOpCount = metrics.getTotalOpCount();

      String keyName = getKeyName();
      int dataLength = MAX_FLUSH_SIZE;
      byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
      KeyOutputStream keyOutputStream;
      RatisBlockOutputStream blockOutputStream;
      try (OzoneOutputStream key = createKey(client, keyName)) {
        key.write(data1);

        keyOutputStream =
            assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
        blockOutputStream =
            assertInstanceOf(RatisBlockOutputStream.class,
                keyOutputStream.getStreamEntries().get(0).getOutputStream());
        BufferPool bufferPool = blockOutputStream.getBufferPool();
        // since it's hitting the full bufferCondition, it will call watchForCommit
        // however, the outputstream will not wait for watchForCommit, but the next call to
        // write() will need to wait for at least one watchForCommit, indirectly when asking for new buffer allocation.
        bufferPool.waitUntilAvailable();

        assertThat(metrics.getPendingContainerOpCountMetrics(WriteChunk))
            .isLessThanOrEqualTo(pendingWriteChunkCount + 2);
        assertThat(metrics.getPendingContainerOpCountMetrics(PutBlock))
            .isLessThanOrEqualTo(pendingPutBlockCount + 1);

        assertEquals(1, keyOutputStream.getStreamEntries().size());

        assertEquals(4, blockOutputStream.getBufferPool().getSize());
        // writtenDataLength as well flushedDataLength will be updated here
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(MAX_FLUSH_SIZE,
            blockOutputStream.getTotalDataFlushedLength());

        // since data equals to maxBufferSize is written, this will be a blocking
        // call and hence will wait for atleast flushSize worth of data to get
        // ack'd by all servers right here
        assertThat(blockOutputStream.getTotalAckDataLength())
            .isGreaterThanOrEqualTo(FLUSH_SIZE);

        // watchForCommit will clean up atleast one entry from the map where each
        // entry corresponds to flushSize worth of data

        assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
            .isLessThanOrEqualTo(1);

        // This will flush the data and update the flush length and the map.
        key.flush();
        assertEquals(1, keyOutputStream.getStreamEntries().size());
        assertEquals(totalOpCount + 6, metrics.getTotalOpCount());

        // Since the data in the buffer is already flushed, flush here will have
        // no impact on the counters and data structures

        assertEquals(4, blockOutputStream.getBufferPool().getSize());
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(dataLength, blockOutputStream.getTotalDataFlushedLength());
        assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
            .isLessThanOrEqualTo(1);

        // now close the stream, it will update ack length after watchForCommit
      }
      assertEquals(pendingWriteChunkCount,
          metrics.getPendingContainerOpCountMetrics(WriteChunk));
      assertEquals(pendingPutBlockCount,
          metrics.getPendingContainerOpCountMetrics(PutBlock));
      assertEquals(writeChunkCount + 4,
          metrics.getContainerOpCountMetrics(WriteChunk));
      assertEquals(putBlockCount + 3,
          metrics.getContainerOpCountMetrics(PutBlock));
      assertEquals(totalOpCount + 7, metrics.getTotalOpCount());
      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      // make sure the bufferPool is empty
      assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
      assertEquals(0, keyOutputStream.getStreamEntries().size());
      validateData(keyName, data1, client.getObjectStore(), VOLUME, BUCKET);
    }
  }

  @ParameterizedTest
  @MethodSource("clientParameters")
  @Flaky("HDDS-11564")
  void testWriteMoreThanMaxFlushSize(boolean flushDelay, boolean enablePiggybacking) throws Exception {
    OzoneClientConfig config = newClientConfig(cluster.getConf(), flushDelay, enablePiggybacking);
    try (OzoneClient client = newClient(cluster.getConf(), config)) {
      XceiverClientMetrics metrics =
          XceiverClientManager.getXceiverClientMetrics();
      long writeChunkCount = metrics.getContainerOpCountMetrics(WriteChunk);
      long putBlockCount = metrics.getContainerOpCountMetrics(PutBlock);
      long pendingWriteChunkCount =
          metrics.getPendingContainerOpCountMetrics(WriteChunk);
      long pendingPutBlockCount =
          metrics.getPendingContainerOpCountMetrics(PutBlock);
      long totalOpCount = metrics.getTotalOpCount();
      String keyName = getKeyName();
      int dataLength = MAX_FLUSH_SIZE + 50;
      // write data more than 1 chunk
      byte[] data1 = RandomUtils.secure().randomBytes(dataLength);
      KeyOutputStream keyOutputStream;
      RatisBlockOutputStream blockOutputStream;
      try (OzoneOutputStream key = createKey(client, keyName)) {
        key.write(data1);
        keyOutputStream =
            assertInstanceOf(KeyOutputStream.class, key.getOutputStream());

        // since it's hitting full-buffer, it will call watchForCommit
        // and completes putBlock at least for first flushSize worth of data
        assertThat(metrics.getPendingContainerOpCountMetrics(WriteChunk))
            .isLessThanOrEqualTo(pendingWriteChunkCount + 2);
        assertThat(metrics.getPendingContainerOpCountMetrics(PutBlock))
            .isLessThanOrEqualTo(pendingPutBlockCount + 1);
        assertEquals(writeChunkCount + 4,
            metrics.getContainerOpCountMetrics(WriteChunk));
        assertEquals(putBlockCount + 2,
            metrics.getContainerOpCountMetrics(PutBlock));
        assertEquals(totalOpCount + 6, metrics.getTotalOpCount());
        assertEquals(1, keyOutputStream.getStreamEntries().size());
        blockOutputStream =
            assertInstanceOf(RatisBlockOutputStream.class,
                keyOutputStream.getStreamEntries().get(0).getOutputStream());

        assertThat(blockOutputStream.getBufferPool().getSize())
            .isLessThanOrEqualTo(4);
        // writtenDataLength as well flushedDataLength will be updated here
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());

        assertEquals(MAX_FLUSH_SIZE,
            blockOutputStream.getTotalDataFlushedLength());

        // since data equals to maxBufferSize is written, this will be a blocking
        // call and hence will wait for atleast flushSize worth of data to get
        // ack'd by all servers right here
        assertThat(blockOutputStream.getTotalAckDataLength())
            .isGreaterThanOrEqualTo(FLUSH_SIZE);

        // watchForCommit will clean up atleast one entry from the map where each
        // entry corresponds to flushSize worth of data
        assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
            .isLessThanOrEqualTo(1);

        // Now do a flush.
        key.flush();
        assertEquals(1, keyOutputStream.getStreamEntries().size());
        assertEquals(pendingWriteChunkCount,
            metrics.getPendingContainerOpCountMetrics(WriteChunk));
        assertEquals(pendingPutBlockCount,
            metrics.getPendingContainerOpCountMetrics(PutBlock));

        // Since the data in the buffer is already flushed, flush here will have
        // no impact on the counters and data structures

        assertThat(blockOutputStream.getBufferPool().getSize())
            .isLessThanOrEqualTo(4);
        assertEquals(dataLength, blockOutputStream.getWrittenDataLength());
        // dataLength > MAX_FLUSH_SIZE
        assertEquals(flushDelay ? MAX_FLUSH_SIZE : dataLength,
            blockOutputStream.getTotalDataFlushedLength());
        assertThat(blockOutputStream.getCommitIndex2flushedDataMap().size())
            .isLessThanOrEqualTo(2);

        // now close the stream, it will update ack length after watchForCommit
      }
      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      // make sure the bufferPool is empty
      assertEquals(0, blockOutputStream.getBufferPool().computeBufferData());
      assertEquals(pendingWriteChunkCount,
          metrics.getPendingContainerOpCountMetrics(WriteChunk));
      assertEquals(pendingPutBlockCount,
          metrics.getPendingContainerOpCountMetrics(PutBlock));
      assertEquals(writeChunkCount + 5,
          metrics.getContainerOpCountMetrics(WriteChunk));
      // The previous flush did not trigger any action with flushDelay enabled
      assertEquals(putBlockCount + (flushDelay ? 2 : 3)  + (enablePiggybacking ? 0 : 1),
          metrics.getContainerOpCountMetrics(PutBlock));
      assertEquals(totalOpCount + (flushDelay ? 7 : 8) + ((enablePiggybacking ? 0 : 1)),
          metrics.getTotalOpCount());
      assertEquals(dataLength, blockOutputStream.getTotalAckDataLength());
      assertEquals(0, blockOutputStream.getCommitIndex2flushedDataMap().size());
      assertEquals(0, keyOutputStream.getStreamEntries().size());
      validateData(keyName, data1, client.getObjectStore(), VOLUME, BUCKET);
    }
  }

  static OzoneOutputStream createKey(OzoneClient client, String keyName)
      throws Exception {
    return createKey(client, keyName, 0, ReplicationFactor.THREE);
  }

  static OzoneOutputStream createKey(OzoneClient client, String keyName,
      long size, ReplicationFactor factor) throws Exception {
    return TestHelper.createKey(keyName, ReplicationType.RATIS, factor, size,
        client.getObjectStore(), VOLUME, BUCKET);
  }

  static String getKeyName() {
    return UUID.randomUUID().toString();
  }

}
