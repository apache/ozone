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

package org.apache.hadoop.hdds.scm.storage;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.NotReplicatedException;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class to test CommitWatcher functionality.
 */
public class TestCommitWatcher {
  private static final int CHUNK_SIZE = (int)(1 * MB);
  private static final long FLUSH_SIZE = (long) 2 * CHUNK_SIZE;
  private static final long MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;
  private static final long BLOCK_SIZE = 2 * MAX_FLUSH_SIZE;
  private static final String VOLUME_NAME = "testblockoutputstream";

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private OzoneClient client;
  private StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  @BeforeEach
  public void init() throws Exception {
    // Make sure the pipeline does not get destroyed quickly
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
            10, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 1000,
            TimeUnit.SECONDS);
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(3));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(3));
    conf.setFromObject(raftClientConfig);

    RatisClientConfig ratisClientConfig =
        conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWriteRequestTimeout(Duration.ofSeconds(10));
    ratisClientConfig.setWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(ratisClientConfig);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumType(ChecksumType.NONE);
    conf.setFromObject(clientConfig);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .applyTo(conf);

    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4,
        StorageUnit.MB);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(VOLUME_NAME);
    objectStore.getVolume(VOLUME_NAME).createBucket(VOLUME_NAME);
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReleaseBuffers() throws Exception {
    int capacity = 2;
    BufferPool bufferPool = new BufferPool(CHUNK_SIZE, capacity);
    try (XceiverClientManager mgr = new XceiverClientManager(conf)) {
      ContainerWithPipeline container = storageContainerLocationClient
          .allocateContainer(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE, OzoneConsts.OZONE);
      Pipeline pipeline = container.getPipeline();
      long containerId = container.getContainerInfo().getContainerID();
      try (XceiverClientSpi xceiverClient = mgr.acquireClient(pipeline)) {
        assertEquals(1, xceiverClient.getRefcount());
        XceiverClientRatis ratisClient = assertInstanceOf(XceiverClientRatis.class, xceiverClient);
        CommitWatcher watcher = new CommitWatcher(bufferPool, ratisClient);
        BlockID blockID = ContainerTestHelper.getTestBlockID(containerId);
        List<XceiverClientReply> replies = new ArrayList<>();
        long length = 0;
        List<CompletableFuture<ContainerCommandResponseProto>>
            futures = new ArrayList<>();
        for (int i = 0; i < capacity; i++) {
          ContainerCommandRequestProto writeChunkRequest =
              ContainerTestHelper
                  .getWriteChunkRequest(pipeline, blockID, CHUNK_SIZE);
          // add the data to the buffer pool
          final ChunkBuffer byteBuffer = bufferPool.allocateBuffer(0);
          byteBuffer.put(writeChunkRequest.getWriteChunk().getData());
          ratisClient.sendCommandAsync(writeChunkRequest);
          ContainerCommandRequestProto putBlockRequest =
              ContainerTestHelper.getPutBlockRequest(pipeline,
                  writeChunkRequest.getWriteChunk());
          XceiverClientReply reply =
              ratisClient.sendCommandAsync(putBlockRequest);
          final List<ChunkBuffer> bufferList = singletonList(byteBuffer);
          length += byteBuffer.position();
          CompletableFuture<ContainerCommandResponseProto> future =
              reply.getResponse().thenApply(v -> {
                watcher.updateCommitInfoMap(reply.getLogIndex(), bufferList);
                return v;
              });
          futures.add(future);
          replies.add(reply);
        }

        assertEquals(2, replies.size());
        // wait on the 1st putBlock to complete
        CompletableFuture<ContainerCommandResponseProto> future1 =
            futures.get(0);
        CompletableFuture<ContainerCommandResponseProto> future2 =
            futures.get(1);
        future1.get();
        future2.get();
        assertEquals(2, watcher.
            getCommitIndexMap().size());
        watcher.watchOnFirstIndex();
        assertThat(watcher.getCommitIndexMap()).doesNotContainKey(replies.get(0).getLogIndex());
        assertThat(watcher.getTotalAckDataLength()).isGreaterThanOrEqualTo(CHUNK_SIZE);
        watcher.watchOnLastIndex();
        assertThat(watcher.getCommitIndexMap()).doesNotContainKey(replies.get(1).getLogIndex());
        assertEquals(2 * CHUNK_SIZE, watcher.getTotalAckDataLength());
        assertThat(watcher.getCommitIndexMap()).isEmpty();
      }
    } finally {
      bufferPool.clearBufferPool();
    }
  }

  @Test
  public void testReleaseBuffersOnException() throws Exception {
    int capacity = 2;
    BufferPool bufferPool = new BufferPool(CHUNK_SIZE, capacity);
    try (XceiverClientManager mgr = new XceiverClientManager(conf)) {
      ContainerWithPipeline container = storageContainerLocationClient
          .allocateContainer(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE, OzoneConsts.OZONE);
      Pipeline pipeline = container.getPipeline();
      long containerId = container.getContainerInfo().getContainerID();
      try (XceiverClientSpi xceiverClient = mgr.acquireClient(pipeline)) {
        assertEquals(1, xceiverClient.getRefcount());
        XceiverClientRatis ratisClient = assertInstanceOf(XceiverClientRatis.class, xceiverClient);
        CommitWatcher watcher = new CommitWatcher(bufferPool, ratisClient);
        BlockID blockID = ContainerTestHelper.getTestBlockID(containerId);
        List<XceiverClientReply> replies = new ArrayList<>();
        long length = 0;
        List<CompletableFuture<ContainerCommandResponseProto>>
            futures = new ArrayList<>();
        for (int i = 0; i < capacity; i++) {
          ContainerCommandRequestProto writeChunkRequest =
              ContainerTestHelper
                  .getWriteChunkRequest(pipeline, blockID, CHUNK_SIZE);
          // add the data to the buffer pool
          final ChunkBuffer byteBuffer = bufferPool.allocateBuffer(0);
          byteBuffer.put(writeChunkRequest.getWriteChunk().getData());
          ratisClient.sendCommandAsync(writeChunkRequest);
          ContainerCommandRequestProto putBlockRequest =
              ContainerTestHelper.getPutBlockRequest(pipeline,
                  writeChunkRequest.getWriteChunk());
          XceiverClientReply reply =
              ratisClient.sendCommandAsync(putBlockRequest);
          final List<ChunkBuffer> bufferList = singletonList(byteBuffer);
          length += byteBuffer.position();
          CompletableFuture<ContainerCommandResponseProto> future =
              reply.getResponse().thenApply(v -> {
                watcher.updateCommitInfoMap(reply.getLogIndex(), bufferList);
                return v;
              });
          futures.add(future);
          replies.add(reply);
        }

        assertEquals(2, replies.size());
        // wait on the 1st putBlock to complete
        CompletableFuture<ContainerCommandResponseProto> future1 =
            futures.get(0);
        CompletableFuture<ContainerCommandResponseProto> future2 =
            futures.get(1);
        future1.get();
        // wait on 2nd putBlock to complete
        future2.get();
        assertEquals(2, watcher.getCommitIndexMap().size());
        watcher.watchOnFirstIndex();
        assertThat(watcher.getCommitIndexMap()).doesNotContainKey(replies.get(0).getLogIndex());
        assertThat(watcher.getTotalAckDataLength()).isGreaterThanOrEqualTo(CHUNK_SIZE);
        cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));
        cluster.shutdownHddsDatanode(pipeline.getNodes().get(1));
        // just watch for a higher index so as to ensure, it does an actual
        // call to Ratis. Otherwise, it may just return in case the
        // commitInfoMap is updated to the latest index in putBlock response.
        IOException ioe =
            assertThrows(IOException.class, () -> watcher.watchForCommit(replies.get(1).getLogIndex() + 100));
        Throwable t = HddsClientUtils.checkForException(ioe);
        // with retry count set to noRetry and a lower watch request
        // timeout, watch request will eventually
        // fail with TimeoutIOException from ratis client or the client
        // can itself get AlreadyClosedException from the Ratis Server
        // and the write may fail with RaftRetryFailureException
        assertTrue(
            t instanceof RaftRetryFailureException ||
                t instanceof TimeoutIOException ||
                t instanceof AlreadyClosedException ||
                t instanceof NotReplicatedException,
            "Unexpected exception: " + t.getClass());
        if (ratisClient.getReplicatedMinCommitIndex() < replies.get(1)
            .getLogIndex()) {
          assertEquals(CHUNK_SIZE, watcher.getTotalAckDataLength());
          assertEquals(1, watcher.getCommitIndexMap().size());
        } else {
          assertEquals(2 * CHUNK_SIZE, watcher.getTotalAckDataLength());
          assertThat(watcher.getCommitIndexMap()).isEmpty();
        }
      }
    } finally {
      bufferPool.clearBufferPool();
    }
  }
}
