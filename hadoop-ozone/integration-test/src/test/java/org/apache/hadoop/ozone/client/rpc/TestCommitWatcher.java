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
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.hdds.scm.storage.CommitWatcher;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.ratis.protocol.AlreadyClosedException;
import org.apache.ratis.protocol.NotReplicatedException;
import org.apache.ratis.protocol.RaftRetryFailureException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.rules.Timeout;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.
        OZONE_SCM_STALENODE_INTERVAL;

/**
 * Class to test CommitWatcher functionality.
 */
public class TestCommitWatcher {

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
  private static long flushSize;
  private static long maxFlushSize;
  private static long blockSize;
  private static String volumeName;
  private static String bucketName;
  private static String keyString;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    chunkSize = (int)(1 * OzoneConsts.MB);
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;
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
    conf.setTimeDuration(
            RatisHelper.HDDS_DATANODE_RATIS_CLIENT_PREFIX_KEY+ "." +
                    "rpc.request.timeout",
            3, TimeUnit.SECONDS);
    conf.setTimeDuration(
            RatisHelper.HDDS_DATANODE_RATIS_CLIENT_PREFIX_KEY+ "." +
                    "watch.request.timeout",
            3, TimeUnit.SECONDS);
    conf.setTimeDuration(RatisHelper.HDDS_DATANODE_RATIS_PREFIX_KEY
        + ".client.request.write.timeout", 30, TimeUnit.SECONDS);
    conf.setTimeDuration(RatisHelper.HDDS_DATANODE_RATIS_PREFIX_KEY
        + ".client.request.watch.timeout", 30, TimeUnit.SECONDS);
    conf.set(OzoneConfigKeys.OZONE_CLIENT_CHECKSUM_TYPE, "NONE");
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 4,
        StorageUnit.MB);
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
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReleaseBuffers() throws Exception {
    int capacity = 2;
    BufferPool bufferPool = new BufferPool(chunkSize, capacity);
    XceiverClientManager clientManager = new XceiverClientManager(conf);
    ContainerWithPipeline container = storageContainerLocationClient
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, OzoneConsts.OZONE);
    Pipeline pipeline = container.getPipeline();
    long containerId = container.getContainerInfo().getContainerID();
    XceiverClientSpi xceiverClient = clientManager.acquireClient(pipeline);
    Assert.assertEquals(1, xceiverClient.getRefcount());
    Assert.assertTrue(xceiverClient instanceof XceiverClientRatis);
    XceiverClientRatis ratisClient = (XceiverClientRatis) xceiverClient;
    CommitWatcher watcher = new CommitWatcher(bufferPool, ratisClient);
    BlockID blockID = ContainerTestHelper.getTestBlockID(containerId);
    List<XceiverClientReply> replies = new ArrayList<>();
    long length = 0;
    List<CompletableFuture<ContainerProtos.ContainerCommandResponseProto>>
        futures = new ArrayList<>();
    for (int i = 0; i < capacity; i++) {
      ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
          ContainerTestHelper
              .getWriteChunkRequest(pipeline, blockID, chunkSize, null);
      // add the data to the buffer pool
      final ChunkBuffer byteBuffer = bufferPool.allocateBufferIfNeeded(0);
      byteBuffer.put(writeChunkRequest.getWriteChunk().getData());
      ratisClient.sendCommandAsync(writeChunkRequest);
      ContainerProtos.ContainerCommandRequestProto putBlockRequest =
          ContainerTestHelper
              .getPutBlockRequest(pipeline, writeChunkRequest.getWriteChunk());
      XceiverClientReply reply = ratisClient.sendCommandAsync(putBlockRequest);
      final List<ChunkBuffer> bufferList = singletonList(byteBuffer);
      length += byteBuffer.position();
      CompletableFuture<ContainerProtos.ContainerCommandResponseProto> future =
          reply.getResponse().thenApply(v -> {
            watcher.updateCommitInfoMap(reply.getLogIndex(), bufferList);
            return v;
          });
      futures.add(future);
      watcher.getFutureMap().put(length, future);
      replies.add(reply);
    }

    Assert.assertTrue(replies.size() == 2);
    // wait on the 1st putBlock to complete
    CompletableFuture<ContainerProtos.ContainerCommandResponseProto> future1 =
        futures.get(0);
    CompletableFuture<ContainerProtos.ContainerCommandResponseProto> future2 =
        futures.get(1);
    future1.get();
    Assert.assertNotNull(watcher.getFutureMap().get(new Long(chunkSize)));
    Assert.assertTrue(
        watcher.getFutureMap().get(new Long(chunkSize)).equals(future1));
    // wait on 2nd putBlock to complete
    future2.get();
    Assert.assertNotNull(watcher.getFutureMap().get(
            new Long(2 * chunkSize)));
    Assert.assertTrue(
            watcher.getFutureMap().get(new Long(2 * chunkSize)).
                    equals(future2));
    Assert.assertTrue(watcher.
            getCommitIndex2flushedDataMap().size() == 2);
    watcher.watchOnFirstIndex();
    Assert.assertFalse(watcher.getCommitIndex2flushedDataMap()
            .containsKey(replies.get(0).getLogIndex()));
    Assert.assertFalse(watcher.getFutureMap().containsKey(chunkSize));
    Assert.assertTrue(watcher.getTotalAckDataLength() >= chunkSize);
    watcher.watchOnLastIndex();
    Assert.assertFalse(watcher.getCommitIndex2flushedDataMap()
        .containsKey(replies.get(1).getLogIndex()));
    Assert.assertFalse(watcher.getFutureMap().containsKey(2 * chunkSize));
    Assert.assertTrue(watcher.getTotalAckDataLength() == 2 * chunkSize);
    Assert.assertTrue(watcher.getFutureMap().isEmpty());
    Assert.assertTrue(watcher.getCommitIndex2flushedDataMap().isEmpty());
  }

  @Test
  public void testReleaseBuffersOnException() throws Exception {
    int capacity = 2;
    BufferPool bufferPool = new BufferPool(chunkSize, capacity);
    XceiverClientManager clientManager = new XceiverClientManager(conf);
    ContainerWithPipeline container = storageContainerLocationClient
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, OzoneConsts.OZONE);
    Pipeline pipeline = container.getPipeline();
    long containerId = container.getContainerInfo().getContainerID();
    XceiverClientSpi xceiverClient = clientManager.acquireClient(pipeline);
    Assert.assertEquals(1, xceiverClient.getRefcount());
    Assert.assertTrue(xceiverClient instanceof XceiverClientRatis);
    XceiverClientRatis ratisClient = (XceiverClientRatis) xceiverClient;
    CommitWatcher watcher = new CommitWatcher(bufferPool, ratisClient);
    BlockID blockID = ContainerTestHelper.getTestBlockID(containerId);
    List<XceiverClientReply> replies = new ArrayList<>();
    long length = 0;
    List<CompletableFuture<ContainerProtos.ContainerCommandResponseProto>>
        futures = new ArrayList<>();
    for (int i = 0; i < capacity; i++) {
      ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
          ContainerTestHelper
              .getWriteChunkRequest(pipeline, blockID, chunkSize, null);
      // add the data to the buffer pool
      final ChunkBuffer byteBuffer = bufferPool.allocateBufferIfNeeded(0);
      byteBuffer.put(writeChunkRequest.getWriteChunk().getData());
      ratisClient.sendCommandAsync(writeChunkRequest);
      ContainerProtos.ContainerCommandRequestProto putBlockRequest =
          ContainerTestHelper
              .getPutBlockRequest(pipeline, writeChunkRequest.getWriteChunk());
      XceiverClientReply reply = ratisClient.sendCommandAsync(putBlockRequest);
      final List<ChunkBuffer> bufferList = singletonList(byteBuffer);
      length += byteBuffer.position();
      CompletableFuture<ContainerProtos.ContainerCommandResponseProto> future =
          reply.getResponse().thenApply(v -> {
            watcher.updateCommitInfoMap(reply.getLogIndex(), bufferList);
            return v;
          });
      futures.add(future);
      watcher.getFutureMap().put(length, future);
      replies.add(reply);
    }

    Assert.assertTrue(replies.size() == 2);
    // wait on the 1st putBlock to complete
    CompletableFuture<ContainerProtos.ContainerCommandResponseProto> future1 =
        futures.get(0);
    CompletableFuture<ContainerProtos.ContainerCommandResponseProto> future2 =
        futures.get(1);
    future1.get();
    Assert.assertNotNull(watcher.getFutureMap().get(new Long(chunkSize)));
    Assert.assertTrue(
        watcher.getFutureMap().get(new Long(chunkSize)).equals(future1));
    // wait on 2nd putBlock to complete
    future2.get();
    Assert.assertNotNull(watcher.getFutureMap().get(new Long(2 * chunkSize)));
    Assert.assertTrue(
        watcher.getFutureMap().get(new Long(2 * chunkSize)).equals(future2));
    Assert.assertTrue(watcher.getCommitIndex2flushedDataMap().size() == 2);
    watcher.watchOnFirstIndex();
    Assert.assertFalse(watcher.getCommitIndex2flushedDataMap()
        .containsKey(replies.get(0).getLogIndex()));
    Assert.assertFalse(watcher.getFutureMap().containsKey(chunkSize));
    Assert.assertTrue(watcher.getTotalAckDataLength() >= chunkSize);
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(1));
    try {
      // just watch for a higher index so as to ensure, it does an actual
      // call to Ratis. Otherwise, it may just return in case the commitInfoMap
      // is updated to the latest index in putBlock response.
      watcher.watchForCommit(replies.get(1).getLogIndex() + 100);
      Assert.fail("Expected exception not thrown");
    } catch(IOException ioe) {
      // with retry count set to lower limit and a lower watch request
      // timeout, watch request will eventually
      // fail with RaftRetryFailure exception from ratis client or the client
      // can itself get AlreadyClosedException from the Ratis Server
      Throwable t = HddsClientUtils.checkForException(ioe);
      Assert.assertTrue(t instanceof RaftRetryFailureException ||
              t instanceof AlreadyClosedException ||
              t instanceof NotReplicatedException);
    }
    if (ratisClient.getReplicatedMinCommitIndex() < replies.get(1)
        .getLogIndex()) {
      Assert.assertTrue(watcher.getTotalAckDataLength() == chunkSize);
      Assert.assertTrue(watcher.getCommitIndex2flushedDataMap().size() == 1);
      Assert.assertTrue(watcher.getFutureMap().size() == 1);
    } else {
      Assert.assertTrue(watcher.getTotalAckDataLength() == 2 * chunkSize);
      Assert.assertTrue(watcher.getFutureMap().isEmpty());
      Assert.assertTrue(watcher.getCommitIndex2flushedDataMap().isEmpty());
    }
  }
}
