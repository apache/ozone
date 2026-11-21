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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

/**
 * A stateful test harness that simulates a Ratis datanode pipeline for {@link RatisBlockOutputStream} tests.
 *
 * <p>Tracks all writeChunk, putBlock, and watchForCommit calls. Failure injection can be configured independently
 * for writeChunk, putBlock, and watchForCommit.
 */
public class MockRatisPipeline {

  private final Pipeline pipeline;
  private final MockXceiverClientSpi xceiverClient;
  private final XceiverClientFactory clientFactory;
  private final BlockID blockID;

  private final List<byte[]> receivedChunks = Collections.synchronizedList(new ArrayList<>());
  private final List<ContainerCommandRequestProto> receivedWriteChunks =
      Collections.synchronizedList(new ArrayList<>());
  private final List<ContainerCommandRequestProto> receivedPutBlocks =
      Collections.synchronizedList(new ArrayList<>());
  private final List<BlockData> receivedBlockData = Collections.synchronizedList(new ArrayList<>());
  private final AtomicInteger watchForCommitCount = new AtomicInteger(0);

  private final AtomicLong nextLogIndex = new AtomicLong(1);

  private volatile Supplier<Throwable> writeChunkFailure = null;
  private volatile int writeChunkFailAfter = Integer.MAX_VALUE;
  private final AtomicInteger writeChunkCount = new AtomicInteger(0);

  private volatile Supplier<Throwable> putBlockFailure = null;
  private volatile int putBlockFailAfter = Integer.MAX_VALUE;
  private final AtomicInteger putBlockCount = new AtomicInteger(0);

  private volatile Supplier<Throwable> watchFailure = null;
  private volatile int watchFailAfter = Integer.MAX_VALUE;

  public MockRatisPipeline() throws IOException {
    this(new BlockID(1, 1));
  }

  public MockRatisPipeline(BlockID blockID) throws IOException {
    this.blockID = blockID;
    this.pipeline = MockPipeline.createRatisPipeline();

    // Ensure metrics are initialized.
    XceiverClientManager.getXceiverClientMetrics();

    this.xceiverClient = new MockXceiverClientSpi();
    this.clientFactory = new MockXceiverClientFactory();
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public XceiverClientSpi getXceiverClient() {
    return xceiverClient;
  }

  public XceiverClientFactory getClientFactory() {
    return clientFactory;
  }

  public BlockID getBlockID() {
    return blockID;
  }

  public List<byte[]> getReceivedChunks() {
    return receivedChunks;
  }

  public List<ContainerCommandRequestProto> getReceivedWriteChunks() {
    return receivedWriteChunks;
  }

  public List<ContainerCommandRequestProto> getReceivedPutBlocks() {
    return receivedPutBlocks;
  }

  /** @return block metadata received by either PutBlock or a piggybacked WriteChunk. */
  public List<BlockData> getReceivedBlockData() {
    return receivedBlockData;
  }

  public int getWatchForCommitCount() {
    return watchForCommitCount.get();
  }

  /** Concatenate all received chunk payloads into a single byte array. */
  public byte[] getAllReceivedData() {
    int total = receivedChunks.stream().mapToInt(c -> c.length).sum();
    byte[] result = new byte[total];
    int pos = 0;
    for (byte[] chunk : receivedChunks) {
      System.arraycopy(chunk, 0, result, pos, chunk.length);
      pos += chunk.length;
    }
    return result;
  }

  public MockRatisPipeline failWriteChunkAfter(int n, Supplier<Throwable> err) {
    this.writeChunkFailAfter = n;
    this.writeChunkFailure = err;
    return this;
  }

  public MockRatisPipeline failPutBlockAfter(int n, Supplier<Throwable> err) {
    this.putBlockFailAfter = n;
    this.putBlockFailure = err;
    return this;
  }

  public MockRatisPipeline failWatchAfter(int n, Supplier<Throwable> err) {
    this.watchFailAfter = n;
    this.watchFailure = err;
    return this;
  }

  private XceiverClientReply handleWriteChunk(ContainerCommandRequestProto request) {
    receivedWriteChunks.add(request);
    int count = writeChunkCount.incrementAndGet();
    if (count > writeChunkFailAfter && writeChunkFailure != null) {
      return failedReply(writeChunkFailure.get());
    }

    ContainerProtos.WriteChunkRequestProto writeChunk = request.getWriteChunk();
    receivedChunks.add(writeChunk.getData().toByteArray());

    WriteChunkResponseProto.Builder writeChunkResponse = WriteChunkResponseProto.newBuilder();
    if (writeChunk.hasBlock()) {
      int putBlockCall = putBlockCount.incrementAndGet();
      if (putBlockCall > putBlockFailAfter && putBlockFailure != null) {
        return failedReply(putBlockFailure.get());
      }
      BlockData blockData = writeChunk.getBlock().getBlockData();
      receivedBlockData.add(blockData);
      writeChunkResponse.setCommittedBlockLength(committedBlockLength(blockData));
    }

    return successfulReply(ContainerCommandResponseProto.newBuilder()
        .setCmdType(Type.WriteChunk)
        .setResult(Result.SUCCESS)
        .setWriteChunk(writeChunkResponse)
        .build());
  }

  private XceiverClientReply handlePutBlock(ContainerCommandRequestProto request) {
    receivedPutBlocks.add(request);
    int count = putBlockCount.incrementAndGet();
    if (count > putBlockFailAfter && putBlockFailure != null) {
      return failedReply(putBlockFailure.get());
    }

    BlockData blockData = request.getPutBlock().getBlockData();
    receivedBlockData.add(blockData);
    return successfulReply(ContainerCommandResponseProto.newBuilder()
        .setCmdType(Type.PutBlock)
        .setResult(Result.SUCCESS)
        .setPutBlock(PutBlockResponseProto.newBuilder()
            .setCommittedBlockLength(committedBlockLength(blockData))
            .build())
        .build());
  }

  private XceiverClientReply successfulReply(ContainerCommandResponseProto response) {
    XceiverClientReply reply = new XceiverClientReply(CompletableFuture.completedFuture(response));
    reply.setLogIndex(nextLogIndex.getAndIncrement());
    return reply;
  }

  private XceiverClientReply failedReply(Throwable t) {
    CompletableFuture<ContainerCommandResponseProto> failed = new CompletableFuture<>();
    failed.completeExceptionally(t);
    XceiverClientReply reply = new XceiverClientReply(failed);
    reply.setLogIndex(nextLogIndex.getAndIncrement());
    return reply;
  }

  private static GetCommittedBlockLengthResponseProto committedBlockLength(BlockData blockData) {
    long length = blockData.getChunksList().stream()
        .mapToLong(ContainerProtos.ChunkInfo::getLen)
        .sum();
    return GetCommittedBlockLengthResponseProto.newBuilder()
        .setBlockID(blockData.getBlockID())
        .setBlockLength(length)
        .build();
  }

  private final class MockXceiverClientSpi extends XceiverClientSpi {

    @Override
    public void connect() {
    }

    @Override
    public void close() {
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public XceiverClientReply sendCommandAsync(ContainerCommandRequestProto request)
        throws IOException, ExecutionException, InterruptedException {
      switch (request.getCmdType()) {
      case WriteChunk:
        return handleWriteChunk(request);
      case PutBlock:
        return handlePutBlock(request);
      default:
        return successfulReply(ContainerCommandResponseProto.newBuilder()
            .setCmdType(request.getCmdType())
            .setResult(Result.SUCCESS)
            .build());
      }
    }

    @Override
    public ReplicationType getPipelineType() {
      return pipeline.getType();
    }

    @Override
    public CompletableFuture<XceiverClientReply> watchForCommit(long index) {
      int count = watchForCommitCount.incrementAndGet();
      CompletableFuture<XceiverClientReply> f = new CompletableFuture<>();
      if (count > watchFailAfter && watchFailure != null) {
        f.completeExceptionally(watchFailure.get());
      } else {
        XceiverClientReply reply = new XceiverClientReply(null);
        reply.setLogIndex(index);
        f.complete(reply);
      }
      return f;
    }

    @Override
    public long getReplicatedMinCommitIndex() {
      return 0;
    }

    @Override
    public Map<DatanodeDetails, ContainerCommandResponseProto> sendCommandOnAllNodes(
        ContainerCommandRequestProto request) {
      return Collections.emptyMap();
    }
  }

  private final class MockXceiverClientFactory implements XceiverClientFactory {

    @Override
    public XceiverClientSpi acquireClient(Pipeline p) {
      return xceiverClient;
    }

    @Override
    public XceiverClientSpi acquireClient(Pipeline p, boolean topologyAware) {
      return xceiverClient;
    }

    @Override
    public XceiverClientSpi acquireClient(Pipeline p, boolean topologyAware, boolean allowShortCircuit) {
      return xceiverClient;
    }

    @Override
    public XceiverClientSpi acquireClientForReadData(Pipeline p, boolean allowShortCircuit) {
      return xceiverClient;
    }

    @Override
    public void releaseClient(XceiverClientSpi client, boolean invalidateClient) {
    }

    @Override
    public void releaseClient(XceiverClientSpi client, boolean invalidateClient, boolean topologyAware) {
    }

    @Override
    public void releaseClientForReadData(XceiverClientSpi client, boolean invalidateClient) {
    }

    @Override
    public void close() {
    }
  }
}
