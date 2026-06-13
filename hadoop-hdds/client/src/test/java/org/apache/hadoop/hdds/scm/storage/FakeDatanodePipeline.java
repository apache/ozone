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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.ratis.client.api.DataStreamApi;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.io.FilePositionCount;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.io.WriteOption;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RoutingTable;

/**
 * A stateful test harness that simulates a datanode pipeline for {@link BlockDataStreamOutput} unit tests.
 * Replaces a real Ratis pipeline with mocked {@link XceiverClientRatis} and a concrete {@link DataStreamOutput}
 * implementation.
 *
 * <p>Tracks all chunks written, putBlock calls, and watchForCommit calls.
 * Configurable failure injection for each operation.
 */
public class FakeDatanodePipeline {

  private final Pipeline pipeline;
  private final XceiverClientRatis xceiverClient;
  private final XceiverClientFactory clientFactory;
  private final BlockID blockID;

  // Recorded state
  private final List<byte[]> receivedChunks = Collections.synchronizedList(new ArrayList<>());
  private final List<ContainerCommandRequestProto> receivedPutBlocks = Collections.synchronizedList(new ArrayList<>());
  private final AtomicInteger watchForCommitCount = new AtomicInteger(0);

  // Commit tracking
  private final AtomicLong nextLogIndex = new AtomicLong(1);

  // Failure injection
  private volatile Supplier<Throwable> chunkFailure = null;
  private volatile int chunkFailAfter = Integer.MAX_VALUE;
  private final AtomicInteger chunkCount = new AtomicInteger(0);

  private volatile Supplier<Throwable> putBlockFailure = null;
  private volatile int putBlockFailAfter = Integer.MAX_VALUE;
  private final AtomicInteger putBlockCount = new AtomicInteger(0);

  private volatile Supplier<Throwable> watchFailure = null;
  private volatile int watchFailAfter = Integer.MAX_VALUE;

  public FakeDatanodePipeline() throws IOException {
    this(new BlockID(1, 1));
  }

  public FakeDatanodePipeline(BlockID blockID) throws IOException {
    this.blockID = blockID;
    this.pipeline = MockPipeline.createRatisPipeline();

    // Ensure metrics are initialized
    XceiverClientManager.getXceiverClientMetrics();

    // Create concrete DataStreamOutput
    FakeDataStreamOutput fakeOut = new FakeDataStreamOutput();

    // Mock XceiverClientRatis
    this.xceiverClient = mock(XceiverClientRatis.class);
    when(xceiverClient.getPipeline()).thenReturn(pipeline);
    doReturn(0L).when(xceiverClient).getReplicatedMinCommitIndex();

    // Mock DataStreamApi to return our concrete DataStreamOutput
    // Both overloads must be stubbed: stream(ByteBuffer) and stream(ByteBuffer, RoutingTable) — the pipeline-mode
    // default is true, so the 2-arg overload is what BlockDataStreamOutput.setupStream calls.
    DataStreamApi dataStreamApi = mock(DataStreamApi.class);
    doReturn(fakeOut).when(dataStreamApi).stream(any(ByteBuffer.class));
    doReturn(fakeOut).when(dataStreamApi).stream(any(ByteBuffer.class), any(RoutingTable.class));
    doReturn(dataStreamApi).when(xceiverClient).getDataStreamApi();

    // Setup sendCommandAsync (putBlock) behavior
    doAnswer(invocation -> {
      ContainerCommandRequestProto request = invocation.getArgument(0);
      if (request.getCmdType() == Type.PutBlock) {
        receivedPutBlocks.add(request);
        int count = putBlockCount.incrementAndGet();
        CompletableFuture<ContainerCommandResponseProto> f = new CompletableFuture<>();
        if (count > putBlockFailAfter && putBlockFailure != null) {
          f.completeExceptionally(putBlockFailure.get());
        } else {
          ContainerCommandResponseProto response = buildPutBlockResponse(blockID);
          f.complete(response);
        }
        XceiverClientReply reply = new XceiverClientReply(f);
        reply.setLogIndex(nextLogIndex.getAndIncrement());
        return reply;
      }
      // Default: return success
      ContainerCommandResponseProto response =
          ContainerCommandResponseProto.newBuilder()
              .setCmdType(request.getCmdType())
              .setResult(Result.SUCCESS)
              .build();
      XceiverClientReply reply = new XceiverClientReply(CompletableFuture.completedFuture(response));
      reply.setLogIndex(0);
      return reply;
    }).when(xceiverClient).sendCommandAsync(any());

    // Setup watchForCommit behavior
    doAnswer(invocation -> {
      long index = invocation.getArgument(0);
      int count = watchForCommitCount.incrementAndGet();
      CompletableFuture<XceiverClientReply> f = new CompletableFuture<>();
      if (count > watchFailAfter && watchFailure != null) {
        f.completeExceptionally(watchFailure.get());
      } else {
        XceiverClientReply watchReply = new XceiverClientReply(null);
        watchReply.setLogIndex(index);
        f.complete(watchReply);
      }
      return f;
    }).when(xceiverClient).watchForCommit(anyLong());

    // Setup updateCommitInfosMap — no-op
    doReturn(0L).when(xceiverClient).updateCommitInfosMap(any(Collection.class));

    // Mock XceiverClientFactory
    this.clientFactory = mock(XceiverClientFactory.class);
    doReturn(xceiverClient).when(clientFactory).acquireClient(any(Pipeline.class), anyBoolean());
    doReturn(xceiverClient).when(clientFactory).acquireClient(any(Pipeline.class));
  }

  // --- Accessors ---

  public Pipeline getPipeline() {
    return pipeline;
  }

  public XceiverClientRatis getXceiverClient() {
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

  public List<ContainerCommandRequestProto> getReceivedPutBlocks() {
    return receivedPutBlocks;
  }

  public int getWatchForCommitCount() {
    return watchForCommitCount.get();
  }

  /** Concatenate all received chunks into a single byte array. */
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

  // --- Failure injection ---

  public FakeDatanodePipeline failChunkAfter(int n, Supplier<Throwable> err) {
    this.chunkFailAfter = n;
    this.chunkFailure = err;
    return this;
  }

  public FakeDatanodePipeline failPutBlockAfter(int n, Supplier<Throwable> err) {
    this.putBlockFailAfter = n;
    this.putBlockFailure = err;
    return this;
  }

  public FakeDatanodePipeline failWatchAfter(int n, Supplier<Throwable> err) {
    this.watchFailAfter = n;
    this.watchFailure = err;
    return this;
  }

  // --- Helpers ---

  private static ContainerCommandResponseProto buildPutBlockResponse(BlockID blockID) {
    return ContainerCommandResponseProto.newBuilder()
        .setCmdType(Type.PutBlock)
        .setResult(Result.SUCCESS)
        .setPutBlock(PutBlockResponseProto.newBuilder()
            .setCommittedBlockLength(
                GetCommittedBlockLengthResponseProto.newBuilder()
                    .setBlockID(blockID.getDatanodeBlockIDProtobuf())
                    .setBlockLength(0)
                    .build())
            .build())
        .build();
  }

  /**
   * Concrete DataStreamOutput implementation that records data chunks and supports failure injection.
   *
   * <p>Filters out close-protocol writes from {@code executePutBlockClose}, which sends 2 writes:
   * a putBlock protobuf (no special option) followed by a proto-length trailer with {@link StandardWriteOption#CLOSE}.
   * When we see CLOSE, we remove the preceding putBlock write and skip the current one, so only real data chunks are
   * recorded.
   */
  private class FakeDataStreamOutput implements DataStreamOutput {

    private boolean containsCloseOption(Iterable<WriteOption> options) {
      return Iterables.any(options, opt -> opt == StandardWriteOption.CLOSE);
    }

    @Override
    public CompletableFuture<DataStreamReply> writeAsync(ByteBuffer src, Iterable<WriteOption> options) {
      int size = src.remaining();
      if (containsCloseOption(options)) {
        // CLOSE write (proto-length trailer from executePutBlockClose).
        // Remove the preceding putBlock write that was recorded, then skip recording this one too.
        if (!receivedChunks.isEmpty()) {
          receivedChunks.remove(receivedChunks.size() - 1);
        }
        // Consume the buffer to advance its position
        src.position(src.limit());
        DataStreamReply reply = new SimpleDataStreamReply(true, size);
        return CompletableFuture.completedFuture(reply);
      }

      int count = chunkCount.incrementAndGet();
      if (count > chunkFailAfter && chunkFailure != null) {
        CompletableFuture<DataStreamReply> f = new CompletableFuture<>();
        f.completeExceptionally(chunkFailure.get());
        return f;
      }
      byte[] data = new byte[size];
      src.get(data);
      receivedChunks.add(data);
      DataStreamReply reply = new SimpleDataStreamReply(true, data.length);
      return CompletableFuture.completedFuture(reply);
    }

    @Override
    public CompletableFuture<DataStreamReply> writeAsync(FilePositionCount filePositionCount, WriteOption... options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<RaftClientReply> getRaftClientReplyFuture() {
      throw new UnsupportedOperationException();
    }

    @Override
    public WritableByteChannel getWritableByteChannel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DataStreamReply> closeAsync() {
      return CompletableFuture.completedFuture(new SimpleDataStreamReply(true, 0));
    }
  }

  /**
   * Minimal DataStreamReply implementation for test purposes.
   */
  static class SimpleDataStreamReply implements DataStreamReply {
    private final boolean success;
    private final long bytesWritten;

    SimpleDataStreamReply(boolean success, long bytesWritten) {
      this.success = success;
      this.bytesWritten = bytesWritten;
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    @Override
    public long getBytesWritten() {
      return bytesWritten;
    }

    @Override
    public Collection<CommitInfoProto> getCommitInfos() {
      return Collections.emptyList();
    }

    @Override
    public ClientId getClientId() {
      return ClientId.randomId();
    }

    @Override
    public DataStreamPacketHeaderProto.Type getType() {
      return DataStreamPacketHeaderProto.Type.STREAM_DATA;
    }

    @Override
    public long getStreamId() {
      return 0;
    }

    @Override
    public long getStreamOffset() {
      return 0;
    }

    @Override
    public long getDataLength() {
      return bytesWritten;
    }
  }
}
