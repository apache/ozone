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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBufOutputStream;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.util.ResourceLeakDetector;

/**
 * Minimal xceiver client for {@link BlockOutputStreamWriteBenchmark}.
 *
 * <p>Replicates the production serialization path without real network I/O:
 * the proto is serialized via {@code request.writeTo(OutputStream)} — which
 * causes protobuf to use {@code OutputStreamEncoder}, the same encoder gRPC's
 * {@code MessageFramer} uses — into a pooled direct {@code ByteBuf}, then the
 * buffer is released immediately.
 *
 * <p>This exercises the full cross-domain copy chain:
 * <ul>
 *   <li><b>direct chunk (pre-patch):</b> {@code NioByteString.writeTo} allocates
 *       a temporary {@code byte[]}, copies from off-heap via {@code copyMemory}
 *       (copy 1), then writes heap→direct ByteBuf (copy 2).</li>
 *   <li><b>heap chunk (this patch):</b> {@code BoundedByteString.writeTo} writes
 *       directly heap→direct ByteBuf (copy 1 only).</li>
 * </ul>
 */
final class BenchmarkMockXceiverClient extends XceiverClientSpi {

  static {
    // The thread-local ByteBufs are never released (they live for the worker
    // thread's lifetime), which triggers ResourceLeakDetector warnings when
    // benchmark thread pools are torn down between phases.  Disable detection
    // for this benchmark-only class to keep output clean.
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
  }

  // One reusable direct buffer per worker thread — eliminates the per-call
  // PoolArena.allocateHuge + ByteBuffer.allocateDirect + OS zero-fill cycle
  // that dominates CPU at large (≥4 MB) write sizes.
  private static final ThreadLocal<ByteBuf> FRAME_BUF = ThreadLocal.withInitial(
      () -> PooledByteBufAllocator.DEFAULT.directBuffer(4 * 1024 * 1024 + 1024));

  private final Pipeline pipeline;
  private final AtomicLong logIndex = new AtomicLong();
  private final long commitLatencyNs;

  BenchmarkMockXceiverClient(Pipeline pipeline) {
    this(pipeline, 0);
  }

  BenchmarkMockXceiverClient(Pipeline pipeline, long commitLatencyNs) {
    this.pipeline = pipeline;
    this.commitLatencyNs = commitLatencyNs;
  }

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
  public XceiverClientReply sendCommandAsync(ContainerCommandRequestProto request) {
    // Replicate: gRPC MessageFramer allocates a pooled direct ByteBuf via
    // NettyWritableBufferAllocator, then serializes the proto into it through
    // OutputStreamEncoder → WritableBufferOutputStream → ByteBuf.writeBytes().
    // For NioByteString (direct chunk data): copyMemory(direct→heap tmp) + write(heap→direct).
    // For BoundedByteString (heap chunk data): write(heap→direct) only.
    // Reuse a thread-local direct buffer rather than allocating per call:
    // ≥4 MB requests exceed PoolArena's chunk size and fall into allocateHuge,
    // which calls ByteBuffer.allocateDirect + OS zero-fill on every invocation.
    final ByteBuf frame = FRAME_BUF.get();
    frame.clear();
    frame.ensureWritable(request.getSerializedSize());
    try {
      request.writeTo(new ByteBufOutputStream(frame));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final ContainerCommandResponseProto.Builder builder =
        ContainerCommandResponseProto.newBuilder()
            .setResult(Result.SUCCESS)
            .setCmdType(request.getCmdType());
    if (request.getCmdType() == Type.PutBlock) {
      builder.setPutBlock(PutBlockResponseProto.newBuilder()
          .setCommittedBlockLength(
              GetCommittedBlockLengthResponseProto.newBuilder()
                  .setBlockID(request.getPutBlock().getBlockData().getBlockID())
                  .setBlockLength(request.getPutBlock().getBlockData().getSize())
                  .build())
          .build());
    }
    final XceiverClientReply reply =
        new XceiverClientReply(CompletableFuture.completedFuture(builder.build()));
    reply.setLogIndex(logIndex.incrementAndGet());
    return reply;
  }

  @Override
  public ReplicationType getPipelineType() {
    return pipeline.getType();
  }

  @Override
  public CompletableFuture<XceiverClientReply> watchForCommit(long index) {
    if (commitLatencyNs > 0) {
      LockSupport.parkNanos(commitLatencyNs);
    }
    final ContainerCommandResponseProto response =
        ContainerCommandResponseProto.newBuilder()
            .setCmdType(Type.WriteChunk)
            .setResult(Result.SUCCESS)
            .build();
    final XceiverClientReply reply =
        new XceiverClientReply(CompletableFuture.completedFuture(response));
    reply.setLogIndex(index);
    return CompletableFuture.completedFuture(reply);
  }

  @Override
  public long getReplicatedMinCommitIndex() {
    return logIndex.get();
  }

  @Override
  public Map<DatanodeDetails, ContainerCommandResponseProto> sendCommandOnAllNodes(
      ContainerCommandRequestProto request) {
    return null;
  }
}
