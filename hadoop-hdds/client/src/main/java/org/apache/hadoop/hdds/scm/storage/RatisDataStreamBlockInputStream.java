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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockResponseProto;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.client.api.DataStreamInput;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.ReferenceCountedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads RATIS blocks exclusively through the Ratis data stream read-only API.
 * <p>
 * A datanode sends the whole range of a ReadBlock request, and closing a read-only stream does not stop it. So a
 * sequential reader requests at most {@code ozone.client.ratis.stream.read.window-size} bytes ahead of its position,
 * as two pipelined requests of half the window each: the datanode keeps sending the second while the reader consumes
 * the first. This bounds both the bytes wasted when the reader stops or seeks and the replies buffered per stream.
 */
public class RatisDataStreamBlockInputStream extends BlockExtendedInputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(RatisDataStreamBlockInputStream.class);
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  private final BlockID blockID;
  private final long blockLength;
  private final AtomicReference<Pipeline> pipelineRef = new AtomicReference<>();
  private final AtomicReference<Token<OzoneBlockTokenIdentifier>> tokenRef =
      new AtomicReference<>();
  private final XceiverClientFactory xceiverClientFactory;
  private final boolean verifyChecksum;
  private final long readAheadRequestSize;
  private final int responseDataSize;
  private final Duration readTimeout;

  private XceiverClientRatis xceiverClient;
  /** The request being consumed. */
  private ReadRequest current;
  /** The read-ahead request for the bytes right after {@link #current}. */
  private ReadRequest next;
  private ByteBuffer buffer = EMPTY_BUFFER;
  private ReferenceCountedObject<DataStreamReply> retainedDataReply;
  private long position;
  /** Set once a request has been consumed to its end without a seek, so the reader is reading sequentially. */
  private boolean sequential;
  private boolean closed;

  public RatisDataStreamBlockInputStream(BlockID blockID, long length,
      Pipeline pipeline, Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverClientFactory,
      OzoneClientConfig config) {
    this.blockID = Objects.requireNonNull(blockID, "blockID == null");
    this.blockLength = length;
    pipelineRef.set(Objects.requireNonNull(pipeline, "pipeline == null"));
    tokenRef.set(token);
    this.xceiverClientFactory = Objects.requireNonNull(xceiverClientFactory,
        "xceiverClientFactory == null");
    Objects.requireNonNull(config, "config == null");
    this.verifyChecksum = config.isChecksumVerify();
    this.readAheadRequestSize = Math.max(1L, config.getRatisStreamReadWindowSize() / 2);
    this.responseDataSize = config.getStreamReadResponseDataSize();
    this.readTimeout = config.getStreamReadTimeout();
  }

  @Override
  public BlockID getBlockID() {
    return blockID;
  }

  @Override
  public long getLength() {
    return blockLength;
  }

  @Override
  public long getPos() {
    return position;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    Objects.requireNonNull(b, "b == null");
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    return read(ByteBuffer.wrap(b, off, len));
  }

  @Override
  public synchronized int read(ByteBuffer targetBuf) throws IOException {
    return readFully(targetBuf, true);
  }

  public synchronized int readFully(ByteBuffer targetBuf, boolean preRead)
      throws IOException {
    checkOpen();
    if (!targetBuf.hasRemaining()) {
      return 0;
    }
    int read = 0;
    while (targetBuf.hasRemaining() && position < blockLength) {
      if (!buffer.hasRemaining()) {
        releaseRetainedDataReply();
        buffer = readBlock(targetBuf.remaining(), preRead);
      }
      if (!buffer.hasRemaining()) {
        break;
      }

      final int toCopy = Math.min(buffer.remaining(), targetBuf.remaining());
      final ByteBuffer tmp = buffer.duplicate();
      tmp.limit(tmp.position() + toCopy);
      targetBuf.put(tmp);
      buffer.position(tmp.position());
      position += toCopy;
      read += toCopy;
      if (!buffer.hasRemaining()) {
        releaseRetainedDataReply();
        if (current != null && position >= current.end) {
          // All data of the request is consumed: close it now instead of waiting for its terminal reply, which would
          // keep the stream and the netty buffer holding that reply until the next read or seek.
          finishCurrent();
        }
      }
    }
    return read > 0 ? read : EOF;
  }

  @Override
  protected int readWithStrategy(ByteReaderStrategy strategy) {
    throw new NotImplementedException("readWithStrategy is not implemented.");
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    checkOpen();
    if (pos < 0) {
      throw new IOException("Cannot seek to negative offset");
    }
    if (pos > blockLength) {
      throw new EOFException("Failed to seek to position " + pos
          + " > block length = " + blockLength);
    }
    if (pos != position) {
      closeRequests();
      position = pos;
      discardBufferedData();
      sequential = false;
    }
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos)
      throws IOException {
    return false;
  }

  @Override
  public synchronized void unbuffer() {
    discardBufferedData();
    releaseClient(false);
  }

  @Override
  public synchronized void close() {
    closed = true;
    discardBufferedData();
    closeRequests();
    releaseClient(false);
  }

  private ByteBuffer readBlock(int length, boolean preRead) throws IOException {
    int leaderRedirects = 0;
    while (position < blockLength) {
      final boolean readAhead = preRead && sequential;
      if (current == null) {
        current = openRequest(position,
            requestLength(blockLength, position, length, readAhead, readAheadRequestSize));
      }
      if (readAhead && next == null && current.end < blockLength) {
        next = openRequest(current.end,
            requestLength(blockLength, current.end, 0, true, readAheadRequestSize));
      }

      final ReferenceCountedObject<DataStreamReply> ref = readReply(current);
      final DataStreamReply reply = ref.get();
      if (reply.getType() == Type.STREAM_DATA) {
        current.dataSeen = true;
        final ByteBuffer data = readDataReply(ref);
        if (data.hasRemaining()) {
          return data;
        }
      } else if (reply.getType() == Type.STREAM_HEADER) {
        final RaftPeer leader = handleTerminalReply(ref, current.dataSeen);
        if (leader != null) {
          redirectToLeader(leader, ++leaderRedirects);
          continue;
        }
        if (!current.dataSeen && position < blockLength) {
          throw new EOFException("ReadBlock stream returned no data for "
              + blockID + " at position " + position);
        }
        finishCurrent();
      } else {
        try {
          throw new IOException("Unexpected data stream reply type "
              + reply.getType() + " for " + blockID);
        } finally {
          ref.release();
        }
      }
    }
    return ByteBuffer.allocate(0);
  }

  /** Closes the consumed request and moves on to the read-ahead request, if any. */
  private void finishCurrent() {
    current.close(blockID);
    current = next;
    next = null;
    sequential = true;
  }

  private ReadRequest openRequest(long offset, long length) throws IOException {
    acquireClient();
    // Name the datanode the client streams to (the closest node of the client's pipeline): a datanode serves a
    // closed-container read only when the request names it, see ClosedContainerReadResolver.
    final ContainerCommandRequestProto request =
        ContainerProtocolCalls.buildReadBlockCommandProto(blockID, offset,
            length, responseDataSize, tokenRef.get(), xceiverClient.getPipeline());
    final ContainerCommandRequestMessage message =
        ContainerCommandRequestMessage.toMessage(request,
            TracingUtil.exportCurrentSpan());
    return new ReadRequest(offset + length, xceiverClient.getDataStreamApi()
        .streamReadOnly(message.getContent().asReadOnlyByteBuffer()));
  }

  private ReferenceCountedObject<DataStreamReply> readReply(ReadRequest request) throws IOException {
    try {
      return request.input.readAsync().get(readTimeout.toMillis(),
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted Ratis read-only data stream request",
          e);
    } catch (ExecutionException e) {
      releaseClient(true);
      final Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("Failed Ratis read-only data stream request",
          cause != null ? cause : e);
    } catch (TimeoutException e) {
      releaseClient(true);
      throw new IOException("Timed out waiting for Ratis read-only data "
          + "stream reply", e);
    }
  }

  private ByteBuffer readDataReply(ReferenceCountedObject<DataStreamReply> ref)
      throws IOException {
    final DataStreamReply reply = ref.get();
    boolean releaseReply = true;
    try {
      final ReadBlockData readBlockData = ReadBlockData.parse(reply);
      final ContainerCommandResponseProto response =
          readBlockData.getResponse();
      ContainerProtocolCalls.validateContainerResponse(response);
      if (!response.hasReadBlock()) {
        throw new IOException("Missing ReadBlock response: " + response);
      }

      final ReadBlockResponseProto readBlock = response.getReadBlock();
      final ByteBuffer dataBuffer = readBlockData.getData() != null ?
          readBlockData.getData() : readBlock.getData().asReadOnlyByteBuffer();
      if (verifyChecksum) {
        final ChecksumData checksumData =
            ChecksumData.getFromProtoBuf(readBlock.getChecksumData());
        Checksum.verifyChecksum(dataBuffer.duplicate(), checksumData, 0);
      }

      final long blockOffset = readBlock.getOffset();
      if (position < blockOffset) {
        throw new IOException("ReadBlock response is ahead of requested "
            + "position " + position + ", response offset " + blockOffset);
      }
      dataBuffer.position(Math.toIntExact(
          Math.min(position - blockOffset, dataBuffer.limit())));
      if (dataBuffer.hasRemaining()) {
        retainedDataReply = ref;
        releaseReply = false;
      }
      return dataBuffer;
    } catch (InvalidProtocolBufferException e) {
      releaseClient(true);
      throw new IOException("Failed to parse ReadBlock response", e);
    } finally {
      if (releaseReply) {
        ref.release();
      }
    }
  }

  /**
   * @return the suggested leader when a follower refused the read before sending data (reads of open containers
   *     are served by the Raft leader), or null when the stream completed successfully
   */
  private RaftPeer handleTerminalReply(ReferenceCountedObject<DataStreamReply> ref, boolean dataSeen)
      throws IOException {
    try {
      final RaftClientReply raftReply =
          ClientProtoUtils.getRaftClientReply(ref.get());
      if (raftReply.isSuccess()) {
        return null;
      }
      final NotLeaderException notLeader = raftReply.getNotLeaderException();
      if (notLeader != null && notLeader.getSuggestedLeader() != null && !dataSeen) {
        return notLeader.getSuggestedLeader();
      }
      throw new IOException("Failed Ratis read-only data stream request",
          raftReply.getException());
    } finally {
      ref.release();
    }
  }

  /** Moves the suggested leader to the front of the pipeline, so the next stream is opened on it. */
  private void redirectToLeader(RaftPeer leader, int attempt) throws IOException {
    final Pipeline pipeline = pipelineRef.get();
    final DatanodeDetails target = pipeline.getNodes().stream()
        .filter(dn -> RatisHelper.toRaftPeerId(dn).equals(leader.getId()))
        .findFirst()
        .orElse(null);
    if (target == null || attempt > pipeline.getNodes().size()) {
      throw new IOException("Failed Ratis read-only data stream request for " + blockID
          + ": not leader, suggested leader " + leader + ", attempt " + attempt + ", " + pipeline);
    }
    final List<DatanodeDetails> nodes = new ArrayList<>(pipeline.getNodes());
    nodes.remove(target);
    nodes.add(0, target);
    releaseClient(false);
    pipelineRef.set(pipeline.copyWithNodesInOrder(nodes));
    LOG.debug("Redirecting Ratis read-only data stream for {} to leader {}", blockID, target);
  }

  private synchronized void acquireClient() throws IOException {
    checkOpen();
    if (xceiverClient == null) {
      // Topology aware: one cached client per target datanode (the pipeline's closest node), so a stream can be
      // redirected to the Raft leader.
      final XceiverClientSpi client =
          xceiverClientFactory.acquireClient(pipelineRef.get(), true);
      if (!(client instanceof XceiverClientRatis)) {
        xceiverClientFactory.releaseClient(client, false, true);
        throw new IOException("Unexpected client class: "
            + client.getClass().getName() + ", " + pipelineRef.get());
      }
      xceiverClient = (XceiverClientRatis) client;
    }
  }

  private synchronized void releaseClient(boolean invalidateClient) {
    discardBufferedData();
    sequential = false;
    if (xceiverClient != null) {
      closeRequests();
      xceiverClientFactory.releaseClient(xceiverClient, invalidateClient, true);
      xceiverClient = null;
    }
  }

  private synchronized void closeRequests() {
    if (current != null) {
      current.close(blockID);
      current = null;
    }
    if (next != null) {
      next.close(blockID);
      next = null;
    }
  }

  private void discardBufferedData() {
    buffer = EMPTY_BUFFER;
    releaseRetainedDataReply();
  }

  private void releaseRetainedDataReply() {
    if (retainedDataReply != null) {
      retainedDataReply.release();
      retainedDataReply = null;
    }
  }

  /**
   * @return the length of a request at {@code offset}: exactly the {@code length} the caller asked for, since a read
   *     after a seek may be followed by another seek; at least {@code readAheadRequestSize} when reading ahead
   */
  static long requestLength(long blockLength, long offset, int length, boolean readAhead,
      long readAheadRequestSize) {
    final long wanted = readAhead ? Math.max(length, readAheadRequestSize) : Math.max(1L, length);
    return Math.min(blockLength - offset, wanted);
  }

  private void checkOpen() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed for block " + blockID);
    }
  }

  /** A ReadBlock request for the block range ending at {@link #end}, served over its own read-only stream. */
  private static final class ReadRequest {
    private final long end;
    private final DataStreamInput input;
    private boolean dataSeen;

    ReadRequest(long end, DataStreamInput input) {
      this.end = end;
      this.input = input;
    }

    void close(BlockID blockID) {
      try {
        input.close();
      } catch (IOException e) {
        LOG.debug("Failed to close Ratis read-only stream for {}", blockID, e);
      }
    }
  }
}
