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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 * Reads RATIS blocks exclusively through the Ratis data stream read-only API. The reading, seeking and buffering are
 * those of {@link StreamBlockInputStream}; this class replaces its gRPC stream with Ratis read-only streams.
 * <p>
 * A datanode sends the whole range of a ReadBlock request, and closing a read-only stream does not stop it. So a
 * sequential reader requests at most {@code ozone.client.ratis.stream.read.window-size} bytes ahead of its position,
 * as two pipelined requests of half the window each: the datanode keeps sending the second while the reader consumes
 * the first. This bounds both the bytes wasted when the reader stops or seeks and the replies buffered per stream.
 * The read-ahead starts at {@link #INITIAL_READ_AHEAD} and doubles with each request the reader consumes, so a short
 * run of small reads (such as Parquet reading its page indexes) wastes little at its next seek, while a long scan
 * (such as HBase reading 64 KB blocks) soon reads ahead the full half window.
 * A reader that asks for at least half the window in one read (Parquet reads a run of column chunks in 8 MB buffers)
 * already reads in large pieces, so it gets exactly what it asks for until its next seek.
 */
public class RatisDataStreamBlockInputStream extends StreamBlockInputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(RatisDataStreamBlockInputStream.class);
  /** The first read-ahead request of a sequential run, before it doubles up to {@link #readAheadRequestSize}. */
  static final long INITIAL_READ_AHEAD = 128 << 10;

  private final long readAheadRequestSize;

  private XceiverClientRatis xceiverClient;
  /** The request being consumed. */
  private ReadRequest current;
  /** The read-ahead request for the bytes right after {@link #current}. */
  private ReadRequest next;
  /** The buffered data, released as soon as it is consumed or dropped. */
  private DataReply dataReply;
  /** Set once a request has been consumed to its end without a seek, so the reader is reading sequentially. */
  private boolean sequential;
  /** Set when a read since the last seek asked for at least {@link #readAheadRequestSize}: no read-ahead then. */
  private boolean largeReads;
  /** The size of the next read-ahead request; 0 until the reader reads sequentially. */
  private long readAheadSize;

  public RatisDataStreamBlockInputStream(BlockID blockID, long length,
      Pipeline pipeline, Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverClientFactory,
      OzoneClientConfig config) throws IOException {
    super(blockID, length, pipeline, token, xceiverClientFactory, null, config);
    this.readAheadRequestSize = Math.max(1L, config.getRatisStreamReadWindowSize() / 2);
  }

  /** Keeps the RATIS pipeline, which the data stream is opened on, instead of converting it for standalone reads. */
  @Override
  protected Pipeline setPipeline(Pipeline pipeline) {
    return Objects.requireNonNull(pipeline, "pipeline == null");
  }

  @Override
  synchronized ReadBuffer readNext(int length, boolean preRead) throws IOException {
    releaseDataReply();
    int leaderRedirects = 0;
    largeReads |= length >= readAheadRequestSize;
    while (getPos() < getLength()) {
      final boolean readAhead = preRead && sequential && !largeReads;
      if (readAhead && readAheadSize == 0) {
        readAheadSize = Math.min(INITIAL_READ_AHEAD, readAheadRequestSize);
      }
      if (current == null) {
        current = openRequest(getPos(),
            requestLength(getLength(), getPos(), length, readAhead, readAheadSize));
      }
      if (readAhead && next == null && current.end < getLength()) {
        next = openRequest(current.end,
            requestLength(getLength(), current.end, 0, true, readAheadSize));
      }

      final ReferenceCountedObject<DataStreamReply> ref = readReply(current);
      final DataStreamReply reply = ref.get();
      if (reply.getType() == Type.STREAM_DATA) {
        current.dataSeen = true;
        dataReply = readDataReply(ref);
        if (dataReply != null) {
          return dataReply;
        }
      } else if (reply.getType() == Type.STREAM_HEADER) {
        final RaftPeer leader = handleTerminalReply(ref, current.dataSeen);
        if (leader != null) {
          redirectToLeader(leader, ++leaderRedirects);
          continue;
        }
        if (!current.dataSeen && getPos() < getLength()) {
          throw new EOFException("ReadBlock stream returned no data for "
              + getBlockID() + " at position " + getPos());
        }
        finishCurrent();
      } else {
        try {
          throw new IOException("Unexpected data stream reply type "
              + reply.getType() + " for " + getBlockID());
        } finally {
          ref.release();
        }
      }
    }
    return null;
  }

  @Override
  synchronized void advancePosition(long delta, boolean preRead) {
    super.advancePosition(delta, preRead);
    if (dataReply != null && !dataReply.getByteBuffer().hasRemaining()) {
      releaseDataReply();
      if (current != null && getPos() >= current.end) {
        // All data of the request is consumed: close it now instead of waiting for its terminal reply, which would
        // keep the stream and the netty buffer holding that reply until the next read or seek.
        finishCurrent();
      }
    }
  }

  /** A seek closes the requests in flight, so the first read after it requests exactly what the reader asks for. */
  @Override
  synchronized ReadBuffer seekReader(ReadBuffer buffered, long pos) {
    closeReads("seek");
    return null;
  }

  @Override
  synchronized void closeReads(String reason) {
    releaseDataReply();
    closeRequests();
    sequential = false;
    largeReads = false;
    readAheadSize = 0;
  }

  /** Closes the consumed request and moves on to the read-ahead request, if any. */
  private void finishCurrent() {
    current.close(getBlockID());
    current = next;
    next = null;
    sequential = true;
    if (readAheadSize > 0) {
      readAheadSize = Math.min(2 * readAheadSize, readAheadRequestSize);
    }
  }

  private ReadRequest openRequest(long offset, long length) throws IOException {
    acquireClient();
    // Name the datanode the client streams to (the closest node of the client's pipeline): a datanode serves a
    // closed-container read only when the request names it, see ClosedContainerReadResolver.
    final ContainerCommandRequestProto request =
        ContainerProtocolCalls.buildReadBlockCommandProto(getBlockID(), offset,
            length, getResponseDataSize(), getToken(), xceiverClient.getPipeline());
    final ContainerCommandRequestMessage message =
        ContainerCommandRequestMessage.toMessage(request,
            TracingUtil.exportCurrentSpan());
    return new ReadRequest(offset + length, xceiverClient.getDataStreamApi()
        .streamReadOnly(message.getContent().asReadOnlyByteBuffer()));
  }

  private ReferenceCountedObject<DataStreamReply> readReply(ReadRequest request) throws IOException {
    try {
      return request.input.readAsync().get(getReadTimeout().toMillis(),
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

  /** @return the data of the reply from the reader's position, or null when the reply has no data there */
  private DataReply readDataReply(ReferenceCountedObject<DataStreamReply> ref)
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
      validateChecksums(readBlock, dataBuffer);

      final long blockOffset = readBlock.getOffset();
      if (getPos() < blockOffset) {
        throw new IOException("ReadBlock response is ahead of requested "
            + "position " + getPos() + ", response offset " + blockOffset);
      }
      dataBuffer.position(Math.toIntExact(
          Math.min(getPos() - blockOffset, dataBuffer.limit())));
      if (!dataBuffer.hasRemaining()) {
        return null;
      }
      releaseReply = false;
      return new DataReply(readBlock, dataBuffer, ref);
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
    final Pipeline pipeline = getPipeline();
    final DatanodeDetails target = pipeline.getNodes().stream()
        .filter(dn -> RatisHelper.toRaftPeerId(dn).equals(leader.getId()))
        .findFirst()
        .orElse(null);
    if (target == null || attempt > pipeline.getNodes().size()) {
      throw new IOException("Failed Ratis read-only data stream request for " + getBlockID()
          + ": not leader, suggested leader " + leader + ", attempt " + attempt + ", " + pipeline);
    }
    final List<DatanodeDetails> nodes = new ArrayList<>(pipeline.getNodes());
    nodes.remove(target);
    nodes.add(0, target);
    releaseClient(false);
    updatePipeline(pipeline.copyWithNodesInOrder(nodes));
    LOG.debug("Redirecting Ratis read-only data stream for {} to leader {}", getBlockID(), target);
  }

  @Override
  protected synchronized void acquireClient() throws IOException {
    checkOpen();
    if (xceiverClient == null) {
      // Topology aware: one cached client per target datanode (the pipeline's closest node), so a stream can be
      // redirected to the Raft leader.
      final XceiverClientSpi client =
          getXceiverClientFactory().acquireClient(getPipeline(), true);
      if (!(client instanceof XceiverClientRatis)) {
        getXceiverClientFactory().releaseClient(client, false, true);
        throw new IOException("Unexpected client class: "
            + client.getClass().getName() + ", " + getPipeline());
      }
      xceiverClient = (XceiverClientRatis) client;
    }
  }

  @Override
  protected synchronized void releaseClient() {
    releaseClient(false);
  }

  private synchronized void releaseClient(boolean invalidateClient) {
    closeReader("releaseClient");
    if (xceiverClient != null) {
      getXceiverClientFactory().releaseClient(xceiverClient, invalidateClient, true);
      xceiverClient = null;
    }
  }

  private synchronized void closeRequests() {
    if (current != null) {
      current.close(getBlockID());
      current = null;
    }
    if (next != null) {
      next.close(getBlockID());
      next = null;
    }
  }

  private void releaseDataReply() {
    if (dataReply != null) {
      dataReply.release();
      dataReply = null;
    }
  }

  /**
   * @return the length of a request at {@code offset}: exactly the {@code length} the caller asked for, since a read
   *     after a seek may be followed by another seek; at least {@code readAheadSize} when reading ahead
   */
  static long requestLength(long blockLength, long offset, int length, boolean readAhead,
      long readAheadSize) {
    final long wanted = readAhead ? Math.max(length, readAheadSize) : Math.max(1L, length);
    return Math.min(blockLength - offset, wanted);
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

  /** The data of a reply, which keeps the netty buffer the reply was received in until it is released. */
  private static final class DataReply extends ReadBuffer {
    private final ReferenceCountedObject<DataStreamReply> reply;

    DataReply(ReadBlockResponseProto readBlock, ByteBuffer data, ReferenceCountedObject<DataStreamReply> reply) {
      super(readBlock, data);
      this.reply = reply;
    }

    void release() {
      reply.release();
    }
  }
}
