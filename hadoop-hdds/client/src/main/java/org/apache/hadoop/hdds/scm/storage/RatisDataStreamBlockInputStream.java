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

import com.google.common.annotations.VisibleForTesting;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
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
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuf;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReferenceCountedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads RATIS blocks exclusively through the Ratis data stream read-only API.
 */
public class RatisDataStreamBlockInputStream extends BlockExtendedInputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(RatisDataStreamBlockInputStream.class);
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
  private static final int RATIS_READ_BLOCK_STREAM_HEADER_BYTES =
      Integer.BYTES;

  private final BlockID blockID;
  private final long blockLength;
  private final AtomicReference<Pipeline> pipelineRef = new AtomicReference<>();
  private final AtomicReference<Token<OzoneBlockTokenIdentifier>> tokenRef =
      new AtomicReference<>();
  private final XceiverClientFactory xceiverClientFactory;
  private final boolean verifyChecksum;
  private final long preReadSize;
  private final long readWindowSize;
  private final int responseDataSize;
  private final Duration readTimeout;

  private XceiverClientRatis xceiverClient;
  private DataStreamInput streamInput;
  private ByteBuffer buffer = EMPTY_BUFFER;
  private ReferenceCountedObject<DataStreamReply> retainedDataReply;
  private long position;
  private boolean streamDataSeen;
  private boolean largeReadWindowEligible;
  private boolean closed;

  public RatisDataStreamBlockInputStream(BlockID blockID, long length,
      Pipeline pipeline, Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverClientFactory,
      OzoneClientConfig config) throws IOException {
    this.blockID = Objects.requireNonNull(blockID, "blockID == null");
    this.blockLength = length;
    pipelineRef.set(setRatisPipeline(pipeline));
    tokenRef.set(token);
    this.xceiverClientFactory = Objects.requireNonNull(xceiverClientFactory,
        "xceiverClientFactory == null");
    Objects.requireNonNull(config, "config == null");
    this.verifyChecksum = config.isChecksumVerify();
    this.preReadSize = config.getStreamReadPreReadSize();
    this.readWindowSize = config.getRatisStreamReadWindowSize();
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
      closeStream();
      position = pos;
      discardBufferedData();
      largeReadWindowEligible = false;
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
    closeStream();
    releaseClient(false);
  }

  private ByteBuffer readBlock(int length, boolean preRead) throws IOException {
    while (position < blockLength) {
      if (streamInput == null) {
        streamDataSeen = false;
        streamInput = openStream(length, preRead);
      }

      final ReferenceCountedObject<DataStreamReply> ref = readReply();
      final DataStreamReply reply = ref.get();
      if (reply.getType() == Type.STREAM_DATA) {
        streamDataSeen = true;
        final ByteBuffer data = readDataReply(ref);
        if (data.hasRemaining()) {
          return data;
        }
      } else if (reply.getType() == Type.STREAM_HEADER) {
        handleTerminalReply(ref);
        if (!streamDataSeen && position < blockLength) {
          throw new EOFException("ReadBlock stream returned no data for "
              + blockID + " at position " + position);
        }
        largeReadWindowEligible = streamDataSeen;
        closeStream();
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

  private DataStreamInput openStream(int length, boolean preRead)
      throws IOException {
    final long readLength = computeReadLength(blockLength, position, length,
        preRead, largeReadWindowEligible, preReadSize, readWindowSize);
    final ContainerCommandRequestProto request =
        ContainerProtocolCalls.buildReadBlockCommandProto(blockID, position,
            readLength, responseDataSize, tokenRef.get(), pipelineRef.get());
    acquireClient();
    final ContainerCommandRequestMessage message =
        ContainerCommandRequestMessage.toMessage(request,
            TracingUtil.exportCurrentSpan());
    return xceiverClient.getDataStreamApi()
        .streamReadOnly(message.getContent().asReadOnlyByteBuffer());
  }

  private ReferenceCountedObject<DataStreamReply> readReply() throws IOException {
    try {
      return streamInput.readAsync().get(readTimeout.toMillis(),
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
      final ReadBlockData readBlockData = parseReadBlockData(reply);
      final ContainerCommandResponseProto response = readBlockData.response;
      ContainerProtocolCalls.validateContainerResponse(response);
      if (!response.hasReadBlock()) {
        throw new IOException("Missing ReadBlock response: " + response);
      }

      final ReadBlockResponseProto readBlock = response.getReadBlock();
      final ByteBuffer dataBuffer = readBlockData.data != null ?
          readBlockData.data : readBlock.getData().asReadOnlyByteBuffer();
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
      if (readBlockData.retainReply && dataBuffer.hasRemaining()) {
        retainedDataReply = ref;
        releaseReply = false;
      }
      return dataBuffer;
    } finally {
      if (releaseReply) {
        ref.release();
      }
    }
  }

  private void handleTerminalReply(ReferenceCountedObject<DataStreamReply> ref)
      throws IOException {
    try {
      final RaftClientReply raftReply =
          ClientProtoUtils.getRaftClientReply(ref.get());
      if (!raftReply.isSuccess()) {
        throw new IOException("Failed Ratis read-only data stream request",
            raftReply.getException());
      }
    } finally {
      ref.release();
    }
  }

  private ReadBlockData parseReadBlockData(DataStreamReply reply)
      throws IOException {
    try {
      if (reply instanceof DataStreamReplyByteBuffer) {
        return parseReadBlockData(((DataStreamReplyByteBuffer) reply).slice(),
            false);
      } else if (reply instanceof DataStreamReplyByteBuf) {
        return parseReadBlockData(((DataStreamReplyByteBuf) reply).slice(),
            true);
      }
      throw new IOException("Unexpected reply class " + reply.getClass()
          + " for " + blockID);
    } catch (InvalidProtocolBufferException e) {
      releaseClient(true);
      throw new IOException("Failed to parse ReadBlock response", e);
    }
  }

  private ReadBlockData parseReadBlockData(ByteBuf buf, boolean retainReply)
      throws InvalidProtocolBufferException {
    if (buf.readableBytes() < RATIS_READ_BLOCK_STREAM_HEADER_BYTES) {
      throw new InvalidProtocolBufferException(
          "Missing Ratis ReadBlock metadata length");
    }
    final int metadataLength = buf.getInt(buf.readerIndex());
    if (metadataLength < 0
        || metadataLength > buf.readableBytes()
            - RATIS_READ_BLOCK_STREAM_HEADER_BYTES) {
      throw new InvalidProtocolBufferException(
          "Invalid Ratis ReadBlock metadata length " + metadataLength);
    }
    final int metadataOffset =
        buf.readerIndex() + RATIS_READ_BLOCK_STREAM_HEADER_BYTES;
    final int dataOffset = metadataOffset + metadataLength;
    final int dataLength =
        buf.readerIndex() + buf.readableBytes() - dataOffset;
    final ContainerCommandResponseProto response =
        ContainerCommandResponseProto.parseFrom(
            buf.slice(metadataOffset, metadataLength).nioBuffer());
    final ByteBuffer data =
        buf.slice(dataOffset, dataLength).nioBuffer();
    return new ReadBlockData(response, data, retainReply);
  }

  private ReadBlockData parseReadBlockData(ByteBuffer buffer,
      boolean retainReply) throws InvalidProtocolBufferException {
    final ByteBuffer duplicate = buffer.duplicate();
    if (duplicate.remaining() < RATIS_READ_BLOCK_STREAM_HEADER_BYTES) {
      throw new InvalidProtocolBufferException(
          "Missing Ratis ReadBlock metadata length");
    }
    final int metadataLength = duplicate.getInt(duplicate.position());
    if (metadataLength < 0
        || metadataLength > duplicate.remaining()
            - RATIS_READ_BLOCK_STREAM_HEADER_BYTES) {
      throw new InvalidProtocolBufferException(
          "Invalid Ratis ReadBlock metadata length " + metadataLength);
    }
    duplicate.position(
        duplicate.position() + RATIS_READ_BLOCK_STREAM_HEADER_BYTES);
    final ByteBuffer metadata = duplicate.slice();
    metadata.limit(metadataLength);
    duplicate.position(duplicate.position() + metadataLength);
    final ByteBuffer data = duplicate.slice();
    return new ReadBlockData(
        ContainerCommandResponseProto.parseFrom(metadata), data, retainReply);
  }

  private static final class ReadBlockData {
    private final ContainerCommandResponseProto response;
    private final ByteBuffer data;
    private final boolean retainReply;

    private ReadBlockData(ContainerCommandResponseProto response,
        ByteBuffer data, boolean retainReply) {
      this.response = response;
      this.data = data;
      this.retainReply = retainReply;
    }
  }

  private synchronized void acquireClient() throws IOException {
    checkOpen();
    if (xceiverClient == null) {
      final XceiverClientSpi client =
          xceiverClientFactory.acquireClientForReadData(pipelineRef.get());
      if (!(client instanceof XceiverClientRatis)) {
        xceiverClientFactory.releaseClientForReadData(client, false);
        throw new IOException("Unexpected client class: "
            + client.getClass().getName() + ", " + pipelineRef.get());
      }
      xceiverClient = (XceiverClientRatis) client;
    }
  }

  private synchronized void releaseClient(boolean invalidateClient) {
    discardBufferedData();
    largeReadWindowEligible = false;
    if (xceiverClient != null) {
      closeStream();
      xceiverClientFactory.releaseClientForReadData(xceiverClient,
          invalidateClient);
      xceiverClient = null;
    }
  }

  private synchronized void closeStream() {
    if (streamInput != null) {
      try {
        streamInput.close();
      } catch (IOException e) {
        LOG.debug("Failed to close Ratis read-only stream for {}", blockID, e);
      } finally {
        streamInput = null;
        streamDataSeen = false;
      }
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

  private static long addWithSaturation(long left, long right) {
    final long result = left + right;
    return result < 0 ? Long.MAX_VALUE : result;
  }

  private static long computeReadLength(long blockLength, long position,
      int length, boolean preRead, boolean largeReadWindowEligible,
      long preReadSize, long readWindowSize) {
    final long requestedLength = Math.max(1L, (long) length);
    long wantedLength = requestedLength;
    if (preRead && largeReadWindowEligible) {
      wantedLength = Math.max(addWithSaturation(requestedLength, preReadSize),
          readWindowSize);
    }
    return Math.min(blockLength - position, wantedLength);
  }

  @VisibleForTesting
  static long computeReadLengthForTesting(long blockLength, long position,
      int length, boolean preRead, boolean largeReadWindowEligible,
      long preReadSize, long readWindowSize) {
    return computeReadLength(blockLength, position, length, preRead,
        largeReadWindowEligible, preReadSize, readWindowSize);
  }

  private void checkOpen() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed for block " + blockID);
    }
  }

  private static Pipeline setRatisPipeline(Pipeline pipeline) throws IOException {
    Objects.requireNonNull(pipeline, "pipeline == null");
    Preconditions.assertTrue(
        pipeline.getType() == HddsProtos.ReplicationType.RATIS,
        () -> "Expected RATIS pipeline but got " + pipeline);
    for (DatanodeDetails dn : pipeline.getNodes()) {
      if (!dn.hasPort(DatanodeDetails.Port.Name.RATIS_DATASTREAM)) {
        throw new IOException("RATIS_DATASTREAM port is missing for datanode "
            + dn + " in pipeline " + pipeline.getId());
      }
    }
    return pipeline;
  }
}
