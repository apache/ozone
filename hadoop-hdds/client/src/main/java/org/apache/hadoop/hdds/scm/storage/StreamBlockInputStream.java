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

import static org.apache.ratis.thirdparty.io.grpc.Status.Code.CANCELLED;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockResponseProto;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamingReadResponse;
import org.apache.hadoop.hdds.scm.StreamingReaderSpi;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCallStreamObserver;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link java.io.InputStream} called from KeyInputStream to read a block from the
 * container.
 */
public class StreamBlockInputStream extends BlockExtendedInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(StreamBlockInputStream.class);
  private static final int EOF = -1;
  private static final String STREAM_CLOSE_REASON = "StreamBlockInputStream closed";
  private static final AtomicInteger STREAM_ID = new AtomicInteger(0);
  private static final AtomicInteger READER_ID = new AtomicInteger(0);

  private final String name = "stream" + STREAM_ID.getAndIncrement();
  private final BlockID blockID;
  private final long blockLength;
  private final int responseDataSize;
  private final long preReadSize;
  private final Duration readTimeout;
  private final long readTimeoutNanos;
  private final AtomicReference<Pipeline> pipelineRef = new AtomicReference<>();
  private final AtomicReference<Token<OzoneBlockTokenIdentifier>> tokenRef = new AtomicReference<>();
  private XceiverClientFactory xceiverClientFactory;
  private XceiverClientGrpc xceiverClient;

  private ReadBuffer readBuffer;
  private long position = 0;
  private long requestedLength = 0;
  private StreamingReader streamingReader;

  private final boolean verifyChecksum;
  private final Function<BlockID, BlockLocationInfo> refreshFunction;
  private final RetryPolicy retryPolicy;
  private int retries = 0;

  public StreamBlockInputStream(
      BlockID blockID, long length, Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverClientFactory,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      OzoneClientConfig config) throws IOException {
    this.blockID = blockID;
    this.blockLength = length;
    pipelineRef.set(setPipeline(pipeline));
    tokenRef.set(token);
    this.xceiverClientFactory = xceiverClientFactory;
    this.verifyChecksum = config.isChecksumVerify();
    this.retryPolicy = getReadRetryPolicy(config);
    this.refreshFunction = refreshFunction;
    this.preReadSize = config.getStreamReadPreReadSize();
    this.responseDataSize = config.getStreamReadResponseDataSize();
    this.readTimeout = config.getStreamReadTimeout();
    this.readTimeoutNanos = readTimeout.toNanos();

    LOG.debug("{}: new StreamBlockInputStream", name);
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
  public synchronized long getPos() {
    return position;
  }

  @Override
  public synchronized int read() throws IOException {
    checkOpen();
    final boolean preRead = true;
    if (!dataAvailableToRead(1, preRead)) {
      return EOF;
    }
    final int value = readBuffer.getByteBuffer().get();
    advancePosition(1, preRead);
    return value;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    ByteBuffer tmpBuffer = ByteBuffer.wrap(b, off, len);
    return read(tmpBuffer);
  }

  @Override
  public synchronized int read(ByteBuffer targetBuf) throws IOException {
    return readFully(targetBuf, true);
  }

  synchronized int readFully(ByteBuffer targetBuf, boolean preRead) throws IOException {
    checkOpen();
    int read = 0;
    while (targetBuf.hasRemaining()) {
      if (!dataAvailableToRead(targetBuf.remaining(), preRead)) {
        break;
      }

      final ByteBuffer buffer = readBuffer.getByteBuffer();
      int toCopy = Math.min(buffer.remaining(), targetBuf.remaining());
      ByteBuffer tmpBuf = buffer.duplicate();
      tmpBuf.limit(tmpBuf.position() + toCopy);
      targetBuf.put(tmpBuf);
      buffer.position(tmpBuf.position());
      advancePosition(toCopy, preRead);
      read += toCopy;
    }
    return read > 0 ? read : EOF;
  }

  private synchronized boolean dataAvailableToRead(int length, boolean preRead) throws IOException {
    if (position >= blockLength) {
      return false;
    }
    initialize();

    if (!hasRemaining(readBuffer)) {
      readBuffer = streamingReader.read(length, preRead);
    }
    return hasRemaining(readBuffer);
  }

  private synchronized void advancePosition(long delta, boolean preRead) {
    LOG.trace("{}: advance {} -> {}", getName(streamingReader), position, position + delta);
    position += delta;
    if (preRead && position >= blockLength) {
      closeReader("advancePosition");
    }
  }

  private static boolean hasRemaining(ReadBuffer read) {
    return read != null && read.getByteBuffer().hasRemaining();
  }

  @Override
  protected int readWithStrategy(ByteReaderStrategy strategy) throws IOException {
    throw new NotImplementedException("readWithStrategy is not implemented.");
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    checkOpen();
    if (pos < 0) {
      throw new IOException("Cannot seek to negative offset");
    }
    if (pos > blockLength) {
      throw new EOFException("Failed to seek to position " + pos + " > block length = " + blockLength);
    }
    if (pos == position) {
      return;
    }
    LOG.debug("{}: seek {} -> {}", getName(streamingReader), position, pos);
    readBuffer = reuseReadBuffer(readBuffer, pos);
    position = pos;
    if (readBuffer == null) {
      // Only rewind the request high-watermark when the buffered (already requested/served) data cannot be reused;
      // otherwise we would re-request data that is still buffered.
      requestedLength = pos;
    }
  }

  static ReadBuffer reuseReadBuffer(ReadBuffer previous, long blockOffset) {
    if (previous != null) {
      final ByteBuffer buffer = getByteBuffer(previous.getProto(), blockOffset);
      if (buffer != null && buffer.hasRemaining()) {
        previous.getByteBuffer().position(buffer.position());
        Preconditions.assertSame(buffer.remaining(), previous.getByteBuffer().remaining(), "remaining");
        return previous;
      }
    }
    return null;
  }

  static ByteBuffer getByteBuffer(ReadBlockResponseProto proto, long blockOffset) {
    final ByteBuffer buffer = proto.getData().asReadOnlyByteBuffer();
    // Adjust buffer position since the server always returns data starting at checksum boundary.
    final long protoOffset = proto.getOffset();
    if (blockOffset < protoOffset) {
      // This can happen after seek, just drop it for now
      // TODO: consider to cache the proto, which will be useful when seeking back.
      return null;
    }
    final long offset = blockOffset - protoOffset;
    if (offset > 0) {
      buffer.position(Math.toIntExact(Math.min(offset, buffer.limit())));
    }
    return buffer;
  }

  @Override
  // The seekable interface indicates that seekToNewSource should seek to a new source of the data,
  // ie a different datanode. This is not supported for now.
  public synchronized boolean seekToNewSource(long l) throws IOException {
    return false;
  }

  @Override
  public synchronized void unbuffer() {
    releaseClient();
  }

  private synchronized void closeReader(String reason) {
    readBuffer = null;
    if (streamingReader == null) {
      return;
    }

    final StreamingReader reader = streamingReader;
    streamingReader = null;
    LOG.debug("{} closeReader for {}", getName(reader), reason);

    reader.onCompleted();

    final StreamingReadResponse response = reader.getResponse();
    if (response != null) {
      final ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> requestObserver =
          response.getRequestObserver();
      try {
        requestObserver.onCompleted();
      } catch (RuntimeException e) {
        LOG.warn("Failed to close gRPC request stream for {}", reader, e);
        try {
          requestObserver.cancel(STREAM_CLOSE_REASON, e);
        } catch (RuntimeException cancelEx) {
          LOG.warn("Failed to cancel gRPC request stream for {}", reader, cancelEx);
        }
      }
    }
  }

  protected synchronized void checkOpen() throws IOException {
    if (xceiverClientFactory == null) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED + " Block: " + blockID);
    }
  }

  protected synchronized void acquireClient() throws IOException {
    checkOpen();
    if (xceiverClient == null) {
      final Pipeline pipeline = pipelineRef.get();
      final XceiverClientSpi client;
      try {
        client = xceiverClientFactory.acquireClientForReadData(pipeline);
      } catch (IOException ioe) {
        LOG.warn("Failed to acquire client for pipeline {}, block {}", pipeline, blockID);
        throw ioe;
      }

      if (client == null) {
        throw new IOException("Failed to acquire client for " + pipeline);
      }
      if (!(client instanceof XceiverClientGrpc)) {
        throw new IOException("Unexpected client class: " + client.getClass().getName() + ", " + pipeline);
      }

      xceiverClient =  (XceiverClientGrpc) client;
    }
  }

  private synchronized void initialize() throws IOException {
    while (streamingReader == null) {
      try {
        acquireClient();
        final StreamingReader reader = new StreamingReader();
        LOG.debug("{}: new StreamingReader", getName(reader));
        xceiverClient.initStreamRead(blockID, reader);
        streamingReader = reader;
      } catch (IOException ioe) {
        handleExceptions(ioe);
      }
    }
  }

  synchronized void readBlock(int length, boolean preRead) throws IOException {
    final long required = position + length - requestedLength;
    final long preReadLength = preRead ? preReadSize : 0;
    final long readLength = required + preReadLength;

    if (readLength > 0) {
      LOG.debug("position {}, length {}, requested {}, diff {}, readLength {}, preReadSize={}",
          position, length, requestedLength, required, readLength, preReadLength);
      readBlockImpl(readLength);
      requestedLength += readLength;
    }
  }

  synchronized void readBlockImpl(long length) throws IOException {
    if (streamingReader == null) {
      throw new IOException("Uninitialized StreamingReader: " + blockID);
    }
    final StreamingReadResponse r = streamingReader.getResponse();
    if (r == null) {
      throw new IOException("Uninitialized StreamingReadResponse: " + blockID);
    }
    xceiverClient.streamRead(ContainerProtocolCalls.buildReadBlockCommandProto(
        blockID, requestedLength, length, responseDataSize, tokenRef.get(), pipelineRef.get()), r);
  }

  private void handleExceptions(IOException cause) throws IOException {
    if (cause instanceof StorageContainerException || isConnectivityIssue(cause)) {
      if (shouldRetryRead(cause, retryPolicy, retries++)) {
        releaseClient();
        refreshBlockInfo(cause);
        LOG.warn("Refreshing block data to read block {} due to {}", blockID, cause.getMessage());
      } else {
        throw cause;
      }
    } else {
      throw cause;
    }
  }

  protected synchronized void releaseClient() {
    if (xceiverClientFactory != null && xceiverClient != null) {
      closeReader("releaseClient");
      xceiverClientFactory.releaseClientForReadData(xceiverClient, false);
      xceiverClient = null;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    releaseClient();
    xceiverClientFactory = null;
  }

  private void refreshBlockInfo(IOException cause) throws IOException {
    refreshBlockInfo(cause, blockID, pipelineRef, tokenRef, refreshFunction);
  }

  private synchronized void releaseStreamResources() {
    if (xceiverClient != null) {
      xceiverClient.completeStreamRead();
    }
  }

  @Override
  public String toString() {
    return name;
  }

  public long getPreReadSize() {
    return preReadSize;
  }

  public int getResponseDataSize() {
    return responseDataSize;
  }

  /** Visible for testing: returns the configured streaming read timeout. */
  public Duration getReadTimeout() {
    return readTimeout;
  }

  private Object getName(StreamingReader reader) {
    return reader != null ? reader : name;
  }

  static class ReadBuffer {
    private final ReadBlockResponseProto proto;
    private final ByteBuffer buffer;

    ReadBuffer(ReadBlockResponseProto proto, ByteBuffer buffer) {
      this.proto = proto;
      this.buffer = buffer;
    }

    ReadBlockResponseProto getProto() {
      return proto;
    }

    ByteBuffer getByteBuffer() {
      return buffer;
    }

    @Override
    public String toString() {
      return "ReadBuffer: offset=" + proto.getOffset()
          + ", dataSize=" + proto.getData().size()
          + ", buffer=" + buffer;
    }
  }

  /**
   * Implementation of a StreamObserver used to received and buffer streaming GRPC reads.
   */
  public class StreamingReader implements StreamingReaderSpi {
    private final String name = StreamBlockInputStream.this.name + "-reader" + READER_ID.getAndIncrement();

    /** Response queue: poll is blocking while offer is non-blocking. */
    private final BlockingQueue<ReadBlockResponseProto> responseQueue = new LinkedBlockingQueue<>();

    private final CompletableFuture<Void> future = new CompletableFuture<>();
    private final AtomicBoolean semaphoreReleased = new AtomicBoolean(false);
    private final AtomicReference<StreamingReadResponse> response = new AtomicReference<>();

    void checkError() throws IOException {
      if (future.isCompletedExceptionally()) {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          throw new IOException("Streaming read failed", e);
        }
      }
    }

    ReadBlockResponseProto poll() throws IOException {
      final long startTime = System.nanoTime();
      final long pollTimeoutNanos = Math.min(readTimeoutNanos / 10, 100_000_000);

      while (true) {
        checkError();

        final ReadBlockResponseProto proto;
        try {
          proto = responseQueue.poll(pollTimeoutNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while waiting for response", e);
        }
        if (proto != null) {
          return proto;
        }

        // Check isDone only after confirming the queue is empty. If isDone() were
        // checked first, an item delivered by onNext() just before onCompleted()
        // fired would be silently dropped, causing data corruption.
        if (future.isDone()) {
          return null; // Stream ended, queue is empty
        }

        final long elapsedNanos = System.nanoTime() - startTime;
        if (elapsedNanos >= readTimeoutNanos && !future.isDone()) {
          final TimeoutIOException e = new TimeoutIOException(
              this + ": Failed to poll a response, timed out " + readTimeout);
          if (setFailed(e)) {
            throw e;
          }
          return null;
        }
      }
    }

    private ReadBuffer read(int length, boolean preRead) throws IOException {
      checkError();
      if (future.isDone()) {
        // Don't return null while items remain in the queue. onNext() may have delivered items just before
        // onCompleted() fired.
        return responseQueue.isEmpty() ? null : readFromQueue();
      }

      readBlock(length, preRead);

      while (true) {
        final ReadBlockResponseProto proto = poll();
        if (proto == null) {
          return null;
        }
        final ByteBuffer buffer = getByteBuffer(proto, getPos());
        final ReadBuffer read = buffer != null ? new ReadBuffer(proto, buffer) : null;
        if (hasRemaining(read)) {
          LOG.debug("{}: read(length={}, preRead={}) returns {}", name, length, preRead, read);
          return read;
        }
      }
    }

    private void releaseResources() {
      if (semaphoreReleased.compareAndSet(false, true)) {
        releaseStreamResources();
      }
    }

    @Override
    public void onNext(ContainerProtos.ContainerCommandResponseProto containerCommandResponseProto) {
      final ReadBlockResponseProto readBlock = containerCommandResponseProto.getReadBlock();
      try {
        ByteBuffer data = readBlock.getData().asReadOnlyByteBuffer();
        if (verifyChecksum) {
          ChecksumData checksumData = ChecksumData.getFromProtoBuf(readBlock.getChecksumData());
          Checksum.verifyChecksum(data, checksumData, 0);
        }
        offerToQueue(readBlock);
      } catch (Exception e) {
        final ByteString data = readBlock.getData();
        final long offset = readBlock.getOffset();
        final StreamingReadResponse r = getResponse();
        LOG.warn("Failed to process block {} response at offset={}, size={}: {}, {}",
            getBlockID().getContainerBlockID(),
            offset, data.size(), StringUtils.bytes2Hex(data.substring(0, 10).asReadOnlyByteBuffer()),
            readBlock.getChecksumData(), e);
        setFailed(e);
        r.getRequestObserver().onError(e);
        releaseResources();
      }
    }

    @Override
    public void onError(Throwable throwable) {
      if (throwable instanceof StatusRuntimeException) {
        if (((StatusRuntimeException) throwable).getStatus().getCode() == CANCELLED) {
          // This is expected when the client cancels the stream.
          setCompleted();
        }
      } else {
        setFailed(throwable);
      }
      releaseResources();
    }

    @Override
    public void onCompleted() {
      setCompleted();
      releaseResources();
    }

    StreamingReadResponse getResponse() {
      return response.get();
    }

    private boolean setFailed(Throwable throwable) {
      final boolean completed = future.completeExceptionally(throwable);
      if (!completed) {
        LOG.warn("{}: Already completed, suppress ", this, throwable);
      }
      return completed;
    }

    private void setCompleted() {
      final boolean changed = future.complete(null);
      if (changed) {
        LOG.debug("{} setCompleted success", this);
      } else {
        try {
          future.get();
          LOG.debug("{} Failed to setCompleted: Already completed", this);
        } catch (InterruptedException e) {
          LOG.warn("{}: Interrupted setCompleted", this, e);
        } catch (ExecutionException e) {
          LOG.warn("{}: Failed to setCompleted: already completed exceptionally", this, e);
        }
      }

      releaseResources();
    }

    private void offerToQueue(ReadBlockResponseProto item) {
      if (LOG.isTraceEnabled()) {
        final ContainerProtos.ChecksumData checksumData = item.getChecksumData();
        LOG.trace("{}: enqueue response offset {}, length {}, numChecksums {}, bytesPerChecksum={}",
            name, item.getOffset(), item.getData().size(),
            checksumData.getChecksumsList().size(), checksumData.getBytesPerChecksum());
      }
      final boolean offered = responseQueue.offer(item);
      Preconditions.assertTrue(offered, () -> "Failed to offer " + item);
    }

    @Override
    public void setStreamingReadResponse(StreamingReadResponse streamingReadResponse) {
      final boolean set = response.compareAndSet(null, streamingReadResponse);
      Preconditions.assertTrue(set, () -> "Failed to set streamingReadResponse");
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
