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

  private ByteBuffer buffer;
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
    if (!dataAvailableToRead(1, true)) {
      return EOF;
    }
    position++;
    return buffer.get();
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
      int toCopy = Math.min(buffer.remaining(), targetBuf.remaining());
      ByteBuffer tmpBuf = buffer.duplicate();
      tmpBuf.limit(tmpBuf.position() + toCopy);
      targetBuf.put(tmpBuf);
      buffer.position(tmpBuf.position());
      position += toCopy;
      read += toCopy;
    }
    return read > 0 ? read : EOF;
  }

  private synchronized boolean dataAvailableToRead(int length, boolean preRead) throws IOException {
    if (position >= blockLength) {
      return false;
    }
    initialize();

    if (bufferHasRemaining()) {
      return true;
    }
    buffer = streamingReader.read(length, preRead);
    return bufferHasRemaining();
  }

  private synchronized boolean bufferHasRemaining() {
    return buffer != null && buffer.hasRemaining();
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
    LOG.debug("{}: seek {} -> {}", this, position, pos);
    closeStream();
    position = pos;
    requestedLength = pos;
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

  private synchronized void closeStream() {
    if (streamingReader != null) {
      LOG.debug("Closing {}", streamingReader);
      streamingReader.onCompleted();
      streamingReader = null;
    }
    buffer = null;
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
      closeStream();
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
        if (future.isDone()) {
          return null; // Stream ended
        }

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

        final long elapsedNanos = System.nanoTime() - startTime;
        if (elapsedNanos >= readTimeoutNanos) {
          setFailedAndThrow(new TimeoutIOException(
              "Timed out waiting for response after " + readTimeout));
          return null;
        }
      }
    }

    private ByteBuffer read(int length, boolean preRead) throws IOException {
      checkError();
      if (future.isDone()) {
        return null; // Stream ended
      }

      readBlock(length, preRead);

      while (true) {
        final ByteBuffer buf = readFromQueue();
        if (buf != null && buf.hasRemaining()) {
          return buf;
        }
      }
    }

    ByteBuffer readFromQueue() throws IOException {
      final ReadBlockResponseProto readBlock = poll();
      // The server always returns data starting from the last checksum boundary. Therefore if the reader position is
      // ahead of the position we received from the server, we need to adjust the buffer position accordingly.
      // If the reader position is behind
      final ByteString data = readBlock.getData();
      final ByteBuffer dataBuffer = data.asReadOnlyByteBuffer();
      final long blockOffset = readBlock.getOffset();
      final long pos = getPos();
      if (pos < blockOffset) {
        // This should not happen, and if it does, we have a bug.
        setFailedAndThrow(new IllegalStateException(
            this + ": out of order, position " + pos + " < block offset " + blockOffset));
      }
      final long offset = pos - blockOffset;
      if (offset > 0) {
        dataBuffer.position(Math.toIntExact(Math.min(offset, dataBuffer.limit())));
      }
      LOG.debug("{}: return response positon {}, length {} (block offset {}, length {})",
          name, pos, dataBuffer.remaining(), blockOffset, data.size());
      return dataBuffer;
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

    private <T extends Throwable> void setFailedAndThrow(T throwable) throws T {
      if (setFailed(throwable)) {
        throw throwable;
      }
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
      if (LOG.isDebugEnabled()) {
        final ContainerProtos.ChecksumData checksumData = item.getChecksumData();
        LOG.debug("{}: enqueue response offset {}, length {}, numChecksums {}, bytesPerChecksum={}",
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
