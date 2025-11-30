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
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockResponseProto;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamingReadResponse;
import org.apache.hadoop.hdds.scm.StreamingReaderSpi;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link java.io.InputStream} called from KeyInputStream to read a block from the
 * container.
 */
public class StreamBlockInputStream extends BlockExtendedInputStream
    implements Seekable, CanUnbuffer, ByteBufferReadable {
  private static final Logger LOG = LoggerFactory.getLogger(StreamBlockInputStream.class);
  private static final int EOF = -1;
  private static final Throwable CANCELLED_EXCEPTION = new Throwable("Cancelled by client");

  private final BlockID blockID;
  private final long blockLength;
  private final AtomicReference<Pipeline> pipelineRef = new AtomicReference<>();
  private final AtomicReference<Token<OzoneBlockTokenIdentifier>> tokenRef = new AtomicReference<>();
  private XceiverClientFactory xceiverClientFactory;
  private XceiverClientSpi xceiverClient;

  private ByteBuffer buffer;
  private long position = 0;
  private boolean initialized = false;
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
    if (!dataAvailableToRead()) {
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
    checkOpen();
    int read = 0;
    while (targetBuf.hasRemaining()) {
      if (!dataAvailableToRead()) {
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

  private boolean dataAvailableToRead() throws IOException {
    if (position >= blockLength) {
      return false;
    }
    initialize();
    if (buffer == null || buffer.remaining() == 0) {
      int loaded = fillBuffer();
      return loaded != EOF;
    }
    return true;
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
      throw new IOException("Cannot seek after the end of the block");
    }
    if (pos == position) {
      return;
    }
    closeStream();
    position = pos;
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

  private void closeStream() {
    if (streamingReader != null) {
      streamingReader.cancel();
      streamingReader = null;
    }
    initialized = false;
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
      try {
        xceiverClient = xceiverClientFactory.acquireClientForReadData(pipeline);
      } catch (IOException ioe) {
        LOG.warn("Failed to acquire client for pipeline {}, block {}", pipeline, blockID);
        throw ioe;
      }
    }
  }

  private void initialize() throws IOException {
    if (initialized) {
      return;
    }
    while (true) {
      try {
        acquireClient();
        streamingReader = new StreamingReader();
        ContainerProtocolCalls.readBlock(xceiverClient, position, blockID, tokenRef.get(),
            pipelineRef.get().getReplicaIndexes(), streamingReader);
        initialized = true;
        return;
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        handleExceptions(new IOException("Interrupted", ie));
      } catch (IOException ioe) {
        handleExceptions(ioe);
      }
    }
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

  private int fillBuffer() throws IOException {
    if (!streamingReader.hasNext()) {
      return EOF;
    }
    buffer = streamingReader.readNext();
    return buffer == null ? EOF : buffer.limit();
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

  private synchronized void releaseStreamResources(StreamingReadResponse response) {
    if (xceiverClient != null) {
      xceiverClient.completeStreamRead(response);
    }
  }

  /**
   * Implementation of a StreamObserver used to received and buffer streaming GRPC reads.
   */
  public class StreamingReader implements StreamingReaderSpi {

    private final BlockingQueue<ContainerProtos.ReadBlockResponseProto> responseQueue = new LinkedBlockingQueue<>(1);
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final AtomicBoolean semaphoreReleased = new AtomicBoolean(false);
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private volatile StreamingReadResponse response;

    public boolean hasNext() {
      return !responseQueue.isEmpty() || !completed.get();
    }

    public ByteBuffer readNext() throws IOException {
      if (failed.get()) {
        Throwable cause = error.get();
        throw new IOException("Streaming read failed", cause);
      }

      if (completed.get() && responseQueue.isEmpty()) {
        return null; // Stream ended
      }

      ReadBlockResponseProto readBlock;
      try {
        readBlock = responseQueue.poll(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for response", e);
      }
      if (readBlock == null) {
        if (failed.get()) {
          Throwable cause = error.get();
          throw new IOException("Streaming read failed", cause);
        } else if (completed.get()) {
          return null; // Stream ended
        } else {
          throw new IOException("Timed out waiting for response");
        }
      }
      // The server always returns data starting from the last checksum boundary. Therefore if the reader position is
      // ahead of the position we received from the server, we need to adjust the buffer position accordingly.
      // If the reader position is behind
      ByteBuffer buf = readBlock.getData().asReadOnlyByteBuffer();
      long blockOffset = readBlock.getOffset();
      long pos = getPos();
      if (pos < blockOffset) {
        // This should not happen, and if it does, we have a bug.
        throw new IOException("Received data out of order. Position is " + pos + " but received data at "
            + blockOffset);
      }
      if (pos > readBlock.getOffset()) {
        int offset = (int)(pos - readBlock.getOffset());
        buf.position(offset);
      }
      return buf;
    }

    private void releaseResources() {
      // release resources only if it was not yet completed
      if (semaphoreReleased.compareAndSet(false, true)) {
        releaseStreamResources(response);
      }
    }

    @Override
    public void onNext(ContainerProtos.ContainerCommandResponseProto containerCommandResponseProto) {
      try {
        ReadBlockResponseProto readBlock = containerCommandResponseProto.getReadBlock();
        ByteBuffer data = readBlock.getData().asReadOnlyByteBuffer();
        if (verifyChecksum) {
          ChecksumData checksumData = ChecksumData.getFromProtoBuf(readBlock.getChecksumData());
          Checksum.verifyChecksum(data, checksumData, 0);
        }
        offerToQueue(readBlock);
      } catch (OzoneChecksumException e) {
        LOG.warn("Checksum verification failed for block {} from datanode {}",
            getBlockID(), response.getDatanodeDetails(), e);
        cancelDueToError(e);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      if (throwable instanceof StatusRuntimeException) {
        if (((StatusRuntimeException) throwable).getStatus().getCode().name().equals("CANCELLED")) {
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

    /**
     * By calling cancel, the client will send a cancel signal to the server, which will stop sending more data and
     * cause the onError() to be called in this observer with a CANCELLED exception.
     */
    public void cancel() {
      if (response != null && response.getRequestObserver() != null) {
        response.getRequestObserver().cancel("Cancelled by client", CANCELLED_EXCEPTION);
        setCompleted();
        releaseResources();
      }
    }

    public void cancelDueToError(Throwable exception) {
      if (response != null && response.getRequestObserver() != null) {
        response.getRequestObserver().onError(exception);
        setFailed(exception);
        releaseResources();
      }
    }

    private void setFailed(Throwable throwable) {
      if (completed.get()) {
        throw new IllegalArgumentException("Cannot mark a completed stream as failed");
      }
      failed.set(true);
      error.set(throwable);
    }

    private void setCompleted() {
      if (!failed.get()) {
        completed.set(true);
      }
    }

    private void offerToQueue(ReadBlockResponseProto item) {
      while (!completed.get() && !failed.get()) {
        try {
          if (responseQueue.offer(item, 100, TimeUnit.MILLISECONDS)) {
            return;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    @Override
    public void setStreamingReadResponse(StreamingReadResponse streamingReadResponse) {
      response = streamingReadResponse;
    }
  }

}
