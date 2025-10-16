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

import static org.apache.hadoop.hdds.client.ReplicationConfig.getLegacyFactor;

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
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCallStreamObserver;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
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
  private int retries;

  public StreamBlockInputStream(
      BlockID blockID, long length, Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverClientFactory,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      OzoneClientConfig config) throws IOException {
    this.blockID = blockID;
    this.blockLength = length;
    setPipeline(pipeline);
    tokenRef.set(token);
    this.xceiverClientFactory = xceiverClientFactory;
    this.verifyChecksum = config.isChecksumVerify();
    this.refreshFunction = refreshFunction;
    this.retryPolicy = HddsClientUtils.createRetryPolicy(config.getMaxReadRetryCount(),
        TimeUnit.SECONDS.toMillis(config.getReadRetryInterval()));
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

  private void setPipeline(Pipeline pipeline) throws IOException {
    if (pipeline == null) {
      return;
    }
    long replicaIndexes = pipeline.getNodes().stream().mapToInt(pipeline::getReplicaIndex).distinct().count();

    if (replicaIndexes > 1) {
      throw new IOException(String.format("Pipeline: %s has nodes containing different replica indexes.", pipeline));
    }

    // irrespective of the container state, we will always read via Standalone protocol.
    boolean okForRead = pipeline.getType() == HddsProtos.ReplicationType.STAND_ALONE
        || pipeline.getType() == HddsProtos.ReplicationType.EC;
    Pipeline readPipeline = okForRead ? pipeline : pipeline.copyForRead().toBuilder()
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(getLegacyFactor(pipeline.getReplicationConfig())))
        .build();
    pipelineRef.set(readPipeline);
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
    acquireClient();
    streamingReader = new StreamingReader();
    ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> requestObserver =
        ContainerProtocolCalls.readBlock(xceiverClient, position, blockID, tokenRef.get(),
            pipelineRef.get().getReplicaIndexes(), streamingReader);
    streamingReader.setRequestObserver(requestObserver);
    initialized = true;
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

  /*
  Commenting out for now as we probably need these for retries which are yet to be implemented.

    private boolean shouldRetryRead(IOException cause) throws IOException {
    RetryPolicy.RetryAction retryAction;
    try {
      retryAction = retryPolicy.shouldRetry(cause, ++retries, 0, true);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
    return retryAction.action == RetryPolicy.RetryAction.RetryDecision.RETRY;
  }

  private void refreshBlockInfo(IOException cause) throws IOException {
    LOG.info("Attempting to update pipeline and block token for block {} from pipeline {}: {}",
        blockID, pipelineRef.get().getId(), cause.getMessage());
    if (refreshFunction != null) {
      LOG.debug("Re-fetching pipeline and block token for block {}", blockID);
      BlockLocationInfo blockLocationInfo = refreshFunction.apply(blockID);
      if (blockLocationInfo == null) {
        LOG.warn("No new block location info for block {}", blockID);
      } else {
        setPipeline(blockLocationInfo.getPipeline());
        LOG.info("New pipeline for block {}: {}", blockID,
            blockLocationInfo.getPipeline());

        tokenRef.set(blockLocationInfo.getToken());
        if (blockLocationInfo.getToken() != null) {
          OzoneBlockTokenIdentifier tokenId = new OzoneBlockTokenIdentifier();
          tokenId.readFromByteArray(tokenRef.get().getIdentifier());
          LOG.info("A new token is added for block {}. Expiry: {}",
              blockID, Instant.ofEpochMilli(tokenId.getExpiryDate()));
        }
      }
    } else {
      throw cause;
    }
  }

  private boolean isConnectivityIssue(IOException ex) {
    return Status.fromThrowable(ex).getCode() == Status.UNAVAILABLE.getCode();
  }

  private void handleStorageContainerException(StorageContainerException e) throws IOException {
    if (shouldRetryRead(e)) {
      releaseClient();
      refreshBlockInfo(e);
    } else {
      throw e;
    }
  }

  private void handleIOException(IOException ioe) throws IOException {
    if (shouldRetryRead(ioe)) {
      if (isConnectivityIssue(ioe)) {
        releaseClient();
        refreshBlockInfo(ioe);
      } else {
        releaseClient();
      }
    } else {
      throw ioe;
    }
  }

   */

  /**
   * Implementation of a StreamObserver used to received and buffer streaming GRPC reads.
   */
  public class StreamingReader implements StreamObserver<ContainerProtos.ContainerCommandResponseProto> {

    private final BlockingQueue<ContainerProtos.ReadBlockResponseProto> responseQueue = new LinkedBlockingQueue<>(1);
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> requestObserver;

    // TODO: Semaphore in XceiverClient which count open stream?
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
      if (position < blockOffset) {
        // This should not happen, and if it does, we have a bug.
        throw new IOException("Received data out of order. Position is " + position + " but received data at "
            + blockOffset);
      }
      if (position > readBlock.getOffset()) {
        int offset = (int)(position - readBlock.getOffset());
        buf.position(offset);
      }
      return buf;
    }

    public void setRequestObserver(
        ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> requestObserver) {
      this.requestObserver = requestObserver;
    }

    /**
     * By calling cancel, the client will send a cancel signal to the server, which will stop sending more data and
     * cause the onError() to be called in this observer with a CANCELLED exception.
     */
    public void cancel() {
      if (requestObserver != null) {
        requestObserver.cancel("Cancelled by client", CANCELLED_EXCEPTION);
        completed.set(true);
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
        // Calling onError will cancel the stream on the server side and also set the failure state.
        onError(e);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      if (throwable == CANCELLED_EXCEPTION) {
        completed.set(true);
      } else {
        failed.set(true);
        error.set(throwable);
      }
    }

    @Override
    public void onCompleted() {
      completed.set(true);
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
  }

}
