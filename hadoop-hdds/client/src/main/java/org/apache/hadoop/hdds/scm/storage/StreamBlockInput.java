/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.StreamDataResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.hadoop.hdds.client.ReplicationConfig.getLegacyFactor;

/**
 * An {@link java.io.InputStream} called from KeyInputStream to read a block from the
 * container.
 */
public class StreamBlockInput extends BlockExtendedInputStream
    implements Seekable, CanUnbuffer, ByteBufferReadable {
  private static final Logger LOG =
      LoggerFactory.getLogger(StreamBlockInput.class);
  private final BlockID blockID;
  private final long length;
  private final AtomicReference<Pipeline> pipelineRef =
      new AtomicReference<>();
  private final AtomicReference<Token<OzoneBlockTokenIdentifier>> tokenRef =
      new AtomicReference<>();
  private XceiverClientFactory xceiverClientFactory;
  private XceiverClientSpi xceiverClient;

  private List<Long> bufferoffsets;
  private int bufferIndex;
  private long blockPosition = -1;
  private List<ByteBuffer> buffers;
  private boolean allocated = false;
  private long bufferOffsetWrtBlockDataData;
  private long buffersSize;
  private static final int EOF = -1;
  private final List<XceiverClientSpi.Validator> validators;
  private final boolean verifyChecksum;
  private final Function<BlockID, BlockLocationInfo> refreshFunction;
  private final RetryPolicy retryPolicy;
  private int retries;


  public StreamBlockInput(
      BlockID blockID, long length, Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverClientFactory,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      OzoneClientConfig config) throws IOException {
    this.blockID = blockID;
    this.length = length;
    setPipeline(pipeline);
    tokenRef.set(token);
    this.xceiverClientFactory = xceiverClientFactory;
    this.validators = ContainerProtocolCalls.toValidatorList(
        (request, response) -> validateBlock(response));
    this.verifyChecksum = config.isChecksumVerify();
    this.refreshFunction = refreshFunction;
    this.retryPolicy =
        HddsClientUtils.createRetryPolicy(config.getMaxReadRetryCount(),
            TimeUnit.SECONDS.toMillis(config.getReadRetryInterval()));

  }


  public BlockID getBlockID() {
    return blockID;
  }

  public long getLength() {
    return length;
  }

  @Override
  public synchronized long getPos() {
    if (length == 0) {
      return 0;
    }
    if (blockPosition >= 0) {
      return blockPosition;
    }

    if (allocated && !buffersHaveData() && !dataRemainingInBlock()) {
      Preconditions.checkState(
          bufferOffsetWrtBlockDataData + buffersSize == length,
          "EOF detected but not at the last byte of the chunk");
      return length;
    }
    if (buffersHaveData()) {
      // BufferOffset w.r.t to BlockData + BufferOffset w.r.t buffers +
      // Position of current Buffer
      return bufferOffsetWrtBlockDataData + bufferoffsets.get(bufferIndex) +
          buffers.get(bufferIndex).position();
    }
    if (buffersAllocated()) {
      return bufferOffsetWrtBlockDataData + buffersSize;
    }
    return 0;
  }

  @Override
  public synchronized int read() throws IOException {
    checkOpen();

    int dataout = EOF;
    int len = 1;
    int available;
    while (len > 0) {
      try {
        acquireClient();
        available = prepareRead(1);
        retries = 0;
      } catch (SCMSecurityException ex) {
        throw ex;
      } catch (StorageContainerException e) {
        if (shouldRetryRead(e)) {
          releaseClient();
          refreshBlockInfo(e);
          continue;
        } else {
          throw e;
        }
      } catch (IOException ioe) {
        if (shouldRetryRead(ioe)) {
          if (isConnectivityIssue(ioe)) {
            releaseClient();
            refreshBlockInfo(ioe);
          } else {
            releaseClient();
          }
          continue;
        } else {
          throw ioe;
        }
      }
      if (available == EOF) {
        // There is no more data in the chunk stream. The buffers should have
        // been released by now
        Preconditions.checkState(buffers == null);
      } else {
        dataout = Byte.toUnsignedInt(buffers.get(bufferIndex).get());
      }

      len -= available;
      if (bufferEOF()) {
        releaseBuffers(bufferIndex);
      }
    }


    return dataout;


  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    // According to the JavaDocs for InputStream, it is recommended that
    // subclasses provide an override of bulk read if possible for performance
    // reasons.  In addition to performance, we need to do it for correctness
    // reasons.  The Ozone REST service uses PipedInputStream and
    // PipedOutputStream to relay HTTP response data between a Jersey thread and
    // a Netty thread.  It turns out that PipedInputStream/PipedOutputStream
    // have a subtle dependency (bug?) on the wrapped stream providing separate
    // implementations of single-byte read and bulk read.  Without this, get key
    // responses might close the connection before writing all of the bytes
    // advertised in the Content-Length.
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return 0;
    }
    int total = 0;
    int available;
    while (len > 0) {
      try {
        acquireClient();
        available = prepareRead(len);
        retries = 0;
      } catch (SCMSecurityException ex) {
        throw ex;
      } catch (StorageContainerException e) {
        if (shouldRetryRead(e)) {
          releaseClient();
          refreshBlockInfo(e);
          continue;
        } else {
          throw e;
        }
      } catch (IOException ioe) {
        if (shouldRetryRead(ioe)) {
          if (isConnectivityIssue(ioe)) {
            releaseClient();
            refreshBlockInfo(ioe);
          } else {
            releaseClient();
          }
          continue;
        } else {
          throw ioe;
        }
      }
      if (available == EOF) {
        // There is no more data in the block stream. The buffers should have
        // been released by now
        Preconditions.checkState(buffers == null);
        return total != 0 ? total : EOF;
      }
      buffers.get(bufferIndex).get(b, off + total, available);
      len -= available;
      total += available;

      if (bufferEOF()) {
        releaseBuffers(bufferIndex);
      }
    }
    return total;

  }

  @Override
  public synchronized void close() throws IOException {
    releaseClient();
    releaseBuffers();
    xceiverClientFactory = null;
  }

  @Override
  public synchronized int read(ByteBuffer byteBuffer) throws IOException {
    if (byteBuffer == null) {
      throw new NullPointerException();
    }
    int len = byteBuffer.remaining();
    if (len == 0) {
      return 0;
    }
    int total = 0;
    int available;
    while (len > 0) {
      try {
        acquireClient();
        available = prepareRead(len);
        retries = 0;
      } catch (SCMSecurityException ex) {
        throw ex;
      } catch (StorageContainerException e) {
        if (shouldRetryRead(e)) {
          releaseClient();
          refreshBlockInfo(e);
          continue;
        } else {
          throw e;
        }
      } catch (IOException ioe) {
        if (shouldRetryRead(ioe)) {
          if (isConnectivityIssue(ioe)) {
            releaseClient();
            refreshBlockInfo(ioe);
          } else {
            releaseClient();
          }
          continue;
        } else {
          throw ioe;
        }
      }
      if (available == EOF) {
        // There is no more data in the block stream. The buffers should have
        // been released by now
        Preconditions.checkState(buffers == null);
        return total != 0 ? total : EOF;
      }
      ByteBuffer readBuf = buffers.get(bufferIndex);
      ByteBuffer tmpBuf = readBuf.duplicate();
      tmpBuf.limit(tmpBuf.position() + available);
      byteBuffer.put(tmpBuf);
      readBuf.position(tmpBuf.position());

      len -= available;
      total += available;

      if (bufferEOF()) {
        releaseBuffers(bufferIndex);
      }
    }
    return total;
  }

  @Override
  protected int readWithStrategy(ByteReaderStrategy strategy) throws IOException {
    throw new NotImplementedException("readWithStrategy is not implemented.");
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos < 0 || pos > length) {
      if (pos == 0) {
        // It is possible for length and pos to be zero in which case
        // seek should return instead of throwing exception
        return;
      }
      throw new EOFException("EOF encountered at pos: " + pos + " for block: " + blockID);
    }

    if (buffersHavePosition(pos)) {
      // The bufferPosition is w.r.t the current block.
      // Adjust the bufferIndex and position to the seeked position.
      adjustBufferPosition(pos - bufferOffsetWrtBlockDataData);
    } else {
      blockPosition = pos;
    }
  }

  @Override
  public synchronized boolean seekToNewSource(long l) throws IOException {
    return false;
  }

  @Override
  public synchronized void unbuffer() {
    blockPosition = getPos();
    releaseClient();
    releaseBuffers();
  }

  private void setPipeline(Pipeline pipeline) throws IOException {
    if (pipeline == null) {
      return;
    }
    long replicaIndexes = pipeline.getNodes().stream().mapToInt(pipeline::getReplicaIndex).distinct().count();

    if (replicaIndexes > 1) {
      throw new IOException(String.format("Pipeline: %s has nodes containing different replica indexes.",
          pipeline));
    }

    // irrespective of the container state, we will always read via Standalone
    // protocol.
    boolean okForRead =
        pipeline.getType() == HddsProtos.ReplicationType.STAND_ALONE
            || pipeline.getType() == HddsProtos.ReplicationType.EC;
    Pipeline readPipeline = okForRead ? pipeline : Pipeline.newBuilder(pipeline)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(
            getLegacyFactor(pipeline.getReplicationConfig())))
        .build();
    pipelineRef.set(readPipeline);
  }

  protected synchronized void checkOpen() throws IOException {
    if (xceiverClientFactory == null) {
      throw new IOException("BlockInputStream has been closed.");
    }
  }

  protected synchronized void acquireClient() throws IOException {
    if (xceiverClientFactory != null && xceiverClient == null) {
      final Pipeline pipeline = pipelineRef.get();
      try {
        xceiverClient = xceiverClientFactory.acquireClientForReadData(pipeline);
      } catch (IOException ioe) {
        LOG.warn("Failed to acquire client for pipeline {}, block {}",
            pipeline, blockID);
        throw ioe;
      }
    }
  }

  private synchronized int prepareRead(int len) throws IOException {
    for (;;) {
      if (blockPosition >= 0) {
        if (buffersHavePosition(blockPosition)) {
          // The current buffers have the seeked position. Adjust the buffer
          // index and position to point to the buffer position.
          adjustBufferPosition(blockPosition - bufferOffsetWrtBlockDataData);
        } else {
          // Read a required block data to fill the buffers with seeked
          // position data
          readDataFromContainer(len);
        }
      }
      if (buffersHaveData()) {
        // Data is available from buffers
        ByteBuffer bb = buffers.get(bufferIndex);
        return Math.min(len, bb.remaining());
      } else if (dataRemainingInBlock()) {
        // There is more data in the block stream which has not
        // been read into the buffers yet.
        readDataFromContainer(len);
      } else {
        // All available input from this block stream has been consumed.
        return EOF;
      }
    }


  }

  private boolean buffersHavePosition(long pos) {
    // Check if buffers have been allocated
    if (buffersAllocated()) {
      // Check if the current buffers cover the input position
      // Released buffers should not be considered when checking if position
      // is available
      return pos >= bufferOffsetWrtBlockDataData +
          bufferoffsets.get(0) &&
          pos < bufferOffsetWrtBlockDataData + buffersSize;
    }
    return false;
  }

  /**
   * Check if the buffers have been allocated data and false otherwise.
   */
  @VisibleForTesting
  protected boolean buffersAllocated() {
    return buffers != null && !buffers.isEmpty();
  }

  /**
   * Adjust the buffers position to account for seeked position and/ or checksum
   * boundary reads.
   * @param bufferPosition the position to which the buffers must be advanced
   */
  private void adjustBufferPosition(long bufferPosition) {
    // The bufferPosition is w.r.t the current buffers.
    // Adjust the bufferIndex and position to the seeked bufferPosition.
    if (bufferIndex >= buffers.size()) {
      bufferIndex = Collections.binarySearch(bufferoffsets, bufferPosition);
    } else if (bufferPosition < bufferoffsets.get(bufferIndex)) {
      bufferIndex = Collections.binarySearch(
          bufferoffsets.subList(0, bufferIndex), bufferPosition);
    } else if (bufferPosition >= bufferoffsets.get(bufferIndex) +
        buffers.get(bufferIndex).capacity()) {
      bufferIndex = Collections.binarySearch(bufferoffsets.subList(
          bufferIndex + 1, buffers.size()), bufferPosition);
    }
    if (bufferIndex < 0) {
      bufferIndex = -bufferIndex - 2;
    }

    buffers.get(bufferIndex).position(
        (int) (bufferPosition - bufferoffsets.get(bufferIndex)));

    // Reset buffers > bufferIndex to position 0. We do this to reset any
    // previous reads/ seeks which might have updated any buffer position.
    // For buffers < bufferIndex, we do not need to reset the position as it
    // not required for this read. If a seek was done to a position in the
    // previous indices, the buffer position reset would be performed in the
    // seek call.
    for (int i = bufferIndex + 1; i < buffers.size(); i++) {
      buffers.get(i).position(0);
    }

    // Reset the blockPosition as chunk stream has been initialized i.e. the
    // buffers have been allocated.
    blockPosition = -1;
  }

  /**
   * Reads full or partial Chunk from DN Container based on the current
   * position of the ChunkInputStream, the number of bytes of data to read
   * and the checksum boundaries.
   * If successful, then the read data in saved in the buffers so that
   * subsequent read calls can utilize it.
   * @param len number of bytes of data to be read
   * @throws IOException if there is an I/O error while performing the call
   * to Datanode
   */
  private synchronized void readDataFromContainer(int len) throws IOException {
    // index of first byte to be read from the block
    long startByteIndex;
    if (blockPosition >= 0) {
      // If seek operation was called to advance the buffer position, the
      // chunk should be read from that position onwards.
      startByteIndex = blockPosition;
    } else {
      // Start reading the block from the last blockPosition onwards.
      startByteIndex = bufferOffsetWrtBlockDataData + buffersSize;
    }

    // bufferOffsetWrtChunkData and buffersSize are updated after the data
    // is read from Container and put into the buffers, but if read fails
    // and is retried, we need the previous position.  Position is reset after
    // successful read in adjustBufferPosition()
    blockPosition = getPos();
    bufferOffsetWrtBlockDataData = readData(startByteIndex, len);
    long tempOffset = 0L;
    buffersSize = 0L;
    bufferoffsets = new ArrayList<>(buffers.size());
    for (ByteBuffer buffer : buffers) {
      bufferoffsets.add(tempOffset);
      tempOffset += buffer.limit();
      buffersSize += buffer.limit();

    }
    bufferIndex = 0;
    allocated = true;
    adjustBufferPosition(startByteIndex - bufferOffsetWrtBlockDataData);

  }

  @VisibleForTesting
  protected long readData(long startByteIndex, long len)
      throws IOException {
    Pipeline pipeline = pipelineRef.get();
    buffers = new ArrayList<>();
    StreamDataResponseProto response =
        ContainerProtocolCalls.readBlock(xceiverClient, startByteIndex,
        len, blockID, validators, tokenRef.get(), pipeline.getReplicaIndexes(), verifyChecksum);
    List<ReadBlockResponseProto> readBlocks = response.getReadBlockList();

    for (ReadBlockResponseProto readBlock : readBlocks) {
      if (readBlock.hasData()) {
        buffers.add(readBlock.getData().asReadOnlyByteBuffer());
      } else if (readBlock.hasDataBuffers()) {
        buffers.addAll(BufferUtils.getReadOnlyByteBuffers(
            readBlock.getDataBuffers().getBuffersList()));
      } else {
        throw new IOException("Unexpected error while reading chunk data " +
            "from container. No data returned.");
      }
    }
    return response.getReadBlock(0)
        .getChunkData().getOffset();
  }

  /**
   * Check if the buffers have any data remaining between the current
   * position and the limit.
   */
  private boolean buffersHaveData() {
    boolean hasData = false;
    if (buffersAllocated()) {
      int buffersLen = buffers.size();
      while (bufferIndex < buffersLen) {
        ByteBuffer buffer = buffers.get(bufferIndex);
        if (buffer != null && buffer.hasRemaining()) {
          // current buffer has data
          hasData = true;
          break;
        } else {
          if (bufferIndex < buffersLen - 1) {
            // move to next available buffer
            ++bufferIndex;
            Preconditions.checkState(bufferIndex < buffers.size());
          } else {
            // no more buffers remaining
            break;
          }
        }
      }
    }

    return hasData;
  }

  /**
   * Check if there is more data in the chunk which has not yet been read
   * into the buffers.
   */
  private boolean dataRemainingInBlock() {
    long bufferPos;
    if (blockPosition >= 0) {
      bufferPos = blockPosition;
    } else {
      bufferPos = bufferOffsetWrtBlockDataData + buffersSize;
    }

    return bufferPos < length;
  }

  /**
   * Check if current buffer had been read till the end.
   */
  private boolean bufferEOF() {
    return allocated && !buffers.get(bufferIndex).hasRemaining();
  }

  /**
   * Release the buffers upto the given index.
   * @param releaseUptoBufferIndex bufferIndex (inclusive) upto which the
   *                               buffers must be released
   */
  private void releaseBuffers(int releaseUptoBufferIndex) {
    int buffersLen = buffers.size();
    if (releaseUptoBufferIndex == buffersLen - 1) {
      // Before releasing all the buffers, if block EOF is not reached, then
      // blockPosition should be set to point to the last position of the
      // buffers. This should be done so that getPos() can return the current
      // block position
      blockPosition = bufferOffsetWrtBlockDataData +
          bufferoffsets.get(releaseUptoBufferIndex) +
          buffers.get(releaseUptoBufferIndex).capacity();
      // Release all the buffers
      releaseBuffers();
    } else {
      buffers = buffers.subList(releaseUptoBufferIndex + 1, buffersLen);
      bufferoffsets = bufferoffsets.subList(
          releaseUptoBufferIndex + 1, buffersLen);
    }
  }

  /**
   * If EOF is reached, release the buffers.
   */
  private void releaseBuffers() {
    buffers = null;
    bufferIndex = 0;
    // We should not reset bufferOffsetWrtChunkData and buffersSize here
    // because when getPos() is called we use these
    // values and determine whether chunk is read completely or not.
  }

  protected synchronized void releaseClient() {
    if (xceiverClientFactory != null && xceiverClient != null) {
      xceiverClientFactory.releaseClientForReadData(xceiverClient, false);
      xceiverClient = null;
    }
  }

  private void validateBlock(
      ContainerProtos.ContainerCommandResponseProto response
  ) throws IOException {

    StreamDataResponseProto streamData = response.getStreamData();
    for (ReadBlockResponseProto readBlock : streamData.getReadBlockList()) {
      List<ByteString> byteStrings;
      boolean isV0 = false;

      ContainerProtos.ChunkInfo chunkInfo =
          readBlock.getChunkData();
      if (chunkInfo.getLen() <= 0) {
        throw new IOException("Failed to get chunk: chunkName == "
            + chunkInfo.getChunkName() + "len == " + chunkInfo.getLen());
      }
      if (readBlock.hasData()) {
        ByteString byteString = readBlock.getData();
        if (byteString.size() != chunkInfo.getLen()) {
          // Bytes read from chunk should be equal to chunk size.
          throw new OzoneChecksumException(String.format(
              "Inconsistent read for chunk=%s len=%d bytesRead=%d",
              chunkInfo.getChunkName(), chunkInfo.getLen(),
              byteString.size()));
        }
        byteStrings = new ArrayList<>();
        byteStrings.add(byteString);
        isV0 = true;
      } else {
        byteStrings = readBlock.getDataBuffers().getBuffersList();
        long buffersLen = BufferUtils.getBuffersLen(byteStrings);
        if (buffersLen != chunkInfo.getLen()) {
          // Bytes read from chunk should be equal to chunk size.
          throw new OzoneChecksumException(String.format(
              "Inconsistent read for chunk=%s len=%d bytesRead=%d",
              chunkInfo.getChunkName(), chunkInfo.getLen(),
              buffersLen));
        }
      }

      if (verifyChecksum) {
        ChecksumData checksumData = ChecksumData.getFromProtoBuf(
            chunkInfo.getChecksumData());

        // ChecksumData stores checksum for each 'numBytesPerChecksum'
        // number of bytes in a list. Compute the index of the first
        // checksum to match with the read data

        Checksum.verifyChecksum(byteStrings, checksumData, readBlock.getStartIndex(),
            isV0);
      }
    }
  }

  @VisibleForTesting
  protected synchronized void setBuffers(List<ByteBuffer> buffers) {
    this.buffers = buffers;
  }

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

  @VisibleForTesting
  public boolean isVerifyChecksum() {
    return verifyChecksum;
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

  @VisibleForTesting
  public synchronized ByteBuffer[] getCachedBuffers() {
    return buffers == null ? null :
        BufferUtils.getReadOnlyByteBuffers(buffers.toArray(new ByteBuffer[0]));
  }

  /**
   * Check if this exception is because datanodes are not reachable.
   */
  private boolean isConnectivityIssue(IOException ex) {
    return Status.fromThrowable(ex).getCode() == Status.UNAVAILABLE.getCode();
  }
}
