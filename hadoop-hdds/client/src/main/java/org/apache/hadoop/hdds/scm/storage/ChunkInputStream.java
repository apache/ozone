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
import java.util.ArrayList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link InputStream} called from BlockInputStream to read a chunk from the
 * container. Each chunk may contain multiple underlying {@link ByteBuffer}
 * instances.
 */
public class ChunkInputStream extends InputStream
    implements Seekable, CanUnbuffer, ByteBufferReadable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChunkInputStream.class);

  private ChunkInfo chunkInfo;
  private final long length;
  private final BlockID blockID;
  private final XceiverClientFactory xceiverClientFactory;
  private XceiverClientSpi xceiverClient;
  private final Supplier<Pipeline> pipelineSupplier;
  private boolean verifyChecksum;
  private boolean allocated = false;
  // Buffers to store the chunk data read from the DN container
  private ByteBuffer[] buffers;

  // Index of the buffers corresponding to the current position of the buffers
  private int bufferIndex;
  // bufferOffsets[i] stores the index of the first data byte in buffer i
  // (buffers.get(i)) w.r.t first byte in the buffers.
  // Let's say each buffer has a capacity of 40 bytes. The bufferOffset for
  // the first buffer would always be 0 as this would be the first data byte
  // in buffers. BufferOffset for the 2nd buffer would be 40 as bytes 0-39
  // would be stored in buffer 0. Hence, bufferOffsets[0] = 0,
  // bufferOffsets[1] = 40, bufferOffsets[2] = 80, etc.
  private long[] bufferOffsets = null;

  // The offset of the current data residing in the buffers w.r.t the start
  // of chunk data
  private long bufferOffsetWrtChunkData;

  // Index of the first buffer which has not been released
  private int firstUnreleasedBufferIndex = 0;

  // The number of bytes of chunk data residing in the buffers currently
  private long buffersSize;

  // Position of the ChunkInputStream is maintained by this variable (if a
  // seek is performed. This position is w.r.t to the chunk only and not the
  // block or key. This variable is also set before attempting a read to enable
  // retry.  Once the chunk is read, this variable is reset.
  private long chunkPosition = -1;

  private final Token<? extends TokenIdentifier> token;

  private static final int EOF = -1;

  ChunkInputStream(ChunkInfo chunkInfo, BlockID blockId,
      XceiverClientFactory xceiverClientFactory,
      Supplier<Pipeline> pipelineSupplier,
      boolean verifyChecksum, Token<? extends TokenIdentifier> token) {
    this.chunkInfo = chunkInfo;
    this.length = chunkInfo.getLen();
    this.blockID = blockId;
    this.xceiverClientFactory = xceiverClientFactory;
    this.pipelineSupplier = pipelineSupplier;
    this.verifyChecksum = verifyChecksum;
    this.token = token;
  }

  public synchronized long getRemaining() {
    return length - getPos();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized int read() throws IOException {
    acquireClient();
    int available = prepareRead(1);
    int dataout = EOF;

    if (available == EOF) {
      // There is no more data in the chunk stream. The buffers should have
      // been released by now
      Preconditions.checkState(buffers == null);
    } else {
      dataout = Byte.toUnsignedInt(buffers[bufferIndex].get());
    }

    if (bufferEOF()) {
      releaseBuffers(bufferIndex);
    }

    return dataout;
  }

  /**
   * {@inheritDoc}
   */
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
    acquireClient();
    int total = 0;
    while (len > 0) {
      int available = prepareRead(len);
      if (available == EOF) {
        // There is no more data in the chunk stream. The buffers should have
        // been released by now
        Preconditions.checkState(buffers == null);
        return total != 0 ? total : EOF;
      }
      buffers[bufferIndex].get(b, off + total, available);
      len -= available;
      total += available;

      if (bufferEOF()) {
        releaseBuffers(bufferIndex);
      }
    }

    return total;
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
    acquireClient();
    int total = 0;
    while (len > 0) {
      int available = prepareRead(len);
      if (available == EOF) {
        // There is no more data in the chunk stream. The buffers should have
        // been released by now
        Preconditions.checkState(buffers == null);
        return total != 0 ? total : EOF;
      }
      ByteBuffer readBuf = buffers[bufferIndex];
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

  /**
   * Seeks the ChunkInputStream to the specified position. This is done by
   * updating the chunkPosition to the seeked position in case the buffers
   * are not allocated or buffers do not contain the data corresponding to
   * the seeked position (determined by buffersHavePosition()). Otherwise,
   * the buffers position is updated to the seeked position.
   */
  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos < 0 || pos >= length) {
      if (pos == 0) {
        // It is possible for length and pos to be zero in which case
        // seek should return instead of throwing exception
        return;
      }
      throw new EOFException("EOF encountered at pos: " + pos + " for chunk: "
          + chunkInfo.getChunkName());
    }

    if (buffersHavePosition(pos)) {
      // The bufferPosition is w.r.t the current chunk.
      // Adjust the bufferIndex and position to the seeked position.
      adjustBufferPosition(pos - bufferOffsetWrtChunkData);
    } else {
      chunkPosition = pos;
    }
  }

  @Override
  public synchronized long getPos() {
    if (chunkPosition >= 0) {
      return chunkPosition;
    }
    if (chunkStreamEOF()) {
      return length;
    }
    if (buffersHaveData()) {
      // BufferOffset w.r.t to ChunkData + BufferOffset w.r.t buffers +
      // Position of current Buffer
      return bufferOffsetWrtChunkData + bufferOffsets[bufferIndex] +
          buffers[bufferIndex].position();
    }
    if (buffersAllocated()) {
      return bufferOffsetWrtChunkData + buffersSize;
    }
    return 0;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized void close() {
    releaseBuffers();
    releaseClient();
  }

  protected synchronized void releaseClient() {
    if (xceiverClientFactory != null && xceiverClient != null) {
      xceiverClientFactory.releaseClientForReadData(xceiverClient, false);
      xceiverClient = null;
    }
  }

  /**
   * Acquire new client if previous one was released.
   */
  protected synchronized void acquireClient() throws IOException {
    if (xceiverClientFactory != null && xceiverClient == null) {
      xceiverClient = xceiverClientFactory.acquireClientForReadData(
          pipelineSupplier.get());
    }
  }

  /**
   * Prepares to read by advancing through buffers or allocating new buffers,
   * as needed until it finds data to return, or encounters EOF.
   * @param len desired length of data to read
   * @return length of data available to read, possibly less than desired length
   */
  private synchronized int prepareRead(int len) throws IOException {
    for (;;) {
      if (chunkPosition >= 0) {
        if (buffersHavePosition(chunkPosition)) {
          // The current buffers have the seeked position. Adjust the buffer
          // index and position to point to the chunkPosition.
          adjustBufferPosition(chunkPosition - bufferOffsetWrtChunkData);
        } else {
          // Read a required chunk data to fill the buffers with seeked
          // position data
          readChunkFromContainer(len);
        }
      }
      if (buffersHaveData()) {
        // Data is available from buffers
        ByteBuffer bb = buffers[bufferIndex];
        return len > bb.remaining() ? bb.remaining() : len;
      } else if (dataRemainingInChunk()) {
        // There is more data in the chunk stream which has not
        // been read into the buffers yet.
        readChunkFromContainer(len);
      } else {
        // All available input from this chunk stream has been consumed.
        return EOF;
      }
    }
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
  private synchronized void readChunkFromContainer(int len) throws IOException {

    // index of first byte to be read from the chunk
    long startByteIndex;
    if (chunkPosition >= 0) {
      // If seek operation was called to advance the buffer position, the
      // chunk should be read from that position onwards.
      startByteIndex = chunkPosition;
    } else {
      // Start reading the chunk from the last chunkPosition onwards.
      startByteIndex = bufferOffsetWrtChunkData + buffersSize;
    }

    // bufferOffsetWrtChunkData and buffersSize are updated after the data
    // is read from Container and put into the buffers, but if read fails
    // and is retried, we need the previous position.  Position is reset after
    // successful read in adjustBufferPosition()
    storePosition();

    long adjustedBuffersOffset, adjustedBuffersLen;
    if (verifyChecksum) {
      // Adjust the chunk offset and length to include required checksum
      // boundaries
      Pair<Long, Long> adjustedOffsetAndLength =
          computeChecksumBoundaries(startByteIndex, len);
      adjustedBuffersOffset = adjustedOffsetAndLength.getLeft();
      adjustedBuffersLen = adjustedOffsetAndLength.getRight();
    } else {
      // Read from the startByteIndex
      adjustedBuffersOffset = startByteIndex;
      adjustedBuffersLen = len;
    }

    // Adjust the chunkInfo so that only the required bytes are read from
    // the chunk.
    final ChunkInfo adjustedChunkInfo = ChunkInfo.newBuilder(chunkInfo)
        .setOffset(chunkInfo.getOffset() + adjustedBuffersOffset)
        .setLen(adjustedBuffersLen)
        .build();

    readChunkDataIntoBuffers(adjustedChunkInfo);
    bufferOffsetWrtChunkData = adjustedBuffersOffset;

    // If the stream was seeked to position before, then the buffer
    // position should be adjusted as the reads happen at checksum boundaries.
    // The buffers position might need to be adjusted for the following
    // scenarios:
    //    1. Stream was seeked to a position before the chunk was read
    //    2. Chunk was read from index < the current position to account for
    //    checksum boundaries.
    adjustBufferPosition(startByteIndex - bufferOffsetWrtChunkData);
  }

  private void readChunkDataIntoBuffers(ChunkInfo readChunkInfo)
      throws IOException {
    buffers = readChunk(readChunkInfo);
    buffersSize = readChunkInfo.getLen();

    bufferOffsets = new long[buffers.length];
    int tempOffset = 0;
    for (int i = 0; i < buffers.length; i++) {
      bufferOffsets[i] = tempOffset;
      tempOffset += buffers[i].limit();
    }

    bufferIndex = 0;
    firstUnreleasedBufferIndex = 0;
    allocated = true;
  }

  /**
   * Send RPC call to get the chunk from the container.
   */
  @VisibleForTesting
  protected ByteBuffer[] readChunk(ChunkInfo readChunkInfo)
      throws IOException {
    ReadChunkResponseProto readChunkResponse;

    try {
      List<CheckedBiFunction> validators =
          ContainerProtocolCalls.getValidatorList();
      validators.add(validator);

      readChunkResponse = ContainerProtocolCalls.readChunk(xceiverClient,
          readChunkInfo, blockID, validators, token);

    } catch (IOException e) {
      if (e instanceof StorageContainerException) {
        throw e;
      }
      throw new IOException("Unexpected OzoneException: " + e.toString(), e);
    }

    if (readChunkResponse.hasData()) {
      return readChunkResponse.getData().asReadOnlyByteBufferList()
          .toArray(new ByteBuffer[0]);
    } else if (readChunkResponse.hasDataBuffers()) {
      List<ByteString> buffersList = readChunkResponse.getDataBuffers()
          .getBuffersList();
      return BufferUtils.getReadOnlyByteBuffersArray(buffersList);
    } else {
      throw new IOException("Unexpected error while reading chunk data " +
          "from container. No data returned.");
    }
  }

  private CheckedBiFunction<ContainerCommandRequestProto,
      ContainerCommandResponseProto, IOException> validator =
          (request, response) -> {
            final ChunkInfo reqChunkInfo =
                request.getReadChunk().getChunkData();

            ReadChunkResponseProto readChunkResponse = response.getReadChunk();
            List<ByteString> byteStrings;
            boolean isV0 = false;

            if (readChunkResponse.hasData()) {
              ByteString byteString = readChunkResponse.getData();
              if (byteString.size() != reqChunkInfo.getLen()) {
                // Bytes read from chunk should be equal to chunk size.
                throw new OzoneChecksumException(String.format(
                    "Inconsistent read for chunk=%s len=%d bytesRead=%d",
                    reqChunkInfo.getChunkName(), reqChunkInfo.getLen(),
                    byteString.size()));
              }
              byteStrings = new ArrayList<>();
              byteStrings.add(byteString);
              isV0 = true;
            } else {
              byteStrings = readChunkResponse.getDataBuffers().getBuffersList();
              long buffersLen = BufferUtils.getBuffersLen(byteStrings);
              if (buffersLen != reqChunkInfo.getLen()) {
                // Bytes read from chunk should be equal to chunk size.
                throw new OzoneChecksumException(String.format(
                    "Inconsistent read for chunk=%s len=%d bytesRead=%d",
                    reqChunkInfo.getChunkName(), reqChunkInfo.getLen(),
                    buffersLen));
              }
            }

            if (verifyChecksum) {
              ChecksumData checksumData = ChecksumData.getFromProtoBuf(
                  chunkInfo.getChecksumData());

              // ChecksumData stores checksum for each 'numBytesPerChecksum'
              // number of bytes in a list. Compute the index of the first
              // checksum to match with the read data

              long relativeOffset = reqChunkInfo.getOffset() -
                  chunkInfo.getOffset();
              int bytesPerChecksum = checksumData.getBytesPerChecksum();
              int startIndex = (int) (relativeOffset / bytesPerChecksum);
              Checksum.verifyChecksum(byteStrings, checksumData, startIndex,
                  isV0);
            }
          };

  /**
   * Return the offset and length of bytes that need to be read from the
   * chunk file to cover the checksum boundaries covering the actual start and
   * end of the chunk index to be read.
   * For example, lets say the client is reading from index 120 to 450 in the
   * chunk. And let's say checksum is stored for every 100 bytes in the chunk
   * i.e. the first checksum is for bytes from index 0 to 99, the next for
   * bytes from index 100 to 199 and so on. To verify bytes from 120 to 450,
   * we would need to read from bytes 100 to 499 so that checksum
   * verification can be done.
   *
   * @param startByteIndex the first byte index to be read by client
   * @param dataLen number of bytes to be read from the chunk
   * @return Adjusted (Chunk Offset, Chunk Length) which needs to be read
   * from Container
   */
  private Pair<Long, Long> computeChecksumBoundaries(long startByteIndex,
      int dataLen) {

    int bytesPerChecksum = chunkInfo.getChecksumData().getBytesPerChecksum();
    // index of the last byte to be read from chunk, inclusively.
    final long endByteIndex = startByteIndex + dataLen - 1;

    long adjustedChunkOffset =  (startByteIndex / bytesPerChecksum)
        * bytesPerChecksum; // inclusive
    final long endIndex = ((endByteIndex / bytesPerChecksum) + 1)
        * bytesPerChecksum; // exclusive
    long adjustedChunkLen = Math.min(endIndex, length) - adjustedChunkOffset;
    return Pair.of(adjustedChunkOffset, adjustedChunkLen);
  }

  /**
   * Adjust the buffers position to account for seeked position and/ or checksum
   * boundary reads.
   * @param bufferPosition the position to which the buffers must be advanced
   */
  private void adjustBufferPosition(long bufferPosition) {
    // The bufferPosition is w.r.t the current buffers.
    // Adjust the bufferIndex and position to the seeked bufferPosition.
    if (bufferIndex >= buffers.length) {
      bufferIndex = Arrays.binarySearch(bufferOffsets, bufferPosition);
    } else if (bufferPosition < bufferOffsets[bufferIndex]) {
      bufferIndex = Arrays.binarySearch(bufferOffsets, 0, bufferIndex,
          bufferPosition);
    } else if (bufferPosition >= bufferOffsets[bufferIndex] +
        buffers[bufferIndex].capacity()) {
      bufferIndex = Arrays.binarySearch(bufferOffsets, bufferIndex + 1,
          buffers.length, bufferPosition);
    }
    if (bufferIndex < 0) {
      bufferIndex = -bufferIndex - 2;
    }

    buffers[bufferIndex].position(
        (int) (bufferPosition - bufferOffsets[bufferIndex]));

    // Reset buffers > bufferIndex to position 0. We do this to reset any
    // previous reads/ seeks which might have updated any buffer position.
    // For buffers < bufferIndex, we do not need to reset the position as it
    // not required for this read. If a seek was done to a position in the
    // previous indices, the buffer position reset would be performed in the
    // seek call.
    for (int i = bufferIndex + 1; i < buffers.length; i++) {
      buffers[i].position(0);
    }

    // Reset the chunkPosition as chunk stream has been initialized i.e. the
    // buffers have been allocated.
    resetPosition();
  }

  /**
   * Check if the buffers have been allocated data and false otherwise.
   */
  @VisibleForTesting
  protected boolean buffersAllocated() {
    return buffers != null && buffers.length > 0;
  }

  /**
   * Check if the buffers have any data remaining between the current
   * position and the limit.
   */
  private boolean buffersHaveData() {
    boolean hasData = false;

    if (buffersAllocated()) {
      while (bufferIndex < (buffers.length)) {
        if (buffers[bufferIndex] != null &&
            buffers[bufferIndex].hasRemaining()) {
          // current buffer has data
          hasData = true;
          break;
        } else {
          if (buffersRemaining()) {
            // move to next available buffer
            ++bufferIndex;
            Preconditions.checkState(bufferIndex < buffers.length);
          } else {
            // no more buffers remaining
            break;
          }
        }
      }
    }

    return hasData;
  }

  private boolean buffersRemaining() {
    return (bufferIndex < (buffers.length - 1));
  }

  /**
   * Check if curernt buffers have the data corresponding to the input position.
   */
  private boolean buffersHavePosition(long pos) {
    // Check if buffers have been allocated
    if (buffersAllocated()) {
      // Check if the current buffers cover the input position
      // Released buffers should not be considered when checking if position
      // is available
      return pos >= bufferOffsetWrtChunkData +
          bufferOffsets[firstUnreleasedBufferIndex] &&
          pos < bufferOffsetWrtChunkData + buffersSize;
    }
    return false;
  }

  /**
   * Check if there is more data in the chunk which has not yet been read
   * into the buffers.
   */
  private boolean dataRemainingInChunk() {
    long bufferPos;
    if (chunkPosition >= 0) {
      bufferPos = chunkPosition;
    } else {
      bufferPos = bufferOffsetWrtChunkData + buffersSize;
    }

    return bufferPos < length;
  }

  /**
   * Check if current buffer had been read till the end.
   */
  private boolean bufferEOF() {
    if (!allocated) {
      // Chunk data has not been read yet
      return false;
    }

    if (!buffers[bufferIndex].hasRemaining()) {
      return true;
    }
    return false;
  }

  /**
   * Check if end of chunkStream has been reached.
   */
  private boolean chunkStreamEOF() {
    if (!allocated) {
      // Chunk data has not been read yet
      return false;
    }

    if (buffersHaveData() || dataRemainingInChunk()) {
      return false;
    } else {
      Preconditions.checkState(
          bufferOffsetWrtChunkData + buffersSize == length,
          "EOF detected but not at the last byte of the chunk");
      return true;
    }
  }


  /**
   * Release the buffers upto the given index.
   * @param releaseUptoBufferIndex bufferIndex (inclusive) upto which the
   *                               buffers must be released
   */
  private void releaseBuffers(int releaseUptoBufferIndex) {
    if (releaseUptoBufferIndex == buffers.length - 1) {
      // Before releasing all the buffers, if chunk EOF is not reached, then
      // chunkPosition should be set to point to the last position of the
      // buffers. This should be done so that getPos() can return the current
      // chunk position
      chunkPosition = bufferOffsetWrtChunkData +
          bufferOffsets[releaseUptoBufferIndex] +
          buffers[releaseUptoBufferIndex].capacity();
      // Release all the buffers
      releaseBuffers();
    } else {
      for (int i = 0; i <= releaseUptoBufferIndex; i++) {
        buffers[i] = null;
      }
      firstUnreleasedBufferIndex = releaseUptoBufferIndex + 1;
    }
  }

  /**
   * If EOF is reached, release the buffers.
   */
  private void releaseBuffers() {
    buffers = null;
    bufferIndex = 0;
    firstUnreleasedBufferIndex = 0;
    // We should not reset bufferOffsetWrtChunkData and buffersSize here
    // because when getPos() is called in chunkStreamEOF() we use these
    // values and determine whether chunk is read completely or not.
  }

  /**
   * Reset the chunkPosition once the buffers are allocated.
   */
  void resetPosition() {
    this.chunkPosition = -1;
  }

  private void storePosition() {
    chunkPosition = getPos();
  }

  String getChunkName() {
    return chunkInfo.getChunkName();
  }

  protected long getLength() {
    return length;
  }

  @VisibleForTesting
  protected long getChunkPosition() {
    return chunkPosition;
  }

  @Override
  public synchronized void unbuffer() {
    storePosition();
    releaseBuffers();
    releaseClient();
  }

  @VisibleForTesting
  public ByteBuffer[] getCachedBuffers() {
    return BufferUtils.getReadOnlyByteBuffers(buffers);
  }
}
