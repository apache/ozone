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
import com.google.common.base.Preconditions;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.XceiverClientSpi.Validator;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link InputStream} called from KeyInputStream to read a block from the
 * container.
 * This class encapsulates all state management for iterating
 * through the sequence of chunks through {@link ChunkInputStream}.
 */
public class BlockInputStream extends BlockExtendedInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(BlockInputStream.class);

  private static final List<Validator> VALIDATORS =
      ContainerProtocolCalls.toValidatorList((request, response) -> validate(response));

  private final BlockID blockID;
  private long length;
  private final BlockLocationInfo blockInfo;
  private final AtomicReference<Pipeline> pipelineRef =
      new AtomicReference<>();
  private final AtomicReference<Token<OzoneBlockTokenIdentifier>> tokenRef =
      new AtomicReference<>();
  private final boolean verifyChecksum;
  private XceiverClientFactory xceiverClientFactory;
  private XceiverClientSpi xceiverClient;
  private boolean initialized = false;
  // TODO: do we need to change retrypolicy based on exception.
  private final RetryPolicy retryPolicy;

  private int retries;

  // List of ChunkInputStreams, one for each chunk in the block
  private List<ChunkInputStream> chunkStreams;

  // chunkOffsets[i] stores the index of the first data byte in
  // chunkStream i w.r.t the block data.
  // Let’s say we have chunk size as 40 bytes. And let's say the parent
  // block stores data from index 200 and has length 400.
  // The first 40 bytes of this block will be stored in chunk[0], next 40 in
  // chunk[1] and so on. But since the chunkOffsets are w.r.t the block only
  // and not the key, the values in chunkOffsets will be [0, 40, 80,....].
  private long[] chunkOffsets = null;

  // Index of the chunkStream corresponding to the current position of the
  // BlockInputStream i.e offset of the data to be read next from this block
  private int chunkIndex;

  // Position of the BlockInputStream is maintained by this variable till
  // the stream is initialized. This position is w.r.t to the block only and
  // not the key.
  // For the above example, if we seek to position 240 before the stream is
  // initialized, then value of blockPosition will be set to 40.
  // Once, the stream is initialized, the position of the stream
  // will be determined by the current chunkStream and its position.
  private long blockPosition = 0;

  // Tracks the chunkIndex corresponding to the last blockPosition so that it
  // can be reset if a new position is seeked.
  private int chunkIndexOfPrevPosition;

  private final Function<BlockID, BlockLocationInfo> refreshFunction;

  private BlockData blockData;

  public BlockInputStream(
      BlockLocationInfo blockInfo,
      Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverClientFactory,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      OzoneClientConfig config) throws IOException {
    this.blockInfo = blockInfo;
    this.blockID = blockInfo.getBlockID();
    this.length = blockInfo.getLength();
    pipelineRef.set(setPipeline(pipeline));
    tokenRef.set(token);
    this.verifyChecksum = config.isChecksumVerify();
    this.xceiverClientFactory = xceiverClientFactory;
    this.refreshFunction = refreshFunction;
    this.retryPolicy = getReadRetryPolicy(config);
  }

  // only for unit tests
  public BlockInputStream(BlockID blockId, long blockLen, Pipeline pipeline,
                          Token<OzoneBlockTokenIdentifier> token,
                          XceiverClientFactory xceiverClientFactory,
                          OzoneClientConfig config
  ) throws IOException {
    this(new BlockLocationInfo(new BlockLocationInfo.Builder().setBlockID(blockId).setLength(blockLen)),
        pipeline, token, xceiverClientFactory, null, config);
  }

  /**
   * Initialize the BlockInputStream. Get the BlockData (list of chunks) from
   * the Container and create the ChunkInputStreams for each Chunk in the Block.
   */
  public synchronized void initialize() throws IOException {

    // Pre-check that the stream has not been intialized already
    if (initialized) {
      return;
    }

    List<ChunkInfo> chunks = null;
    IOException catchEx = null;
    do {
      try {
        blockData = getBlockData();
        chunks = blockData.getChunksList();
        if (blockInfo != null && blockInfo.isUnderConstruction()) {
          // use the block length from DN if block is under construction.
          length = blockData.getSize();
          LOG.debug("Updated block length to {} for block {}", length, blockID);
        }
        break;
        // If we get a StorageContainerException or an IOException due to
        // datanodes are not reachable, refresh to get the latest pipeline
        // info and retry.
        // Otherwise, just retry according to the retry policy.
      } catch (SCMSecurityException ex) {
        throw ex;
      } catch (StorageContainerException ex) {
        refreshBlockInfo(ex);
        catchEx = ex;
      } catch (IOException ex) {
        LOG.debug("Retry to get chunk info fail", ex);
        if (isConnectivityIssue(ex)) {
          refreshBlockInfo(ex);
        }
        catchEx = ex;
      }
    } while (shouldRetryRead(catchEx, retryPolicy, ++retries));

    if (chunks == null) {
      throw catchEx;
    } else {
      // Reset retry count if we get chunks successfully.
      retries = 0;
    }

    if (!chunks.isEmpty()) {
      // For each chunk in the block, create a ChunkInputStream and compute
      // its chunkOffset
      this.chunkOffsets = new long[chunks.size()];
      long tempOffset = 0;

      this.chunkStreams = new ArrayList<>(chunks.size());
      for (int i = 0; i < chunks.size(); i++) {
        addStream(chunks.get(i));
        chunkOffsets[i] = tempOffset;
        tempOffset += chunks.get(i).getLen();
      }

      initialized = true;
      this.chunkIndex = 0;

      if (blockPosition > 0) {
        // Stream was seeked to blockPosition before initialization. Seek to the
        // blockPosition now.
        seek(blockPosition);
      }
    }
  }

  private void refreshBlockInfo(IOException cause) throws IOException {
    refreshBlockInfo(cause, blockID, pipelineRef, tokenRef, refreshFunction);
  }

  /**
   * Send RPC call to get the block info from the container.
   * @return BlockData.
   */
  protected BlockData getBlockData() throws IOException {
    acquireClient();
    try {
      return getBlockDataUsingClient();
    } finally {
      releaseClient();
    }
  }

  /**
   * Send RPC call to get the block info from the container.
   * @return BlockData.
   */
  protected BlockData getBlockDataUsingClient() throws IOException {
    Pipeline pipeline = pipelineRef.get();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing BlockInputStream for get key to access block {}",
          blockID);
    }

    GetBlockResponseProto response = ContainerProtocolCalls.getBlock(
        xceiverClient, VALIDATORS, blockID, tokenRef.get(), pipeline.getReplicaIndexes());
    return response.getBlockData();
  }

  private static void validate(ContainerCommandResponseProto response)
      throws IOException {
    if (!response.hasGetBlock()) {
      throw new IllegalArgumentException("Not GetBlock: response=" + response);
    }
    final GetBlockResponseProto b = response.getGetBlock();
    final long blockLength = b.getBlockData().getSize();
    final List<ChunkInfo> chunks = b.getBlockData().getChunksList();
    for (int i = 0; i < chunks.size(); i++) {
      final ChunkInfo c = chunks.get(i);
      // HDDS-10682 caused an empty chunk to get written to the end of some EC blocks. Due to this
      // validation, these blocks will not be readable. In the EC case, the empty chunk is always
      // the last chunk and the offset is the block length. We can safely ignore this case and not fail.
      if (c.getLen() <= 0 && i == chunks.size() - 1 && c.getOffset() == blockLength) {
        DatanodeBlockID blockID = b.getBlockData().getBlockID();
        LOG.warn("The last chunk is empty for container/block {}/{} with an offset of the block length. " +
            "Likely due to HDDS-10682. This is safe to ignore.", blockID.getContainerID(), blockID.getLocalID());
      } else if (c.getLen() <= 0) {
        throw new IOException("Failed to get chunkInfo["
            + i + "]: len == " + c.getLen());
      }
    }
  }

  private void acquireClient() throws IOException {
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

  /**
   * Append another ChunkInputStream to the end of the list. Note that the
   * ChunkInputStream is only created here. The chunk will be read from the
   * Datanode only when a read operation is performed on for that chunk.
   */
  protected synchronized void addStream(ChunkInfo chunkInfo) {
    chunkStreams.add(createChunkInputStream(chunkInfo));
  }

  protected ChunkInputStream createChunkInputStream(ChunkInfo chunkInfo) {
    return new ChunkInputStream(chunkInfo, blockID,
        xceiverClientFactory, pipelineRef::get, verifyChecksum, tokenRef::get);
  }

  @Override
  protected synchronized int readWithStrategy(ByteReaderStrategy strategy)
      throws IOException {
    Preconditions.checkArgument(strategy != null);
    if (!initialized) {
      initialize();
    }

    checkOpen();
    int totalReadLen = 0;
    int len = strategy.getTargetLength();
    while (len > 0) {
      // if we are at the last chunk and have read the entire chunk, return
      if (chunkStreams.isEmpty() ||
          (chunkStreams.size() - 1 <= chunkIndex &&
              chunkStreams.get(chunkIndex)
                  .getRemaining() == 0)) {
        return totalReadLen == 0 ? EOF : totalReadLen;
      }

      // Get the current chunkStream and read data from it
      ChunkInputStream current = chunkStreams.get(chunkIndex);
      int numBytesToRead = Math.min(len, (int)current.getRemaining());
      int numBytesRead;
      try {
        numBytesRead = strategy.readFromBlock(current, numBytesToRead);
        retries = 0;
        // If we get a StorageContainerException or an IOException due to
        // datanodes are not reachable, refresh to get the latest pipeline
        // info and retry.
        // Otherwise, just retry according to the retry policy.
      } catch (SCMSecurityException ex) {
        throw ex;
      } catch (StorageContainerException e) {
        if (shouldRetryRead(e, retryPolicy, ++retries)) {
          handleReadError(e);
          continue;
        } else {
          throw e;
        }
      } catch (IOException ex) {
        if (shouldRetryRead(ex, retryPolicy, ++retries)) {
          if (isConnectivityIssue(ex)) {
            handleReadError(ex);
          } else {
            current.releaseClient();
          }
          continue;
        } else {
          throw ex;
        }
      }

      if (numBytesRead != numBytesToRead) {
        // This implies that there is either data loss or corruption in the
        // chunk entries. Even EOF in the current stream would be covered in
        // this case.
        throw new IOException(String.format(
            "Inconsistent read for chunkName=%s length=%d numBytesToRead= %d " +
                "numBytesRead=%d", current.getChunkName(), current.getLength(),
            numBytesToRead, numBytesRead));
      }
      totalReadLen += numBytesRead;
      len -= numBytesRead;
      if (current.getRemaining() <= 0 &&
          ((chunkIndex + 1) < chunkStreams.size())) {
        chunkIndex += 1;
      }
    }
    return totalReadLen;
  }

  /**
   * Seeks the BlockInputStream to the specified position. If the stream is
   * not initialized, save the seeked position via blockPosition. Otherwise,
   * update the position in 2 steps:
   *    1. Updating the chunkIndex to the chunkStream corresponding to the
   *    seeked position.
   *    2. Seek the corresponding chunkStream to the adjusted position.
   *
   * Let’s say we have chunk size as 40 bytes. And let's say the parent block
   * stores data from index 200 and has length 400. If the key was seeked to
   * position 90, then this block will be seeked to position 90.
   * When seek(90) is called on this blockStream, then
   *    1. chunkIndex will be set to 2 (as indices 80 - 120 reside in chunk[2]).
   *    2. chunkStream[2] will be seeked to position 10
   *       (= 90 - chunkOffset[2] (= 80)).
   */
  @Override
  public synchronized void seek(long pos) throws IOException {
    if (!initialized) {
      // Stream has not been initialized yet. Save the position so that it
      // can be seeked when the stream is initialized.
      blockPosition = pos;
      return;
    }

    checkOpen();
    if (pos < 0 || pos > length) {
      if (pos == 0) {
        // It is possible for length and pos to be zero in which case
        // seek should return instead of throwing exception
        return;
      }
      throw new EOFException(
          "EOF encountered at pos: " + pos + " for block: " + blockID);
    }

    if (chunkIndex >= chunkStreams.size()) {
      chunkIndex = Arrays.binarySearch(chunkOffsets, pos);
    } else if (pos < chunkOffsets[chunkIndex]) {
      chunkIndex =
          Arrays.binarySearch(chunkOffsets, 0, chunkIndex, pos);
    } else if (pos >= chunkOffsets[chunkIndex] + chunkStreams
        .get(chunkIndex).getLength()) {
      chunkIndex = Arrays.binarySearch(chunkOffsets,
          chunkIndex + 1, chunkStreams.size(), pos);
    }
    if (chunkIndex < 0) {
      // Binary search returns -insertionPoint - 1  if element is not present
      // in the array. insertionPoint is the point at which element would be
      // inserted in the sorted array. We need to adjust the chunkIndex
      // accordingly so that chunkIndex = insertionPoint - 1
      chunkIndex = -chunkIndex - 2;
    }

    // Reset the previous chunkStream's position
    chunkStreams.get(chunkIndexOfPrevPosition).resetPosition();

    // Reset all the chunkStreams above the chunkIndex. We do this to reset
    // any previous reads which might have updated the chunkPosition.
    for (int index =  chunkIndex + 1; index < chunkStreams.size(); index++) {
      chunkStreams.get(index).seek(0);
    }
    // seek to the proper offset in the ChunkInputStream
    chunkStreams.get(chunkIndex).seek(pos - chunkOffsets[chunkIndex]);
    chunkIndexOfPrevPosition = chunkIndex;
  }

  @Override
  public synchronized long getPos() {
    if (length == 0) {
      return 0;
    }

    if (!initialized) {
      // The stream is not initialized yet. Return the blockPosition
      return blockPosition;
    } else {
      return chunkOffsets[chunkIndex] + chunkStreams.get(chunkIndex).getPos();
    }
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized void close() {
    releaseClient();
    xceiverClientFactory = null;

    final List<ChunkInputStream> inputStreams = this.chunkStreams;
    if (inputStreams != null) {
      for (ChunkInputStream is : inputStreams) {
        is.close();
      }
    }
  }

  private void releaseClient() {
    if (xceiverClientFactory != null && xceiverClient != null) {
      xceiverClientFactory.releaseClientForReadData(xceiverClient, false);
      xceiverClient = null;
    }
  }

  /**
   * Checks if the stream is open.  If not, throw an exception.
   *
   * @throws IOException if stream is closed
   */
  protected synchronized void checkOpen() throws IOException {
    if (xceiverClientFactory == null) {
      throw new IOException("BlockInputStream has been closed.");
    }
  }

  @Override
  public BlockID getBlockID() {
    return blockID;
  }

  @Override
  public long getLength() {
    return length;
  }

  public synchronized int getChunkIndex() {
    return chunkIndex;
  }

  @VisibleForTesting
  synchronized long getBlockPosition() {
    return blockPosition;
  }

  @Override
  public synchronized void unbuffer() {
    storePosition();
    releaseClient();

    final List<ChunkInputStream> inputStreams = this.chunkStreams;
    if (inputStreams != null) {
      for (ChunkInputStream is : inputStreams) {
        is.unbuffer();
      }
    }
  }

  private synchronized void storePosition() {
    blockPosition = getPos();
  }

  private void handleReadError(IOException cause) throws IOException {
    releaseClient();
    final List<ChunkInputStream> inputStreams = this.chunkStreams;
    if (inputStreams != null) {
      for (ChunkInputStream is : inputStreams) {
        is.releaseClient();
      }
    }

    refreshBlockInfo(cause);
  }

  public synchronized List<ChunkInputStream> getChunkStreams() {
    return chunkStreams;
  }

  public BlockData getStreamBlockData() {
    return blockData;
  }

}
