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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.token.Token;

import com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link InputStream} called from KeyInputStream to read a block from the
 * container.
 * This class encapsulates all state management for iterating
 * through the sequence of chunks through {@link ChunkInputStream}.
 */
public class BlockInputStream extends BlockExtendedInputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(BlockInputStream.class);

  private final BlockID blockID;
  private final long length;
  private Pipeline pipeline;
  private final Token<OzoneBlockTokenIdentifier> token;
  private final boolean verifyChecksum;
  private XceiverClientFactory xceiverClientFactory;
  private XceiverClientSpi xceiverClient;
  // TODO: do we need to change retrypolicy based on exception.
  private final RetryPolicy retryPolicy =
      HddsClientUtils.createRetryPolicy(3, TimeUnit.SECONDS.toMillis(1));
  private int retries;

  // Position of the BlockInputStream is maintained by this variable till
  // the stream is initialized. This position is w.r.t to the block only and
  // not the key.
  // For the above example, if we seek to position 240 before the stream is
  // initialized, then value of blockPosition will be set to 40.
  // Once, the stream is initialized, the position of the stream
  // will be determined by the current chunkStream and its position.
  private long blockPosition;

  private final Function<BlockID, Pipeline> refreshPipelineFunction;

  private MultiBlockInputStream delegateStream;

  public BlockInputStream(BlockID blockId, long blockLen, Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token, boolean verifyChecksum,
      XceiverClientFactory xceiverClientFactory,
      Function<BlockID, Pipeline> refreshPipelineFunction) {
    this.blockID = blockId;
    this.length = blockLen;
    this.pipeline = pipeline;
    this.token = token;
    this.verifyChecksum = verifyChecksum;
    this.xceiverClientFactory = xceiverClientFactory;
    this.refreshPipelineFunction = refreshPipelineFunction;
  }

  public BlockInputStream(BlockID blockId, long blockLen, Pipeline pipeline,
                          Token<OzoneBlockTokenIdentifier> token,
                          boolean verifyChecksum,
                          XceiverClientFactory xceiverClientFactory
  ) {
    this(blockId, blockLen, pipeline, token, verifyChecksum,
        xceiverClientFactory, null);
  }

  /**
   * Initialize the BlockInputStream. Get the BlockData (list of chunks) from
   * the Container and create the ChunkInputStreams for each Chunk in the Block.
   */
  public synchronized void initialize() throws IOException {
    // Pre-check that the stream has not been initialized already
    if (isInitialized()) {
      return;
    }
    List<ChunkInfo> chunks = retrieveChunksWithRetries();
    List<ChunkInputStream> chunkStreams = new ArrayList<>(chunks.size());
    for (ChunkInfo chunk : chunks) {
      // Append another ChunkInputStream to the end of the list. Note that the
      // ChunkInputStream is only created here. The chunk will be read from the
      // Datanode only when a read operation is performed on for that chunk.
      chunkStreams.add(createChunkInputStream(chunk));
    }

    this.delegateStream = new MultiBlockInputStream(blockID, chunkStreams);

    if (blockPosition > 0) {
      // Stream was seeked to blockPosition before initialization. Seek to the
      // blockPosition now.
      seek(blockPosition);
    }
  }

  private List<ChunkInfo> retrieveChunksWithRetries() throws IOException {
    List<ChunkInfo> chunks = null;
    IOException catchEx = null;
    do {
      try {
        chunks = getChunkInfos();
        break;
        // If we get a StorageContainerException or an IOException due to
        // datanodes are not reachable, refresh to get the latest pipeline
        // info and retry.
        // Otherwise, just retry according to the retry policy.
      } catch (SCMSecurityException ex) {
        throw ex;
      } catch (StorageContainerException ex) {
        refreshPipeline(ex);
        catchEx = ex;
      } catch (IOException ex) {
        LOG.debug("Retry to get chunk info fail", ex);
        if (isConnectivityIssue(ex)) {
          refreshPipeline(ex);
        }
        catchEx = ex;
      }
    } while (shouldRetryRead(catchEx));

    if (chunks == null) {
      throw catchEx;
    } else if (chunks.isEmpty()) {
      throw new IOException("Empty list of chunks " + blockID);
    } else {
      // Reset retry count if we get chunks successfully.
      retries = 0;
    }
    return chunks;
  }

  private boolean isInitialized() {
    return delegateStream != null;
  }

  /**
   * Check if this exception is because datanodes are not reachable.
   */
  private boolean isConnectivityIssue(IOException ex) {
    return Status.fromThrowable(ex).getCode() == Status.UNAVAILABLE.getCode();
  }

  private void refreshPipeline(IOException cause) throws IOException {
    LOG.info("Unable to read information for block {} from pipeline {}: {}",
        blockID, pipeline.getId(), cause.getMessage());
    if (refreshPipelineFunction != null) {
      LOG.debug("Re-fetching pipeline for block {}", blockID);
      this.pipeline = refreshPipelineFunction.apply(blockID);
    } else {
      throw cause;
    }
  }

  /**
   * Send RPC call to get the block info from the container.
   *
   * @return List of chunks in this block.
   */
  protected List<ChunkInfo> getChunkInfos() throws IOException {
    // irrespective of the container state, we will always read via Standalone
    // protocol.
    if (pipeline.getType() != HddsProtos.ReplicationType.STAND_ALONE && pipeline
        .getType() != HddsProtos.ReplicationType.EC) {
      pipeline = Pipeline.newBuilder(pipeline)
          .setReplicationConfig(StandaloneReplicationConfig.getInstance(
              ReplicationConfig
                  .getLegacyFactor(pipeline.getReplicationConfig())))
          .build();
    }
    acquireClient();
    boolean success = false;
    List<ChunkInfo> chunks;
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initializing BlockInputStream for get key to access {}",
            blockID.getContainerID());
      }

      DatanodeBlockID.Builder blkIDBuilder =
          DatanodeBlockID.newBuilder().setContainerID(blockID.getContainerID())
              .setLocalID(blockID.getLocalID())
              .setBlockCommitSequenceId(blockID.getBlockCommitSequenceId());

      int replicaIndex = pipeline.getReplicaIndex(pipeline.getClosestNode());
      if (replicaIndex > 0) {
        blkIDBuilder.setReplicaIndex(replicaIndex);
      }
      GetBlockResponseProto response = ContainerProtocolCalls
          .getBlock(xceiverClient, blkIDBuilder.build(), token);

      chunks = response.getBlockData().getChunksList();
      success = true;
    } finally {
      if (!success) {
        xceiverClientFactory.releaseClientForReadData(xceiverClient, false);
      }
    }

    return chunks;
  }

  protected void acquireClient() throws IOException {
    xceiverClient = xceiverClientFactory.acquireClientForReadData(pipeline);
  }

  protected ChunkInputStream createChunkInputStream(ChunkInfo chunkInfo) {
    return new ChunkInputStream(chunkInfo, blockID,
        xceiverClientFactory, () -> pipeline, verifyChecksum, token);
  }

  @Override
  protected synchronized int readWithStrategy(ByteReaderStrategy strategy)
      throws IOException {
    Preconditions.checkArgument(strategy != null);
    if (!isInitialized()) {
      initialize();
    }
    return delegateStream.readWithStrategy(strategy);
  }

  /**
   * Seeks the BlockInputStream to the specified position. If the stream is
   * not initialized, save the seeked position via blockPosition. Otherwise,
   * update the position in 2 steps:
   * 1. Updating the chunkIndex to the chunkStream corresponding to the
   * seeked position.
   * 2. Seek the corresponding chunkStream to the adjusted position.
   * <p>
   * Let’s say we have chunk size as 40 bytes. And let's say the parent block
   * stores data from index 200 and has length 400. If the key was seeked to
   * position 90, then this block will be seeked to position 90.
   * When seek(90) is called on this blockStream, then
   * 1. chunkIndex will be set to 2 (as indices 80 - 120 reside in chunk[2]).
   * 2. chunkStream[2] will be seeked to position 10
   * (= 90 - chunkOffset[2] (= 80)).
   */
  @Override
  public synchronized void seek(long pos) throws IOException {
    if (!isInitialized()) {
      // Stream has not been initialized yet. Save the position so that it
      // can be seeked when the stream is initialized.
      blockPosition = pos;
      return;
    }

    delegateStream.seek(pos);
  }

  @Override
  public synchronized long getPos() {
    if (length == 0) {
      return 0;
    }

    if (!isInitialized()) {
      // The stream is not initialized yet. Return the blockPosition
      return blockPosition;
    } else {
      return delegateStream.getPos();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    releaseClient();
    xceiverClientFactory = null;

    if (isInitialized()) {
      delegateStream.close();
    }
  }

  private void releaseClient() {
    if (xceiverClientFactory != null && xceiverClient != null) {
      xceiverClientFactory.releaseClient(xceiverClient, false);
      xceiverClient = null;
    }
  }

  /**
   * Checks if the stream is open.  If not, throw an exception.
   *
   * @throws IOException if stream is closed
   */
  protected synchronized void checkClientOpen() throws IOException {
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

  @VisibleForTesting
  synchronized int getChunkIndex() {
    return isInitialized() ? delegateStream.getCurrentStreamIndex() : 0;
  }

  @VisibleForTesting
  synchronized long getBlockPosition() {
    return blockPosition;
  }

  @Override
  public synchronized void unbuffer() {
    storePosition();
    releaseClient();

    if (isInitialized()) {
      delegateStream.unbuffer();
    }
  }

  private synchronized void storePosition() {
    blockPosition = getPos();
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

  private void handleReadError(IOException cause) throws IOException {
    releaseClient();
    if (isInitialized()) {
      delegateStream.releaseClient();
    }
    refreshPipeline(cause);
  }

  @VisibleForTesting
  public synchronized List<ChunkInputStream> getChunkStreams() {
    return delegateStream.getPartStreams();
  }

  @VisibleForTesting
  public static Logger getLog() {
    return LOG;
  }

  private class MultiBlockInputStream extends MultipartInputStream {

    MultiBlockInputStream(BlockID blockID,
                          List<ChunkInputStream> inputStreams) {
      super(" Block: " + blockID, inputStreams);
    }

    @Override
    protected int readWithStrategy(ByteReaderStrategy strategy,
                                   InputStream current,
                                   int numBytesToRead)
        throws IOException, RetriableIOException {
      int numBytesRead;
      try {
        numBytesRead =
            super.readWithStrategy(strategy, current, numBytesToRead);
        retries = 0;
        // If we get a StorageContainerException or an IOException due to
        // datanodes are not reachable, refresh to get the latest pipeline
        // info and retry.
        // Otherwise, just retry according to the retry policy.
      } catch (SCMSecurityException ex) {
        throw ex;
      } catch (StorageContainerException e) {
        if (shouldRetryRead(e)) {
          handleReadError(e);
          throw new RetriableIOException(e);
        } else {
          throw e;
        }
      } catch (IOException ex) {
        if (shouldRetryRead(ex)) {
          if (isConnectivityIssue(ex)) {
            handleReadError(ex);
          } else {
            ((ChunkInputStream) current).releaseClient();
          }
          throw new RetriableIOException(ex);
        } else {
          throw ex;
        }
      }
      return numBytesRead;
    }

    @Override
    protected void checkPartBytesRead(
        int numBytesToRead,
        int numBytesRead,
        PartInputStream stream) throws IOException {
      if (numBytesRead != numBytesToRead) {
        // This implies that there is either data loss or corruption in the
        // chunk entries. Even EOF in the current stream would be covered in
        // this case.
        throw new IOException(String.format(
            "Inconsistent read for chunkName=%s length=%d position=%d " +
                "numBytesToRead= %d numBytesRead=%d",
            ((ChunkInputStream) stream).getChunkName(), stream.getLength(),
            stream.getPos(), numBytesToRead, numBytesRead));
      }
    }

    @Override
    public synchronized long getPos() {
      try {
        return super.getPos();
      } catch (IOException e) {
        //Should never happen, ChunkInputStream does not throw IOException
        return 0;
      }
    }

    @Override
    protected void checkOpen() throws IOException {
      super.checkOpen();
      checkClientOpen();
    }

    public void releaseClient() {
      for (ChunkInputStream is : getPartStreams()) {
        is.releaseClient();
      }
    }

    @Override
    public List<ChunkInputStream> getPartStreams() {
      return (List<ChunkInputStream>) super.getPartStreams();
    }
  }
}
