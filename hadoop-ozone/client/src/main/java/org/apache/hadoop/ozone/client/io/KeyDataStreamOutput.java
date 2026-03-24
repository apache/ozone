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

package org.apache.hadoop.ozone.client.io;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.AbstractDataStreamOutput;
import org.apache.hadoop.hdds.scm.storage.BlockDataStreamOutput;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.ratis.util.function.CheckedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintaining a list of BlockInputStream. Write based on offset.
 *
 * Note that this may write to multiple containers in one write call. In case
 * that first container succeeded but later ones failed, the succeeded writes
 * are not rolled back.
 *
 * TODO : currently not support multi-thread access.
 */
public class KeyDataStreamOutput extends AbstractDataStreamOutput
    implements KeyMetadataAware {

  private static final Logger LOG =
      LoggerFactory.getLogger(KeyDataStreamOutput.class);

  private OzoneClientConfig config;
  private boolean closed;

  // how much of data is actually written yet to underlying stream
  private long offset;
  // how much data has been ingested into the stream
  private long writeOffset;

  private final BlockDataStreamOutputEntryPool blockDataStreamOutputEntryPool;

  private long clientID;

  /**
   * Indicates if an atomic write is required. When set to true,
   * the amount of data written must match the declared size during the commit.
   * A mismatch will prevent the commit from succeeding.
   * This is essential for operations like S3 put to ensure atomicity.
   */
  private boolean atomicKeyCreation;

  private List<CheckedRunnable<IOException>> preCommits = Collections.emptyList();

  public void setPreCommits(@Nonnull List<CheckedRunnable<IOException>> preCommits) {
    this.preCommits = preCommits;
  }

  @VisibleForTesting
  public List<BlockDataStreamOutputEntry> getStreamEntries() {
    return blockDataStreamOutputEntryPool.getStreamEntries();
  }

  @VisibleForTesting
  public XceiverClientFactory getXceiverClientFactory() {
    return blockDataStreamOutputEntryPool.getXceiverClientFactory();
  }

  @VisibleForTesting
  public List<OmKeyLocationInfo> getLocationInfoList() {
    return blockDataStreamOutputEntryPool.getLocationInfoList();
  }

  @VisibleForTesting
  public long getClientID() {
    return clientID;
  }

  @VisibleForTesting
  public KeyDataStreamOutput() {
    super(null);
    this.config = new OzoneClientConfig();
    OmKeyInfo info = new OmKeyInfo.Builder().setKeyName("test").build();
    blockDataStreamOutputEntryPool =
        new BlockDataStreamOutputEntryPool(
            config,
            null,
            null,
            null, 0,
            false, info,
            false,
            null,
            0L);

    this.writeOffset = 0;
    this.clientID = 0L;
    this.atomicKeyCreation = false;
  }

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  public KeyDataStreamOutput(
      OzoneClientConfig config,
      OpenKeySession handler,
      XceiverClientFactory xceiverClientManager,
      OzoneManagerProtocol omClient, int chunkSize,
      String requestId, ReplicationConfig replicationConfig,
      String uploadID, int partNumber, boolean isMultipart,
      boolean unsafeByteBufferConversion,
      boolean atomicKeyCreation
  ) {
    super(HddsClientUtils.getRetryPolicyByException(
        config.getMaxRetryCount(), config.getRetryInterval()));
    this.config = config;
    OmKeyInfo info = handler.getKeyInfo();
    blockDataStreamOutputEntryPool =
        new BlockDataStreamOutputEntryPool(
            config,
            omClient,
            replicationConfig,
            uploadID, partNumber,
            isMultipart, info,
            unsafeByteBufferConversion,
            xceiverClientManager,
            handler.getId());

    // Retrieve the file encryption key info, null if file is not in
    // encrypted bucket.
    this.writeOffset = 0;
    this.clientID = handler.getId();
    this.atomicKeyCreation = atomicKeyCreation;
  }

  /**
   * When a key is opened, it is possible that there are some blocks already
   * allocated to it for this open session. In this case, to make use of these
   * blocks, we need to add these blocks to stream entries. But, a key's version
   * also includes blocks from previous versions, we need to avoid adding these
   * old blocks to stream entries, because these old blocks should not be picked
   * for write. To do this, the following method checks that, only those
   * blocks created in this particular open version are added to stream entries.
   *
   * @param version the set of blocks that are pre-allocated.
   * @param openVersion the version corresponding to the pre-allocation.
   * @throws IOException
   */
  public void addPreallocateBlocks(OmKeyLocationInfoGroup version,
      long openVersion) throws IOException {
    blockDataStreamOutputEntryPool.addPreallocateBlocks(version, openVersion);
  }

  @Override
  public void write(ByteBuffer b, int off, int len) throws IOException {
    checkNotClosed();
    if (b == null) {
      throw new NullPointerException();
    }
    handleWrite(b, off, len, false);
    writeOffset += len;
  }

  private void handleWrite(ByteBuffer b, int off, long len, boolean retry)
      throws IOException {
    while (len > 0) {
      try {
        BlockDataStreamOutputEntry current =
            blockDataStreamOutputEntryPool.allocateBlockIfNeeded();
        // length(len) will be in int range if the call is happening through
        // write API of blockDataStreamOutput. Length can be in long range
        // if it comes via Exception path.
        int expectedWriteLen = Math.min((int) len,
                (int) current.getRemaining());
        long currentPos = current.getWrittenDataLength();
        // writeLen will be updated based on whether the write was succeeded
        // or if it sees an exception, how much the actual write was
        // acknowledged.
        int writtenLength =
            writeToDataStreamOutput(current, retry, len, b,
                expectedWriteLen, off, currentPos);
        if (current.getRemaining() <= 0) {
          // since the current block is already written close the stream.
          handleFlushOrClose(StreamAction.FULL);
        }
        len -= writtenLength;
        off += writtenLength;
      } catch (Exception e) {
        markStreamClosed();
        throw new IOException(e);
      }
    }
  }

  private int writeToDataStreamOutput(BlockDataStreamOutputEntry current,
      boolean retry, long len, ByteBuffer b, int writeLen, int off,
      long currentPos) throws IOException {
    try {
      if (retry) {
        current.writeOnRetry(len);
      } else {
        current.write(b, off, writeLen);
        offset += writeLen;
      }
    } catch (IOException ioe) {
      // for the current iteration, totalDataWritten - currentPos gives the
      // amount of data already written to the buffer

      // In the retryPath, the total data to be written will always be equal
      // to or less than the max length of the buffer allocated.
      // The len specified here is the combined sum of the data length of
      // the buffers
      Preconditions.checkState(!retry || len <= config
          .getStreamBufferMaxSize());
      int dataWritten = (int) (current.getWrittenDataLength() - currentPos);
      writeLen = retry ? (int) len : dataWritten;
      // In retry path, the data written is already accounted in offset.
      if (!retry) {
        offset += writeLen;
      }
      LOG.debug("writeLen {}, total len {}", writeLen, len);
      handleException(current, ioe);
    }
    return writeLen;
  }

  @Override
  public void hflush() throws IOException {
    hsync();
  }

  @Override
  public void hsync() throws IOException {
    checkNotClosed();
    final long hsyncPos = writeOffset;
    handleFlushOrClose(KeyDataStreamOutput.StreamAction.HSYNC);
    Preconditions.checkState(offset >= hsyncPos,
        "offset = %s < hsyncPos = %s", offset, hsyncPos);
    blockDataStreamOutputEntryPool.hsyncKey(hsyncPos);
  }

  /**
   * It performs following actions :
   * a. Updates the committed length at datanode for the current stream in
   * datanode.
   * b. Reads the data from the underlying buffer and writes it the next stream.
   *
   * @param streamEntry StreamEntry
   * @param exception   actual exception that occurred
   * @throws IOException Throws IOException if Write fails
   */
  private void handleException(BlockDataStreamOutputEntry streamEntry,
      IOException exception) throws IOException {
    Throwable t = HddsClientUtils.checkForException(exception);
    Objects.requireNonNull(t, "t == null");
    boolean retryFailure = checkForRetryFailure(t);
    boolean containerExclusionException = false;
    if (!retryFailure) {
      containerExclusionException = checkIfContainerToExclude(t);
    }
    Pipeline pipeline = streamEntry.getPipeline();
    PipelineID pipelineId = pipeline.getId();
    long totalSuccessfulFlushedData = streamEntry.getTotalAckDataLength();
    //set the correct length for the current stream
    streamEntry.setCurrentPosition(totalSuccessfulFlushedData);
    long containerId = streamEntry.getBlockID().getContainerID();
    Collection<DatanodeDetails> failedServers = streamEntry.getFailedServers();
    Objects.requireNonNull(failedServers, "failedServers == null");
    if (!containerExclusionException) {
      BlockDataStreamOutputEntry currentStreamEntry =
          blockDataStreamOutputEntryPool.getCurrentStreamEntry();
      if (currentStreamEntry != null) {
        try {
          BlockDataStreamOutput blockDataStreamOutput =
              (BlockDataStreamOutput) currentStreamEntry
                  .getByteBufStreamOutput();
          blockDataStreamOutput.executePutBlock(false, false);
          blockDataStreamOutput.watchForCommit(false);
        } catch (IOException e) {
          LOG.error(
              "Failed to execute putBlock/watchForCommit. " +
                  "Continuing to write chunks" + "in new block", e);
        }
      }
    }
    ExcludeList excludeList = blockDataStreamOutputEntryPool.getExcludeList();
    long bufferedDataLen = blockDataStreamOutputEntryPool.computeBufferData();
    if (!failedServers.isEmpty()) {
      excludeList.addDatanodes(failedServers);
    }

    // if the container needs to be excluded , add the container to the
    // exclusion list , otherwise add the pipeline to the exclusion list
    if (containerExclusionException) {
      excludeList.addConatinerId(ContainerID.valueOf(containerId));
    } else {
      excludeList.addPipeline(pipelineId);
    }
    // just clean up the current stream.
    streamEntry.cleanup(retryFailure);

    // discard all subsequent blocks the containers and pipelines which
    // are in the exclude list so that, the very next retry should never
    // write data on the  closed container/pipeline
    if (containerExclusionException) {
      // discard subsequent pre allocated blocks from the streamEntries list
      // from the closed container
      blockDataStreamOutputEntryPool
          .discardPreallocatedBlocks(streamEntry.getBlockID().getContainerID(),
              null);
    } else {
      // In case there is timeoutException or Watch for commit happening over
      // majority or the client connection failure to the leader in the
      // pipeline, just discard all the pre allocated blocks on this pipeline.
      // Next block allocation will happen with excluding this specific pipeline
      // This will ensure if 2 way commit happens , it cannot span over multiple
      // blocks
      blockDataStreamOutputEntryPool
          .discardPreallocatedBlocks(-1, pipelineId);
    }
    if (bufferedDataLen > 0) {
      // If the data is still cached in the underlying stream, we need to
      // allocate new block and write this data in the datanode.
      handleRetry(exception);
      handleWrite(null, 0, bufferedDataLen, true);
      // reset the retryCount after handling the exception
      resetRetryCount();
    }
  }

  private void markStreamClosed() {
    blockDataStreamOutputEntryPool.cleanup();
    closed = true;
  }

  @Override
  public void flush() throws IOException {
    checkNotClosed();
    handleFlushOrClose(StreamAction.FLUSH);
  }

  /**
   * Close or Flush the latest outputStream depending upon the action.
   * This function gets called when while write is going on, the current stream
   * gets full or explicit flush or close request is made by client. when the
   * stream gets full and we try to close the stream , we might end up hitting
   * an exception in the exception handling path, we write the data residing in
   * in the buffer pool to a new Block. In cases, as such, when the data gets
   * written to new stream , it will be at max half full. In such cases, we
   * should just write the data and not close the stream as the block won't be
   * completely full.
   *
   * @param op Flag which decides whether to call close or flush on the
   *           outputStream.
   * @throws IOException In case, flush or close fails with exception.
   */
  @SuppressWarnings("squid:S1141")
  private void handleFlushOrClose(StreamAction op) throws IOException {
    if (!blockDataStreamOutputEntryPool.isEmpty()) {
      while (true) {
        try {
          BlockDataStreamOutputEntry entry =
              blockDataStreamOutputEntryPool.getCurrentStreamEntry();
          if (entry != null) {
            try {
              handleStreamAction(entry, op);
            } catch (IOException ioe) {
              handleException(entry, ioe);
              continue;
            }
          }
          return;
        } catch (Exception e) {
          markStreamClosed();
          throw e;
        }
      }
    }
  }

  private void handleStreamAction(BlockDataStreamOutputEntry entry,
                                  StreamAction op) throws IOException {
    Collection<DatanodeDetails> failedServers = entry.getFailedServers();
    // failed servers can be null in case there is no data written in
    // the stream
    if (!failedServers.isEmpty()) {
      blockDataStreamOutputEntryPool.getExcludeList().addDatanodes(
          failedServers);
    }
    switch (op) {
    case CLOSE:
      entry.close();
      break;
    case FULL:
      if (entry.getRemaining() == 0) {
        entry.close();
      }
      break;
    case FLUSH:
      entry.flush();
      break;
    case HSYNC:
      entry.hsync();
      break;
    default:
      throw new IOException("Invalid Operation");
    }
  }

  /**
   * Commit the key to OM, this will add the blocks as the new key blocks.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    try {
      handleFlushOrClose(StreamAction.CLOSE);
      if (!isException()) {
        Preconditions.checkArgument(writeOffset == offset);
      }
      if (atomicKeyCreation) {
        long expectedSize = blockDataStreamOutputEntryPool.getDataSize();
        Preconditions.checkArgument(expectedSize == offset,
            String.format("Expected: %d and actual %d write sizes do not match",
                expectedSize, offset));
      }
      for (CheckedRunnable<IOException> preCommit : preCommits) {
        preCommit.run();
      }
      blockDataStreamOutputEntryPool.commitKey(offset);
    } finally {
      blockDataStreamOutputEntryPool.cleanup();
    }
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    return blockDataStreamOutputEntryPool.getCommitUploadPartInfo();
  }

  @VisibleForTesting
  public ExcludeList getExcludeList() {
    return blockDataStreamOutputEntryPool.getExcludeList();
  }

  @Override
  public Map<String, String> getMetadata() {
    return this.blockDataStreamOutputEntryPool.getMetadata();
  }

  /**
   * Builder class of KeyDataStreamOutput.
   */
  public static class Builder {
    private OpenKeySession openHandler;
    private XceiverClientFactory xceiverManager;
    private OzoneManagerProtocol omClient;
    private int chunkSize;
    private final String requestID = UUID.randomUUID().toString();
    private String multipartUploadID;
    private int multipartNumber;
    private boolean isMultipartKey;
    private boolean unsafeByteBufferConversion;
    private OzoneClientConfig clientConfig;
    private ReplicationConfig replicationConfig;
    private boolean atomicKeyCreation = false;

    public Builder setMultipartUploadID(String uploadID) {
      this.multipartUploadID = uploadID;
      return this;
    }

    public Builder setMultipartNumber(int partNumber) {
      this.multipartNumber = partNumber;
      return this;
    }

    public Builder setHandler(OpenKeySession handler) {
      this.openHandler = handler;
      return this;
    }

    public Builder setXceiverClientManager(XceiverClientFactory manager) {
      this.xceiverManager = manager;
      return this;
    }

    public Builder setOmClient(OzoneManagerProtocol client) {
      this.omClient = client;
      return this;
    }

    public Builder setChunkSize(int size) {
      this.chunkSize = size;
      return this;
    }

    public Builder setIsMultipartKey(boolean isMultipart) {
      this.isMultipartKey = isMultipart;
      return this;
    }

    public Builder setConfig(OzoneClientConfig config) {
      this.clientConfig = config;
      return this;
    }

    public Builder enableUnsafeByteBufferConversion(boolean enabled) {
      this.unsafeByteBufferConversion = enabled;
      return this;
    }

    public Builder setReplicationConfig(ReplicationConfig replConfig) {
      this.replicationConfig = replConfig;
      return this;
    }

    public Builder setAtomicKeyCreation(boolean atomicKey) {
      this.atomicKeyCreation = atomicKey;
      return this;
    }

    public KeyDataStreamOutput build() {
      return new KeyDataStreamOutput(
          clientConfig,
          openHandler,
          xceiverManager,
          omClient,
          chunkSize,
          requestID,
          replicationConfig,
          multipartUploadID,
          multipartNumber,
          isMultipartKey,
          unsafeByteBufferConversion,
          atomicKeyCreation);
    }

  }

  /**
   * Verify that the output stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(
          ": " + FSExceptionMessages.STREAM_IS_CLOSED + " Key: "
              + blockDataStreamOutputEntryPool.getKeyName());
    }
  }

  /**
   * Defines stream action while calling handleFlushOrClose.
   */
  enum StreamAction {
    FLUSH, HSYNC, CLOSE, FULL
  }
}
