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
package org.apache.hadoop.ozone.client.io;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;
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
public class KeyOutputStream extends OutputStream {

  private OzoneClientConfig config;

  /**
   * Defines stream action while calling handleFlushOrClose.
   */
  enum StreamAction {
    FLUSH, CLOSE, FULL
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(KeyOutputStream.class);

  private boolean closed;
  private final Map<Class<? extends Throwable>, RetryPolicy> retryPolicyMap;
  private int retryCount;
  // how much of data is actually written yet to underlying stream
  private long offset;
  // how much data has been ingested into the stream
  private long writeOffset;
  // whether an exception is encountered while write and whole write could
  // not succeed
  private boolean isException;
  private final BlockOutputStreamEntryPool blockOutputStreamEntryPool;

  private long clientID;

  public KeyOutputStream(ContainerClientMetrics clientMetrics) {
    closed = false;
    this.retryPolicyMap = HddsClientUtils.getExceptionList()
        .stream()
        .collect(Collectors.toMap(Function.identity(),
            e -> RetryPolicies.TRY_ONCE_THEN_FAIL));
    retryCount = 0;
    offset = 0;
    blockOutputStreamEntryPool = new BlockOutputStreamEntryPool(clientMetrics);
  }

  @VisibleForTesting
  public List<BlockOutputStreamEntry> getStreamEntries() {
    return blockOutputStreamEntryPool.getStreamEntries();
  }

  @VisibleForTesting
  public XceiverClientFactory getXceiverClientFactory() {
    return blockOutputStreamEntryPool.getXceiverClientFactory();
  }

  @VisibleForTesting
  public List<OmKeyLocationInfo> getLocationInfoList() {
    return blockOutputStreamEntryPool.getLocationInfoList();
  }

  @VisibleForTesting
  public int getRetryCount() {
    return retryCount;
  }

  @VisibleForTesting
  public long getClientID() {
    return clientID;
  }

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  public KeyOutputStream(
      OzoneClientConfig config,
      OpenKeySession handler,
      XceiverClientFactory xceiverClientManager,
      OzoneManagerProtocol omClient, int chunkSize,
      String requestId, ReplicationConfig replicationConfig,
      String uploadID, int partNumber, boolean isMultipart,
      boolean unsafeByteBufferConversion,
      ContainerClientMetrics clientMetrics
  ) {
    this.config = config;
    blockOutputStreamEntryPool =
        new BlockOutputStreamEntryPool(
            config,
            omClient,
            requestId, replicationConfig,
            uploadID, partNumber,
            isMultipart, handler.getKeyInfo(),
            unsafeByteBufferConversion,
            xceiverClientManager,
            handler.getId(),
            clientMetrics);
    this.retryPolicyMap = HddsClientUtils.getRetryPolicyByException(
        config.getMaxRetryCount(), config.getRetryInterval());
    this.retryCount = 0;
    this.isException = false;
    this.writeOffset = 0;
    this.clientID = handler.getId();
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
    blockOutputStreamEntryPool.addPreallocateBlocks(version, openVersion);
  }

  @Override
  public void write(int b) throws IOException {
    byte[] buf = new byte[1];
    buf[0] = (byte) b;
    write(buf, 0, 1);
  }

  /**
   * Try to write the bytes sequence b[off:off+len) to streams.
   *
   * NOTE: Throws exception if the data could not fit into the remaining space.
   * In which case nothing will be written.
   * TODO:May need to revisit this behaviour.
   *
   * @param b byte data
   * @param off starting offset
   * @param len length to write
   * @throws IOException
   */
  @Override
  public void write(byte[] b, int off, int len)
      throws IOException {
    checkNotClosed();
    if (b == null) {
      throw new NullPointerException();
    }
    if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return;
    }
    handleWrite(b, off, len, false);
    writeOffset += len;
  }

  private void handleWrite(byte[] b, int off, long len, boolean retry)
      throws IOException {
    while (len > 0) {
      try {
        BlockOutputStreamEntry current =
            blockOutputStreamEntryPool.allocateBlockIfNeeded();
        // length(len) will be in int range if the call is happening through
        // write API of blockOutputStream. Length can be in long range if it
        // comes via Exception path.
        int expectedWriteLen = Math.min((int) len,
                (int) current.getRemaining());
        long currentPos = current.getWrittenDataLength();
        // writeLen will be updated based on whether the write was succeeded
        // or if it sees an exception, how much the actual write was
        // acknowledged.
        int writtenLength =
                writeToOutputStream(current, retry, len, b, expectedWriteLen,
                off, currentPos);
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

  private int writeToOutputStream(BlockOutputStreamEntry current,
      boolean retry, long len, byte[] b, int writeLen, int off, long currentPos)
      throws IOException {
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
  private void handleException(BlockOutputStreamEntry streamEntry,
      IOException exception) throws IOException {
    Throwable t = HddsClientUtils.checkForException(exception);
    Preconditions.checkNotNull(t);
    boolean retryFailure = checkForRetryFailure(t);
    boolean containerExclusionException = false;
    if (!retryFailure) {
      containerExclusionException = checkIfContainerToExclude(t);
    }
    Pipeline pipeline = streamEntry.getPipeline();
    PipelineID pipelineId = pipeline.getId();
    long totalSuccessfulFlushedData = streamEntry.getTotalAckDataLength();
    streamEntry.resetToAckedPosition();
    long bufferedDataLen = blockOutputStreamEntryPool.computeBufferData();
    if (containerExclusionException) {
      LOG.debug(
          "Encountered exception {}. The last committed block length is {}, "
              + "uncommitted data length is {} retry count {}", exception,
          totalSuccessfulFlushedData, bufferedDataLen, retryCount);
    } else {
      LOG.warn(
          "Encountered exception {} on the pipeline {}. "
              + "The last committed block length is {}, "
              + "uncommitted data length is {} retry count {}", exception,
          pipeline, totalSuccessfulFlushedData, bufferedDataLen, retryCount);
    }
    Preconditions.checkArgument(
        bufferedDataLen <= config.getStreamBufferMaxSize());
    Preconditions.checkArgument(
        offset - blockOutputStreamEntryPool.getKeyLength() == bufferedDataLen);
    long containerId = streamEntry.getBlockID().getContainerID();
    Collection<DatanodeDetails> failedServers = streamEntry.getFailedServers();
    Preconditions.checkNotNull(failedServers);
    ExcludeList excludeList = blockOutputStreamEntryPool.getExcludeList();
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
      blockOutputStreamEntryPool
          .discardPreallocatedBlocks(streamEntry.getBlockID().getContainerID(),
              null);
    } else {
      // In case there is timeoutException or Watch for commit happening over
      // majority or the client connection failure to the leader in the
      // pipeline, just discard all the pre allocated blocks on this pipeline.
      // Next block allocation will happen with excluding this specific pipeline
      // This will ensure if 2 way commit happens , it cannot span over multiple
      // blocks
      blockOutputStreamEntryPool
          .discardPreallocatedBlocks(-1, pipelineId);
    }
    if (bufferedDataLen > 0) {
      // If the data is still cached in the underlying stream, we need to
      // allocate new block and write this data in the datanode.
      handleRetry(exception, bufferedDataLen);
      // reset the retryCount after handling the exception
      retryCount = 0;
    }
  }

  private void markStreamClosed() {
    blockOutputStreamEntryPool.cleanup();
    closed = true;
  }

  private void handleRetry(IOException exception, long len) throws IOException {
    RetryPolicy retryPolicy = retryPolicyMap
        .get(HddsClientUtils.checkForException(exception).getClass());
    if (retryPolicy == null) {
      retryPolicy = retryPolicyMap.get(Exception.class);
    }
    RetryPolicy.RetryAction action = null;
    try {
      action = retryPolicy.shouldRetry(exception, retryCount, 0, true);
    } catch (Exception e) {
      setExceptionAndThrow(new IOException(e));
    }
    if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
      String msg = "";
      if (action.reason != null) {
        msg = "Retry request failed. " + action.reason;
        LOG.error(msg, exception);
      }
      setExceptionAndThrow(new IOException(msg, exception));
    }

    // Throw the exception if the thread is interrupted
    if (Thread.currentThread().isInterrupted()) {
      LOG.warn("Interrupted while trying for retry");
      setExceptionAndThrow(exception);
    }
    Preconditions.checkArgument(
        action.action == RetryPolicy.RetryAction.RetryDecision.RETRY);
    if (action.delayMillis > 0) {
      try {
        Thread.sleep(action.delayMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        IOException ioe =  (IOException) new InterruptedIOException(
            "Interrupted: action=" + action + ", retry policy=" + retryPolicy)
            .initCause(e);
        setExceptionAndThrow(ioe);
      }
    }
    retryCount++;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Retrying Write request. Already tried {} time(s); " +
          "retry policy is {} ", retryCount, retryPolicy);
    }
    handleWrite(null, 0, len, true);
  }

  private void setExceptionAndThrow(IOException ioe) throws IOException {
    isException = true;
    throw ioe;
  }

  /**
   * Checks if the provided exception signifies retry failure in ratis client.
   * In case of retry failure, ratis client throws RaftRetryFailureException
   * and all succeeding operations are failed with AlreadyClosedException.
   */
  private boolean checkForRetryFailure(Throwable t) {
    return t instanceof RaftRetryFailureException
        || t instanceof AlreadyClosedException;
  }

  // Every container specific exception from datatnode will be seen as
  // StorageContainerException
  protected boolean checkIfContainerToExclude(Throwable t) {
    return t instanceof StorageContainerException;
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
    if (!blockOutputStreamEntryPool.isEmpty()) {
      while (true) {
        try {
          BlockOutputStreamEntry entry =
              blockOutputStreamEntryPool.getCurrentStreamEntry();
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

  private void handleStreamAction(BlockOutputStreamEntry entry,
                                  StreamAction op) throws IOException {
    Collection<DatanodeDetails> failedServers = entry.getFailedServers();
    // failed servers can be null in case there is no data written in
    // the stream
    if (!failedServers.isEmpty()) {
      blockOutputStreamEntryPool.getExcludeList().addDatanodes(
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
      if (!isException) {
        Preconditions.checkArgument(writeOffset == offset);
      }
      blockOutputStreamEntryPool.commitKey(offset);
    } finally {
      blockOutputStreamEntryPool.cleanup();
    }
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    return blockOutputStreamEntryPool.getCommitUploadPartInfo();
  }

  @VisibleForTesting
  public ExcludeList getExcludeList() {
    return blockOutputStreamEntryPool.getExcludeList();
  }

  /**
   * Builder class of KeyOutputStream.
   */
  public static class Builder {
    private OpenKeySession openHandler;
    private XceiverClientFactory xceiverManager;
    private OzoneManagerProtocol omClient;
    private int chunkSize;
    private String requestID;
    private String multipartUploadID;
    private int multipartNumber;
    private boolean isMultipartKey;
    private boolean unsafeByteBufferConversion;
    private OzoneClientConfig clientConfig;
    private ReplicationConfig replicationConfig;
    private ContainerClientMetrics clientMetrics;

    public String getMultipartUploadID() {
      return multipartUploadID;
    }

    public Builder setMultipartUploadID(String uploadID) {
      this.multipartUploadID = uploadID;
      return this;
    }

    public int getMultipartNumber() {
      return multipartNumber;
    }

    public Builder setMultipartNumber(int partNumber) {
      this.multipartNumber = partNumber;
      return this;
    }

    public OpenKeySession getOpenHandler() {
      return openHandler;
    }

    public Builder setHandler(OpenKeySession handler) {
      this.openHandler = handler;
      return this;
    }

    public XceiverClientFactory getXceiverManager() {
      return xceiverManager;
    }

    public Builder setXceiverClientManager(XceiverClientFactory manager) {
      this.xceiverManager = manager;
      return this;
    }

    public OzoneManagerProtocol getOmClient() {
      return omClient;
    }

    public Builder setOmClient(OzoneManagerProtocol client) {
      this.omClient = client;
      return this;
    }

    public int getChunkSize() {
      return chunkSize;
    }

    public Builder setChunkSize(int size) {
      this.chunkSize = size;
      return this;
    }

    public String getRequestID() {
      return requestID;
    }

    public Builder setRequestID(String id) {
      this.requestID = id;
      return this;
    }

    public boolean isMultipartKey() {
      return isMultipartKey;
    }

    public Builder setIsMultipartKey(boolean isMultipart) {
      this.isMultipartKey = isMultipart;
      return this;
    }

    public OzoneClientConfig getClientConfig() {
      return clientConfig;
    }

    public Builder setConfig(OzoneClientConfig config) {
      this.clientConfig = config;
      return this;
    }

    public boolean isUnsafeByteBufferConversionEnabled() {
      return unsafeByteBufferConversion;
    }

    public Builder enableUnsafeByteBufferConversion(boolean enabled) {
      this.unsafeByteBufferConversion = enabled;
      return this;
    }

    public ReplicationConfig getReplicationConfig() {
      return replicationConfig;
    }

    public Builder setReplicationConfig(ReplicationConfig replConfig) {
      this.replicationConfig = replConfig;
      return this;
    }

    public Builder setClientMetrics(ContainerClientMetrics clientMetrics) {
      this.clientMetrics = clientMetrics;
      return this;
    }

    public ContainerClientMetrics getClientMetrics() {
      return clientMetrics;
    }

    public KeyOutputStream build() {
      return new KeyOutputStream(
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
          clientMetrics);
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
              + blockOutputStreamEntryPool.getKeyName());
    }
  }
}
