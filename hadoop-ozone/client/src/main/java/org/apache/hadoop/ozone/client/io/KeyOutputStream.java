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
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.util.MetricUtil;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;
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
public class KeyOutputStream extends OutputStream
    implements Syncable, KeyMetadataAware {

  private static final Logger LOG =
      LoggerFactory.getLogger(KeyOutputStream.class);

  private final ReplicationConfig replication;

  private boolean closed;
  private final Map<Class<? extends Throwable>, RetryPolicy> retryPolicyMap;
  private int retryCount;
  // how much of data is actually written yet to underlying stream
  private long offset;
  // how much data has been ingested into the stream
  private volatile long writeOffset;
  // whether an exception is encountered while write and whole write could
  // not succeed
  private boolean isException;
  private final BlockOutputStreamEntryPool blockOutputStreamEntryPool;

  private long clientID;
  private StreamBufferArgs streamBufferArgs;

  /**
   * Indicates if an atomic write is required. When set to true,
   * the amount of data written must match the declared size during the commit.
   * A mismatch will prevent the commit from succeeding.
   * This is essential for operations like S3 put to ensure atomicity.
   */
  private boolean atomicKeyCreation;
  private ContainerClientMetrics clientMetrics;
  private OzoneManagerVersion ozoneManagerVersion;
  private final Lock writeLock = new ReentrantLock();
  private final Condition retryHandlingCondition = writeLock.newCondition();

  private final int maxConcurrentWritePerKey;
  private final KeyOutputStreamSemaphore keyOutputStreamSemaphore;

  @VisibleForTesting
  KeyOutputStreamSemaphore getRequestSemaphore() {
    return keyOutputStreamSemaphore;
  }

  /** Required to spy the object in testing. */
  @VisibleForTesting
  @SuppressWarnings("unused")
  KeyOutputStream() {
    maxConcurrentWritePerKey = 0;
    keyOutputStreamSemaphore = null;
    blockOutputStreamEntryPool = null;
    retryPolicyMap = null;
    replication = null;
  }

  public KeyOutputStream(ReplicationConfig replicationConfig, BlockOutputStreamEntryPool blockOutputStreamEntryPool) {
    this.replication = replicationConfig;
    closed = false;
    this.retryPolicyMap = HddsClientUtils.getExceptionList()
        .stream()
        .collect(Collectors.toMap(Function.identity(),
            e -> RetryPolicies.TRY_ONCE_THEN_FAIL));
    retryCount = 0;
    offset = 0;
    this.blockOutputStreamEntryPool = blockOutputStreamEntryPool;
    // Force write concurrency to 1 per key when using this constructor.
    // At the moment, this constructor is only used by ECKeyOutputStream.
    this.maxConcurrentWritePerKey = 1;
    this.keyOutputStreamSemaphore = new KeyOutputStreamSemaphore(maxConcurrentWritePerKey);
  }

  protected BlockOutputStreamEntryPool getBlockOutputStreamEntryPool() {
    return blockOutputStreamEntryPool;
  }

  @VisibleForTesting
  public List<BlockOutputStreamEntry> getStreamEntries() {
    return blockOutputStreamEntryPool.getStreamEntries();
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

  public KeyOutputStream(Builder b) {
    this.replication = b.replicationConfig;
    this.blockOutputStreamEntryPool = new BlockOutputStreamEntryPool(b);
    final OzoneClientConfig config = b.getClientConfig();
    this.maxConcurrentWritePerKey = config.getMaxConcurrentWritePerKey();
    this.keyOutputStreamSemaphore = new KeyOutputStreamSemaphore(maxConcurrentWritePerKey);
    this.retryPolicyMap = HddsClientUtils.getRetryPolicyByException(
        config.getMaxRetryCount(), config.getRetryInterval());
    this.retryCount = 0;
    this.isException = false;
    this.writeOffset = 0;
    this.clientID = b.getOpenHandler().getId();
    this.atomicKeyCreation = b.getAtomicKeyCreation();
    this.streamBufferArgs = b.getStreamBufferArgs();
    this.clientMetrics = b.getClientMetrics();
    this.ozoneManagerVersion = b.ozoneManagerVersion;
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
   */
  public void addPreallocateBlocks(OmKeyLocationInfoGroup version, long openVersion) {
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
    try {
      getRequestSemaphore().acquire();
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

      doInWriteLock(() -> {
        handleWrite(b, off, len, false);
        writeOffset += len;
      });
    } finally {
      getRequestSemaphore().release();
    }
  }

  private <E extends Throwable> void doInWriteLock(CheckedRunnable<E> block) throws E {
    writeLock.lock();
    try {
      block.run();
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  void handleWrite(byte[] b, int off, long len, boolean retry)
      throws IOException {
    while (len > 0) {
      try {
        BlockOutputStreamEntry current =
            blockOutputStreamEntryPool.allocateBlockIfNeeded(retry);
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
      current.registerCallReceived();
      if (retry) {
        current.writeOnRetry(len);
      } else {
        current.waitForRetryHandling(retryHandlingCondition);
        current.write(b, off, writeLen);
        offset += writeLen;
      }
      current.registerCallFinished();
    } catch (InterruptedException e) {
      current.registerCallFinished();
      throw new InterruptedIOException();
    } catch (IOException ioe) {
      // for the current iteration, totalDataWritten - currentPos gives the
      // amount of data already written to the buffer

      // In the retryPath, the total data to be written will always be equal
      // to or less than the max length of the buffer allocated.
      // The len specified here is the combined sum of the data length of
      // the buffers
      Preconditions.checkState(!retry || len <= streamBufferArgs
          .getStreamBufferMaxSize());
      int dataWritten = (int) (current.getWrittenDataLength() - currentPos);
      writeLen = retry ? (int) len : dataWritten;
      // In retry path, the data written is already accounted in offset.
      if (!retry) {
        offset += writeLen;
      }
      LOG.debug("writeLen {}, total len {}", writeLen, len);
      handleException(current, ioe, retry);
    }
    return writeLen;
  }

  private void handleException(BlockOutputStreamEntry entry, IOException exception, boolean fromRetry)
      throws IOException {
    doInWriteLock(() -> {
      handleExceptionInternal(entry, exception);
      BlockOutputStreamEntry current = blockOutputStreamEntryPool.getCurrentStreamEntry();
      if (!fromRetry && entry.registerCallFinished()) {
        // When the faulty block finishes handling all its pending call, the current block can exit retry
        // handling mode and unblock normal calls.
        current.finishRetryHandling(retryHandlingCondition);
      }
    });
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
  private void handleExceptionInternal(BlockOutputStreamEntry streamEntry, IOException exception) throws IOException {
    try {
      // Wait for all pending flushes in the faulty stream. It's possible that a prior write is pending completion
      // successfully. Errors are ignored here and will be handled by the individual flush call. We just want to ensure
      // all the pending are complete before handling exception.
      streamEntry.waitForAllPendingFlushes();
    } catch (IOException ignored) {
    }

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
        bufferedDataLen <= streamBufferArgs.getStreamBufferMaxSize());
    long containerId = streamEntry.getBlockID().getContainerID();
    Collection<DatanodeDetails> failedServers = streamEntry.getFailedServers();
    Objects.requireNonNull(failedServers, "failedServers == null");
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

  private synchronized void markStreamClosed() {
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
    try {
      getRequestSemaphore().acquire();
      checkNotClosed();
      handleFlushOrClose(StreamAction.FLUSH);
    } finally {
      getRequestSemaphore().release();
    }
  }

  @Override
  public void hflush() throws IOException {
    // Note: Semaphore acquired and released inside hsync().
    hsync();
  }

  @Override
  public void hsync() throws IOException {
    try {
      getRequestSemaphore().acquire();

      if (replication.getReplicationType() != ReplicationType.RATIS) {
        throw new UnsupportedOperationException(
            "Replication type is not " + ReplicationType.RATIS);
      }
      if (replication.getRequiredNodes() <= 1) {
        throw new UnsupportedOperationException("The replication factor = "
            + replication.getRequiredNodes() + " <= 1");
      }
      if (ozoneManagerVersion.compareTo(OzoneManagerVersion.HBASE_SUPPORT) < 0) {
        throw new UnsupportedOperationException("Hsync API requires OM version "
            + OzoneManagerVersion.HBASE_SUPPORT + " or later. Current OM version "
            + ozoneManagerVersion);
      }
      checkNotClosed();
      final long hsyncPos = writeOffset;
      handleFlushOrClose(StreamAction.HSYNC);

      doInWriteLock(() -> {
        Preconditions.checkState(offset >= hsyncPos,
            "offset = %s < hsyncPos = %s", offset, hsyncPos);
        MetricUtil.captureLatencyNs(clientMetrics::addHsyncLatency,
            () -> blockOutputStreamEntryPool.hsyncKey(hsyncPos));
      });
    } finally {
      getRequestSemaphore().release();
    }
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
            // If the current block is to handle retries, wait until all the retries are done.
            doInWriteLock(() -> entry.waitForRetryHandling(retryHandlingCondition));
            entry.registerCallReceived();
            try {
              handleStreamAction(entry, op);
              entry.registerCallFinished();
            } catch (IOException ioe) {
              handleException(entry, ioe, false);
              continue;
            } catch (Exception e) {
              entry.registerCallFinished();
              throw e;
            }
          }
          return;
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
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
    doInWriteLock(this::closeInternal);
  }

  private void closeInternal() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    try {
      handleFlushOrClose(StreamAction.CLOSE);
      if (!isException) {
        Preconditions.checkArgument(writeOffset == offset);
      }
      if (atomicKeyCreation) {
        long expectedSize = blockOutputStreamEntryPool.getDataSize();
        Preconditions.checkState(expectedSize == offset,
            String.format("Expected: %d and actual %d write sizes do not match",
                expectedSize, offset));
      }
      blockOutputStreamEntryPool.commitKey(offset);
    } finally {
      blockOutputStreamEntryPool.cleanup();
    }
  }

  synchronized OmMultipartCommitUploadPartInfo
      getCommitUploadPartInfo() {
    return blockOutputStreamEntryPool.getCommitUploadPartInfo();
  }

  @VisibleForTesting
  public ExcludeList getExcludeList() {
    return blockOutputStreamEntryPool.getExcludeList();
  }

  @Override
  public Map<String, String> getMetadata() {
    return this.blockOutputStreamEntryPool.getMetadata();
  }

  /**
   * Builder class of KeyOutputStream.
   */
  public static class Builder {
    private OpenKeySession openHandler;
    private XceiverClientFactory xceiverManager;
    private OzoneManagerProtocol omClient;
    private final String requestID = UUID.randomUUID().toString();
    private String multipartUploadID;
    private int multipartNumber;
    private boolean isMultipartKey;
    private boolean unsafeByteBufferConversion;
    private OzoneClientConfig clientConfig;
    private ReplicationConfig replicationConfig;
    private ContainerClientMetrics clientMetrics;
    private boolean atomicKeyCreation = false;
    private StreamBufferArgs streamBufferArgs;
    private Supplier<ExecutorService> executorServiceSupplier;
    private OzoneManagerVersion ozoneManagerVersion;

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

    public String getRequestID() {
      return requestID;
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

    public StreamBufferArgs getStreamBufferArgs() {
      return streamBufferArgs;
    }

    public Builder setStreamBufferArgs(StreamBufferArgs streamBufferArgs) {
      this.streamBufferArgs = streamBufferArgs;
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

    public Builder setAtomicKeyCreation(boolean atomicKey) {
      this.atomicKeyCreation = atomicKey;
      return this;
    }

    public Builder setClientMetrics(ContainerClientMetrics clientMetrics) {
      this.clientMetrics = clientMetrics;
      return this;
    }

    public ContainerClientMetrics getClientMetrics() {
      return clientMetrics;
    }

    public boolean getAtomicKeyCreation() {
      return atomicKeyCreation;
    }

    public Builder setExecutorServiceSupplier(Supplier<ExecutorService> executorServiceSupplier) {
      this.executorServiceSupplier = executorServiceSupplier;
      return this;
    }

    public Supplier<ExecutorService> getExecutorServiceSupplier() {
      return executorServiceSupplier;
    }

    public Builder setOmVersion(OzoneManagerVersion omVersion) {
      this.ozoneManagerVersion = omVersion;
      return this;
    }

    public OzoneManagerVersion getOmVersion() {
      return ozoneManagerVersion;
    }

    public KeyOutputStream build() {
      return new KeyOutputStream(this);
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

  /**
   * Defines stream action while calling handleFlushOrClose.
   */
  enum StreamAction {
    FLUSH, HSYNC, CLOSE, FULL
  }
}
