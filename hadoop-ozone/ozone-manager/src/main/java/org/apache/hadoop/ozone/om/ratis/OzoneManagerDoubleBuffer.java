/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.ratis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.function.SupplierWithIOException;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.ratis.helpers.DoubleBufferEntry;
import org.apache.hadoop.ozone.om.ratis.metrics.OzoneManagerDoubleBufferMetrics;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

/**
 * This class implements DoubleBuffer implementation of OMClientResponse's. In
 * DoubleBuffer it has 2 buffers one is currentBuffer and other is
 * readyBuffer. The current OM requests will always be added to currentBuffer.
 * Flush thread will be running in background, it checks if currentBuffer has
 * any entries, it swaps the buffer and creates a batch and commit to DB.
 * Adding OM request to doubleBuffer and swap of buffer are synchronized
 * methods.
 *
 */
public final class OzoneManagerDoubleBuffer {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerDoubleBuffer.class);

  // Taken unbounded queue, if sync thread is taking too long time, we
  // might end up taking huge memory to add entries to the buffer.
  // TODO: We can avoid this using unbounded queue and use queue with
  // capacity, if queue is full we can wait for sync to be completed to
  // add entries. But in this also we might block rpc handlers, as we
  // clear entries after sync. Or we can come up with a good approach to
  // solve this.
  private Queue<DoubleBufferEntry<OMClientResponse>> currentBuffer;
  private Queue<DoubleBufferEntry<OMClientResponse>> readyBuffer;


  // future objects which hold the future returned by add method.
  private volatile Queue<CompletableFuture<Void>> currentFutureQueue;

  // Once we have an entry in current buffer, we swap the currentFutureQueue
  // with readyFutureQueue. After flush is completed in flushTransaction
  // daemon thread, we complete the futures in readyFutureQueue and clear them.
  private volatile Queue<CompletableFuture<Void>> readyFutureQueue;

  private Daemon daemon;
  private final OMMetadataManager omMetadataManager;
  private final AtomicLong flushedTransactionCount = new AtomicLong(0);
  private final AtomicLong flushIterations = new AtomicLong(0);
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private OzoneManagerDoubleBufferMetrics ozoneManagerDoubleBufferMetrics;
  private long maxFlushedTransactionsInOneIteration;

  private final OzoneManagerRatisSnapshot ozoneManagerRatisSnapShot;

  private final boolean isRatisEnabled;
  private final boolean isTracingEnabled;
  private final Semaphore unFlushedTransactions;

  /**
   * function which will get term associated with the transaction index.
   */
  private Function<Long, Long> indexToTerm;

  /**
   *  Builder for creating OzoneManagerDoubleBuffer.
   */
  public static class Builder {
    private OMMetadataManager mm;
    private OzoneManagerRatisSnapshot rs;
    private boolean isRatisEnabled = false;
    private boolean isTracingEnabled = false;
    private Function<Long, Long> indexToTerm = null;
    private int maxUnFlushedTransactionCount = 0;

    public Builder setOmMetadataManager(OMMetadataManager omm) {
      this.mm = omm;
      return this;
    }

    public Builder setOzoneManagerRatisSnapShot(
        OzoneManagerRatisSnapshot omrs) {
      this.rs = omrs;
      return this;
    }

    public Builder enableRatis(boolean enableRatis) {
      this.isRatisEnabled = enableRatis;
      return this;
    }

    public Builder enableTracing(boolean enableTracing) {
      this.isTracingEnabled = enableTracing;
      return this;
    }

    public Builder setIndexToTerm(Function<Long, Long> termGet) {
      this.indexToTerm = termGet;
      return this;
    }

    public Builder setmaxUnFlushedTransactionCount(int size) {
      this.maxUnFlushedTransactionCount = size;
      return this;
    }

    public OzoneManagerDoubleBuffer build() {
      if (isRatisEnabled) {
        Preconditions.checkNotNull(rs, "When ratis is enabled, " +
                "OzoneManagerRatisSnapshot should not be null");
        Preconditions.checkNotNull(indexToTerm, "When ratis is enabled " +
            "indexToTerm should not be null");
        Preconditions.checkState(maxUnFlushedTransactionCount > 0L,
            "when ratis is enable, maxUnFlushedTransactions " +
                "should be bigger than 0");
      }
      return new OzoneManagerDoubleBuffer(mm, rs, isRatisEnabled,
          isTracingEnabled, indexToTerm, maxUnFlushedTransactionCount);
    }
  }

  private OzoneManagerDoubleBuffer(OMMetadataManager omMetadataManager,
      OzoneManagerRatisSnapshot ozoneManagerRatisSnapShot,
      boolean isRatisEnabled, boolean isTracingEnabled,
      Function<Long, Long> indexToTerm, int maxUnFlushedTransactions) {
    this.currentBuffer = new ConcurrentLinkedQueue<>();
    this.readyBuffer = new ConcurrentLinkedQueue<>();
    this.isRatisEnabled = isRatisEnabled;
    this.isTracingEnabled = isTracingEnabled;
    if (!isRatisEnabled) {
      this.currentFutureQueue = new ConcurrentLinkedQueue<>();
      this.readyFutureQueue = new ConcurrentLinkedQueue<>();
    }
    this.unFlushedTransactions = new Semaphore(maxUnFlushedTransactions);
    this.omMetadataManager = omMetadataManager;
    this.ozoneManagerRatisSnapShot = ozoneManagerRatisSnapShot;
    this.ozoneManagerDoubleBufferMetrics =
        OzoneManagerDoubleBufferMetrics.create();
    this.indexToTerm = indexToTerm;

    isRunning.set(true);
    // Daemon thread which runs in background and flushes transactions to DB.
    daemon = new Daemon(this::flushTransactions);
    daemon.setName("OMDoubleBufferFlushThread");
    daemon.start();
  }

  /**
   * Acquires the given number of permits from unFlushedTransactions,
   * blocking until all are available, or the thread is interrupted.
   */
  public void acquireUnFlushedTransactions(int n) throws InterruptedException {
    unFlushedTransactions.acquire(n);
  }

  /**
   * Releases the given number of permits,
   * returning them to the unFlushedTransactions.
   */
  public void releaseUnFlushedTransactions(int n) {
    unFlushedTransactions.release(n);
  }

  // TODO: pass the trace id further down and trace all methods of DBStore.

  /**
   * Add to write batch with trace span if tracing is enabled.
   */
  private Void addToBatchWithTrace(OMResponse omResponse,
      SupplierWithIOException<Void> supplier) throws IOException {
    if (!isTracingEnabled) {
      return supplier.get();
    }
    String spanName = "DB-addToWriteBatch" + "-" +
        omResponse.getCmdType().toString();
    return TracingUtil.executeAsChildSpan(spanName, omResponse.getTraceID(),
        supplier);
  }

  /**
   * Flush write batch with trace span if tracing is enabled.
   */
  private Void flushBatchWithTrace(String parentName, int batchSize,
      SupplierWithIOException<Void> supplier) throws IOException {
    if (!isTracingEnabled) {
      return supplier.get();
    }
    String spanName = "DB-commitWriteBatch-Size-" + batchSize;
    return TracingUtil.executeAsChildSpan(spanName, parentName, supplier);
  }

  /**
   * Add to writeBatch {@link TransactionInfo}.
   */
  private Void addToBatchTransactionInfoWithTrace(String parentName,
      long transactionIndex, SupplierWithIOException<Void> supplier)
      throws IOException {
    if (!isTracingEnabled) {
      return supplier.get();
    }
    String spanName = "DB-addWriteBatch-transactioninfo-" + transactionIndex;
    return TracingUtil.executeAsChildSpan(spanName, parentName, supplier);
  }

  /**
   * Runs in a background thread and batches the transaction in currentBuffer
   * and commit to DB.
   */
  private void flushTransactions() {
    while (isRunning.get() && canFlush()) {
      flushCurrentBuffer();
    }
  }

  /**
   * This is to extract out the flushing logic to make it testable.
   * If we don't do that, there could be a race condition which could fail
   * the unit test on different machines.
   */
  @VisibleForTesting
  void flushCurrentBuffer() {
    try {
      swapCurrentAndReadyBuffer();

      // For snapshot, we want to include all the keys that were committed
      // before the snapshot `create` command was executed. To achieve
      // the behaviour, we spilt the request buffer at snapshot create
      // request and flush the buffer in batches split at snapshot create
      // request.
      // For example, if requestBuffer is [request1, request2,
      // snapshotCreateRequest1, request3, snapshotCreateRequest2, request4].
      //
      // Split requestBuffer would be.
      // bufferQueues = [[request1, request2], [snapshotRequest1], [request3],
      //     [snapshotRequest2], [request4]].
      // And bufferQueues will be flushed in following order:
      // Flush #1: [request1, request2]
      // Flush #2: [snapshotRequest1]
      // Flush #3: [request3]
      // Flush #4: [snapshotRequest2]
      // Flush #5: [request4]
      List<Queue<DoubleBufferEntry<OMClientResponse>>> bufferQueues =
          splitReadyBufferAtCreateSnapshot();

      for (Queue<DoubleBufferEntry<OMClientResponse>> buffer : bufferQueues) {
        flushBatch(buffer);
      }

      clearReadyBuffer();
    } catch (IOException ex) {
      terminate(ex, 1);
    } catch (Throwable t) {
      terminate(t, 2);
    }
  }

  private void flushBatch(Queue<DoubleBufferEntry<OMClientResponse>> buffer)
      throws IOException {

    Map<String, List<Long>> cleanupEpochs = new HashMap<>();
    List<Long> flushedEpochs;

    try (BatchOperation batchOperation = omMetadataManager.getStore()
        .initBatchOperation()) {

      String lastTraceId = addToBatch(buffer, batchOperation);

      buffer.iterator().forEachRemaining(
          entry -> addCleanupEntry(entry, cleanupEpochs));

      // Commit transaction info to DB.
      flushedEpochs = buffer.stream()
          .map(DoubleBufferEntry::getTrxLogIndex)
          .sorted()
          .collect(Collectors.toList());

      long lastRatisTransactionIndex = flushedEpochs.get(
          flushedEpochs.size() - 1);

      long term = isRatisEnabled ?
          indexToTerm.apply(lastRatisTransactionIndex) : -1;

      addToBatchTransactionInfoWithTrace(lastTraceId,
          lastRatisTransactionIndex,
          () -> {
            omMetadataManager.getTransactionInfoTable().putWithBatch(
                batchOperation, TRANSACTION_INFO_KEY,
                new TransactionInfo.Builder()
                    .setTransactionIndex(lastRatisTransactionIndex)
                    .setCurrentTerm(term)
                    .build());
            return null;
          });

      long startTime = Time.monotonicNow();
      flushBatchWithTrace(lastTraceId, buffer.size(),
          () -> {
            omMetadataManager.getStore()
                .commitBatchOperation(batchOperation);
            return null;
          });

      ozoneManagerDoubleBufferMetrics.updateFlushTime(
          Time.monotonicNow() - startTime);
    }

    // Complete futures first and then do other things.
    // So that handler threads will be released.
    if (!isRatisEnabled) {
      clearReadyFutureQueue(buffer.size());
    }

    int flushedTransactionsSize = buffer.size();
    flushedTransactionCount.addAndGet(flushedTransactionsSize);
    flushIterations.incrementAndGet();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sync iteration {} flushed transactions in this iteration {}",
          flushIterations.get(),
          flushedTransactionsSize);
    }

    // Clean up committed transactions.
    cleanupCache(cleanupEpochs);

    if (isRatisEnabled) {
      releaseUnFlushedTransactions(flushedTransactionsSize);
    }
    // update the last updated index in OzoneManagerStateMachine.
    ozoneManagerRatisSnapShot.updateLastAppliedIndex(flushedEpochs);

    // set metrics.
    updateMetrics(flushedTransactionsSize);
  }

  private String addToBatch(Queue<DoubleBufferEntry<OMClientResponse>> buffer,
                            BatchOperation batchOperation) {
    String lastTraceId = null;
    for (DoubleBufferEntry<OMClientResponse> entry: buffer) {
      OMClientResponse response = entry.getResponse();
      OMResponse omResponse = response.getOMResponse();
      lastTraceId = omResponse.getTraceID();

      try {
        addToBatchWithTrace(omResponse,
            () -> {
              response.checkAndUpdateDB(omMetadataManager, batchOperation);
              return null;
            });
      } catch (IOException ex) {
        // During Adding to RocksDB batch entry got an exception.
        // We should terminate the OM.
        terminate(ex, 1, omResponse);
      } catch (Throwable t) {
        terminate(t, 2, omResponse);
      }
    }

    return lastTraceId;
  }

  /**
   * Splits the readyBuffer around the create snapshot request.
   * Returns, the list of queue split by create snapshot requests.
   *
   * CreateSnapshot is used as barrier because the checkpoint creation happens
   * in RocksDB callback flush. If multiple operations are flushed in one
   * specific batch, we are not sure at the flush of which specific operation
   * the callback is coming.
   * There could be a possibility of race condition that is exposed to rocksDB
   * behaviour for the batch.
   * Hence, we treat createSnapshot as separate batch flush.
   *
   * e.g. requestBuffer = [request1, request2, snapshotRequest1,
   * request3, snapshotRequest2, request4]
   * response = [[request1, request2], [snapshotRequest1], [request3],
   * [snapshotRequest2], [request4]]
   */
  private List<Queue<DoubleBufferEntry<OMClientResponse>>>
      splitReadyBufferAtCreateSnapshot() {
    List<Queue<DoubleBufferEntry<OMClientResponse>>> response =
        new ArrayList<>();

    Iterator<DoubleBufferEntry<OMClientResponse>> iterator =
        readyBuffer.iterator();

    OMResponse previousOmResponse = null;
    while (iterator.hasNext()) {
      DoubleBufferEntry<OMClientResponse> entry = iterator.next();
      OMResponse omResponse = entry.getResponse().getOMResponse();
      // New queue gets created in three conditions:
      // 1. It is first element in the response,
      // 2. Current request is createSnapshot request.
      // 3. Previous request was createSnapshot request.
      if (response.isEmpty() || omResponse.hasCreateSnapshotResponse()
          || (previousOmResponse != null &&
          previousOmResponse.hasCreateSnapshotResponse())) {
        response.add(new LinkedList<>());
      }

      response.get(response.size() - 1).add(entry);
      previousOmResponse = omResponse;
    }

    return response;
  }

  private void addCleanupEntry(DoubleBufferEntry entry, Map<String,
      List<Long>> cleanupEpochs) {
    Class<? extends OMClientResponse> responseClass =
        entry.getResponse().getClass();
    CleanupTableInfo cleanupTableInfo =
        responseClass.getAnnotation(CleanupTableInfo.class);
    if (cleanupTableInfo != null) {
      String[] cleanupTables;
      if (cleanupTableInfo.cleanupAll()) {
        cleanupTables = Arrays
            .stream(new OMDBDefinition().getColumnFamilies())
            .map(DBColumnFamilyDefinition::getTableName)
            .toArray(String[]::new);
      } else {
        cleanupTables = cleanupTableInfo.cleanupTables();
      }
      for (String table : cleanupTables) {
        cleanupEpochs.computeIfAbsent(table, list -> new ArrayList<>())
            .add(entry.getTrxLogIndex());
      }
    } else {
      // This is to catch early errors, when a new response class missed to
      // add CleanupTableInfo annotation.
      throw new RuntimeException("CleanupTableInfo Annotation is missing " +
          "for" + responseClass);
    }
  }

  /**
   * Completes futures for first count element form the readyFutureQueue
   * so that handler thread can be released asap.
   */
  private void clearReadyFutureQueue(int count) {
    while (!readyFutureQueue.isEmpty() && count > 0) {
      readyFutureQueue.remove().complete(null);
      count--;
    }
  }

  private void cleanupCache(Map<String, List<Long>> cleanupEpochs) {
    cleanupEpochs.forEach((tableName, epochs) -> {
      Collections.sort(epochs);
      omMetadataManager.getTable(tableName).cleanupCache(epochs);
    });
  }

  private synchronized void clearReadyBuffer() {
    readyBuffer.clear();
  }
  /**
   * Update OzoneManagerDoubleBuffer metrics values.
   * @param flushedTransactionsSize
   */
  private void updateMetrics(int flushedTransactionsSize) {
    ozoneManagerDoubleBufferMetrics.incrTotalNumOfFlushOperations();
    ozoneManagerDoubleBufferMetrics.incrTotalSizeOfFlushedTransactions(
        flushedTransactionsSize);
    ozoneManagerDoubleBufferMetrics.setAvgFlushTransactionsInOneIteration(
        (float) ozoneManagerDoubleBufferMetrics
            .getTotalNumOfFlushedTransactions() /
            ozoneManagerDoubleBufferMetrics.getTotalNumOfFlushOperations());
    if (maxFlushedTransactionsInOneIteration < flushedTransactionsSize) {
      maxFlushedTransactionsInOneIteration = flushedTransactionsSize;
      ozoneManagerDoubleBufferMetrics
          .setMaxNumberOfTransactionsFlushedInOneIteration(
              flushedTransactionsSize);
    }
    ozoneManagerDoubleBufferMetrics.updateQueueSize(flushedTransactionsSize);
  }

  /**
   * Stop OM DoubleBuffer flush thread.
   */
  // Ignore the sonar false positive on the InterruptedException issue
  // as this a normal flow of a shutdown.
  @SuppressWarnings("squid:S2142")
  public void stop() {
    stopDaemon();
    ozoneManagerDoubleBufferMetrics.unRegister();
  }

  @VisibleForTesting
  public void stopDaemon() {
    if (isRunning.compareAndSet(true, false)) {
      LOG.info("Stopping OMDoubleBuffer flush thread");
      daemon.interrupt();
      try {
        // Wait for daemon thread to exit
        daemon.join();
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for daemon to exit.", e);
      }
    } else {
      LOG.info("OMDoubleBuffer flush thread is not running.");
    }
  }

  private void terminate(Throwable t, int status) {
    terminate(t, status, null);
  }

  private void terminate(Throwable t, int status, OMResponse omResponse) {
    StringBuilder message = new StringBuilder(
        "During flush to DB encountered error in " +
        "OMDoubleBuffer flush thread " + Thread.currentThread().getName());
    if (omResponse != null) {
      message.append(" when handling OMRequest: ").append(omResponse);
    }
    ExitUtils.terminate(status, message.toString(), t, LOG);
  }

  /**
   * Returns the flushed transaction count to OM DB.
   * @return flushedTransactionCount
   */
  public long getFlushedTransactionCount() {
    return flushedTransactionCount.get();
  }

  /**
   * Returns total number of flush iterations run by sync thread.
   * @return flushIterations
   */
  public long getFlushIterations() {
    return flushIterations.get();
  }

  /**
   * Add OmResponseBufferEntry to buffer.
   * @param response
   * @param transactionIndex
   */
  public synchronized CompletableFuture<Void> add(OMClientResponse response,
      long transactionIndex) {
    currentBuffer.add(new DoubleBufferEntry<>(transactionIndex, response));
    notify();

    if (!isRatisEnabled) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      currentFutureQueue.add(future);
      return future;
    } else {
      // In Non-HA case we don't need future to be returned, and this return
      // status is not used.
      return null;
    }
  }

  /**
   * Check if transactions can be flushed or not. It waits till currentBuffer
   * size is greater than zero. When any item gets added to currentBuffer,
   * it gets notify signal. Returns true once currentBuffer size becomes greater
   * than zero. In case of any interruption, terminates the OM when daemon is
   * running otherwise returns false.
   */
  private synchronized boolean canFlush() {
    try {
      while (currentBuffer.size() == 0) {
        wait(Long.MAX_VALUE);
      }
      return true;
    }  catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      if (isRunning.get()) {
        final String message = "OMDoubleBuffer flush thread " +
            Thread.currentThread().getName() + " encountered Interrupted " +
            "exception while running";
        ExitUtils.terminate(1, message, ex, LOG);
      }
      LOG.info("OMDoubleBuffer flush thread {} is interrupted and will "
          + "exit.", Thread.currentThread().getName());
      return false;
    }
  }

  /**
   * Swaps the currentBuffer with readyBuffer so that the readyBuffer can be
   * used by sync thread to flush transactions to DB.
   */
  private synchronized void swapCurrentAndReadyBuffer() {
    Queue<DoubleBufferEntry<OMClientResponse>> temp = currentBuffer;
    currentBuffer = readyBuffer;
    readyBuffer = temp;

    if (!isRatisEnabled) {
      // Swap future queue.
      Queue<CompletableFuture<Void>> tempFuture = currentFutureQueue;
      currentFutureQueue = readyFutureQueue;
      readyFutureQueue = tempFuture;
    }
  }

  @VisibleForTesting
  public OzoneManagerDoubleBufferMetrics getOzoneManagerDoubleBufferMetrics() {
    return ozoneManagerDoubleBufferMetrics;
  }
}
