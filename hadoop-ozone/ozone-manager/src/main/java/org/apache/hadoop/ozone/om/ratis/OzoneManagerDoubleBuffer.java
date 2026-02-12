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

package org.apache.hadoop.ozone.om.ratis;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private Queue<Entry> currentBuffer;
  private Queue<Entry> readyBuffer;
  /**
   * Limit the number of un-flushed transactions for {@link OzoneManagerStateMachine}.
   */
  private final Semaphore unFlushedTransactions;

  /** To flush the buffers. */
  private final Daemon daemon;
  /** Is the {@link #daemon} running? */
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final AtomicBoolean isPaused = new AtomicBoolean(false);
  /** Notify flush operations are completed by the {@link #daemon}. */
  private final FlushNotifier flushNotifier;

  private final OMMetadataManager omMetadataManager;

  private final Consumer<TermIndex> updateLastAppliedIndex;

  private final S3SecretManager s3SecretManager;

  private final boolean isTracingEnabled;

  private final OzoneManagerDoubleBufferMetrics metrics = OzoneManagerDoubleBufferMetrics.create();

  /** Accumulative count (for testing and debug only). */
  private final AtomicLong flushedTransactionCount = new AtomicLong();
  /** The number of flush iterations (for testing and debug only). */
  private final AtomicLong flushIterations = new AtomicLong();

  /** Entry for {@link #currentBuffer} and {@link #readyBuffer}. */
  private static class Entry {
    private final TermIndex termIndex;
    private final OMClientResponse response;

    Entry(TermIndex termIndex, OMClientResponse response) {
      this.termIndex = termIndex;
      this.response = response;
    }

    TermIndex getTermIndex() {
      return termIndex;
    }

    OMClientResponse getResponse() {
      return response;
    }
  }

  /**
   *  Builder for creating OzoneManagerDoubleBuffer.
   */
  public static final class Builder {
    private OMMetadataManager omMetadataManager;
    private Consumer<TermIndex> updateLastAppliedIndex = termIndex -> { };
    private boolean isTracingEnabled = false;
    private int maxUnFlushedTransactionCount = 0;
    private FlushNotifier flushNotifier;
    private S3SecretManager s3SecretManager;
    private String threadPrefix = "";

    private Builder() { }

    public Builder setOmMetadataManager(OMMetadataManager omMetadataManager) {
      this.omMetadataManager = omMetadataManager;
      return this;
    }

    Builder setUpdateLastAppliedIndex(Consumer<TermIndex> updateLastAppliedIndex) {
      this.updateLastAppliedIndex = updateLastAppliedIndex;
      return this;
    }

    public Builder enableTracing(boolean enableTracing) {
      this.isTracingEnabled = enableTracing;
      return this;
    }

    public Builder setMaxUnFlushedTransactionCount(int maxUnFlushedTransactionCount) {
      this.maxUnFlushedTransactionCount = maxUnFlushedTransactionCount;
      return this;
    }

    Builder setFlushNotifier(FlushNotifier flushNotifier) {
      this.flushNotifier = flushNotifier;
      return this;
    }

    public Builder setThreadPrefix(String prefix) {
      this.threadPrefix = prefix;
      return this;
    }

    public Builder setS3SecretManager(S3SecretManager s3SecretManager) {
      this.s3SecretManager = s3SecretManager;
      return this;
    }

    public OzoneManagerDoubleBuffer build() {
      Preconditions.assertTrue(maxUnFlushedTransactionCount > 0L,
          () -> "maxUnFlushedTransactionCount = " + maxUnFlushedTransactionCount);
      if (flushNotifier == null) {
        flushNotifier = new FlushNotifier();
      }

      return new OzoneManagerDoubleBuffer(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  static Semaphore newSemaphore(int permits) {
    return permits > 0 ? new Semaphore(permits) : null;
  }

  private OzoneManagerDoubleBuffer(Builder b) {
    this.currentBuffer = new ConcurrentLinkedQueue<>();
    this.readyBuffer = new ConcurrentLinkedQueue<>();

    this.omMetadataManager = b.omMetadataManager;
    this.s3SecretManager = b.s3SecretManager;
    this.updateLastAppliedIndex = b.updateLastAppliedIndex;
    this.flushNotifier = b.flushNotifier;
    this.unFlushedTransactions = newSemaphore(b.maxUnFlushedTransactionCount);

    this.isTracingEnabled = b.isTracingEnabled;

    // Daemon thread which runs in background and flushes transactions to DB.
    daemon = new Daemon(this::flushTransactions);
    daemon.setName(b.threadPrefix + "OMDoubleBufferFlushThread");
  }

  public OzoneManagerDoubleBuffer start() {
    isRunning.set(true);
    daemon.start();
    return this;
  }

  @VisibleForTesting
  public void pause() {
    synchronized (this) {
      isPaused.set(true);
      this.notifyAll();
    }
  }

  @VisibleForTesting
  public void unpause() {
    synchronized (this) {
      isPaused.set(false);
      this.notifyAll();
    }
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
  void releaseUnFlushedTransactions(int n) {
    unFlushedTransactions.release(n);
  }

  // TODO: pass the trace id further down and trace all methods of DBStore.

  /**
   * Add to write batch with trace span if tracing is enabled.
   */
  private void addToBatchWithTrace(OMResponse omResponse,
      CheckedRunnable<IOException> runnable) throws IOException {
    if (!isTracingEnabled) {
      runnable.run();
      return;
    }
    String spanName = "DB-addToWriteBatch" + "-" +
        omResponse.getCmdType();
    TracingUtil.executeAsChildSpan(spanName, omResponse.getTraceID(), runnable);
  }

  /**
   * Flush write batch with trace span if tracing is enabled.
   */
  private void flushBatchWithTrace(String parentName, int batchSize,
      CheckedRunnable<IOException> runnable) throws IOException {
    if (!isTracingEnabled) {
      runnable.run();
      return;
    }
    String spanName = "DB-commitWriteBatch-Size-" + batchSize;
    TracingUtil.executeAsChildSpan(spanName, parentName, runnable);
  }

  /**
   * Add to writeBatch {@link TransactionInfo}.
   */
  private void addToBatchTransactionInfoWithTrace(String parentName,
      long transactionIndex, CheckedRunnable<IOException> runnable)
      throws IOException {
    if (!isTracingEnabled) {
      runnable.run();
      return;
    }
    String spanName = "DB-addWriteBatch-transactioninfo-" + transactionIndex;
    TracingUtil.executeAsChildSpan(spanName, parentName, runnable);
  }

  /**
   * Runs in a background thread and batches the transaction in currentBuffer
   * and commit to DB.
   */
  @VisibleForTesting
  public void flushTransactions() {
    while (isRunning.get() && canFlush()) {
      // Check if paused
      synchronized (this) {
        while (isPaused.get() && isRunning.get()) {
          try {
            this.wait();
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }

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
      final List<Queue<Entry>> bufferQueues = splitReadyBufferAtCreateSnapshot();
      for (Queue<Entry> buffer : bufferQueues) {
        flushBatch(buffer);
      }

      clearReadyBuffer();
      flushNotifier.notifyFlush();
    } catch (IOException ex) {
      terminate(ex, 1);
    } catch (Throwable t) {
      terminate(t, 2);
    }
  }

  private void flushBatch(Queue<Entry> buffer) throws IOException {
    Map<String, List<Long>> cleanupEpochs = new HashMap<>();
    // Commit transaction info to DB.
    final List<TermIndex> flushedTransactions = buffer.stream()
        .map(Entry::getTermIndex)
        .sorted()
        .collect(Collectors.toList());
    final int flushedTransactionsSize = flushedTransactions.size();
    final TermIndex lastTransaction = flushedTransactions.get(flushedTransactionsSize - 1);

    try (BatchOperation batchOperation = omMetadataManager.getStore()
        .initBatchOperation()) {

      String lastTraceId = addToBatch(buffer, batchOperation);

      buffer.iterator().forEachRemaining(
          entry -> addCleanupEntry(entry, cleanupEpochs));


      addToBatchTransactionInfoWithTrace(lastTraceId,
          lastTransaction.getIndex(),
          () -> omMetadataManager.getTransactionInfoTable().putWithBatch(
              batchOperation, TRANSACTION_INFO_KEY, TransactionInfo.valueOf(lastTransaction)));

      long startTime = Time.monotonicNow();
      flushBatchWithTrace(lastTraceId, buffer.size(),
          () -> omMetadataManager.getStore()
              .commitBatchOperation(batchOperation));

      metrics.updateFlushTime(Time.monotonicNow() - startTime);
    }

    final long accumulativeCount = flushedTransactionCount.addAndGet(flushedTransactionsSize);
    final long flushedIterations = flushIterations.incrementAndGet();
    LOG.debug("Sync iteration: {}, size in this iteration: {}, accumulative count: {}",
        flushedIterations, flushedTransactionsSize, accumulativeCount);

    // Clean up committed transactions.
    cleanupCache(cleanupEpochs);

    releaseUnFlushedTransactions(flushedTransactionsSize);
    // update the last updated index in OzoneManagerStateMachine.
    updateLastAppliedIndex.accept(lastTransaction);

    // set metrics.
    metrics.updateFlush(flushedTransactionsSize);
  }

  private String addToBatch(Queue<Entry> buffer, BatchOperation batchOperation) {
    String lastTraceId = null;
    for (Entry entry: buffer) {
      OMClientResponse response = entry.getResponse();
      OMResponse omResponse = response.getOMResponse();
      lastTraceId = omResponse.getTraceID();

      try {
        addToBatchWithTrace(omResponse,
            () -> response.checkAndUpdateDB(omMetadataManager, batchOperation));
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
   * <p>
   * CreateSnapshot is used as barrier because the checkpoint creation happens
   * in RocksDB callback flush. If multiple operations are flushed in one
   * specific batch, we are not sure at the flush of which specific operation
   * the callback is coming.
   * PurgeSnapshot is also considered a barrier, since purgeSnapshot transaction on a standalone basis is an
   * idempotent operation. Once the directory gets deleted the previous transactions that have been performed on the
   * snapshotted rocksdb would start failing on replay since those transactions have not been committed but the
   * directory could have been partially deleted/ fully deleted. This could also lead to inconsistencies in the DB
   * reads from the purged rocksdb if operations are not performed consciously.
   * There could be a possibility of race condition that is exposed to rocksDB behaviour for the batch.
   * Hence, we treat createSnapshot as separate batch flush.
   * <p>
   * e.g. requestBuffer = [request1, request2, snapshotRequest1,
   * request3, snapshotRequest2, request4]
   * response = [[request1, request2], [snapshotRequest1], [request3],
   * [snapshotRequest2], [request4]]
   */
  private synchronized List<Queue<Entry>> splitReadyBufferAtCreateSnapshot() {
    final List<Queue<Entry>> response = new ArrayList<>();
    OMResponse previousOmResponse = null;
    for (final Entry entry : readyBuffer) {
      OMResponse omResponse = entry.getResponse().getOMResponse();
      // New queue gets created in three conditions:
      // 1. It is first element in the response,
      // 2. Current request is createSnapshot/purgeSnapshot request.
      // 3. Previous request was createSnapshot/purgeSnapshot request.
      if (response.isEmpty() || isStandaloneBatchCmdTypes(omResponse)
          || isStandaloneBatchCmdTypes(previousOmResponse)) {
        response.add(new LinkedList<>());
      }

      response.get(response.size() - 1).add(entry);
      previousOmResponse = omResponse;
    }

    return response;
  }

  private static boolean isStandaloneBatchCmdTypes(OMResponse response) {
    if (response == null) {
      return false;
    }
    final OzoneManagerProtocolProtos.Type type = response.getCmdType();
    return type == OzoneManagerProtocolProtos.Type.SnapshotPurge
        || type == OzoneManagerProtocolProtos.Type.CreateSnapshot;
  }

  private void addCleanupEntry(Entry entry, Map<String, List<Long>> cleanupEpochs) {
    Class<? extends OMClientResponse> responseClass =
        entry.getResponse().getClass();
    CleanupTableInfo cleanupTableInfo =
        responseClass.getAnnotation(CleanupTableInfo.class);
    if (cleanupTableInfo != null) {
      final List<String> cleanupTables;
      if (cleanupTableInfo.cleanupAll()) {
        cleanupTables = OMDBDefinition.get().getColumnFamilyNames();
      } else {
        cleanupTables = Arrays.asList(cleanupTableInfo.cleanupTables());
      }
      for (String table : cleanupTables) {
        cleanupEpochs.computeIfAbsent(table, list -> new ArrayList<>())
            .add(entry.getTermIndex().getIndex());
      }
    } else {
      // This is to catch early errors, when a new response class missed to
      // add CleanupTableInfo annotation.
      throw new RuntimeException("CleanupTableInfo Annotation is missing " +
          "for" + responseClass);
    }
  }

  private void cleanupCache(Map<String, List<Long>> cleanupEpochs) {
    cleanupEpochs.forEach((tableName, epochs) -> {
      Collections.sort(epochs);
      omMetadataManager.getTable(tableName).cleanupCache(epochs);
      // Check if the table is S3SecretTable, if yes, then clear the cache.
      if (tableName.equals(OMDBDefinition.S3_SECRET_TABLE_DEF.getName())) {
        s3SecretManager.clearS3Cache(epochs);
      }
    });
  }

  private synchronized void clearReadyBuffer() {
    readyBuffer.clear();
  }

  /**
   * Stop OM DoubleBuffer flush thread.
   */
  // Ignore the sonar false positive on the InterruptedException issue
  // as this a normal flow of a shutdown.
  @SuppressWarnings("squid:S2142")
  public void stop() {
    stopDaemon();
    metrics.unRegister();
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
    StringBuilder message = new StringBuilder("During flush to DB encountered error in OMDoubleBuffer flush thread ")
        .append(Thread.currentThread().getName());
    if (omResponse != null) {
      message.append(" when handling OMRequest: ").append(omResponse);
    }
    ExitUtils.terminate(status, message.toString(), t, LOG);
  }

  /**
   * Add OmResponseBufferEntry to buffer.
   */
  public synchronized void add(OMClientResponse response, TermIndex termIndex) {
    currentBuffer.add(new Entry(termIndex, response));
    notify();
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
      while (currentBuffer.isEmpty()) {
        // canFlush() only gets called when the readyBuffer is empty.
        // Since both buffers are empty, notify once for each.
        flushNotifier.notifyFlush();
        flushNotifier.notifyFlush();
        wait(1000L);
      }
      return true;
    } catch (InterruptedException ex) {
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
    final Queue<Entry> temp = currentBuffer;
    currentBuffer = readyBuffer;
    readyBuffer = temp;
  }

  OzoneManagerDoubleBufferMetrics getMetrics() {
    return metrics;
  }

  /** @return the flushed transaction count to OM DB. */
  long getFlushedTransactionCountForTesting() {
    return flushedTransactionCount.get();
  }

  /** @return total number of flush iterations run by sync thread. */
  long getFlushIterationsForTesting() {
    return flushIterations.get();
  }

  int getCurrentBufferSize() {
    return currentBuffer.size();
  }

  synchronized int getReadyBufferSize() {
    return readyBuffer.size();
  }

  @VisibleForTesting
  public void resume() {
    isRunning.set(true);
  }

  CompletableFuture<Integer> awaitFlushAsync() {
    return flushNotifier.await();
  }

  public void awaitFlush() throws InterruptedException {
    try {
      awaitFlushAsync().get();
    } catch (ExecutionException e) {
      // the future will never be completed exceptionally.
      throw new IllegalStateException(e);
    }
  }

  static class FlushNotifier {
    /** The size of the map is at most two since it uses {@link #flushCount} + 2 in {@link #await()} .*/
    private final Map<Integer, Entry> flushFutures = new TreeMap<>();
    private int awaitCount;
    private int flushCount;

    synchronized CompletableFuture<Integer> await() {
      awaitCount++;
      final int flush = flushCount + 2;
      LOG.debug("await flush {}", flush);
      final Entry entry = flushFutures.computeIfAbsent(flush, key -> new Entry());
      Preconditions.assertTrue(flushFutures.size() <= 2);
      return entry.await();
    }

    synchronized int notifyFlush() {
      final int await = awaitCount;
      final int flush = ++flushCount;
      final Entry removed = flushFutures.remove(flush);
      if (removed != null) {
        awaitCount -= removed.complete();
      }
      LOG.debug("notifyFlush {}, awaitCount: {} -> {}", flush, await, awaitCount);
      return await;
    }

    static class Entry {
      private final CompletableFuture<Integer> future = new CompletableFuture<>();
      private int count;

      private CompletableFuture<Integer> await() {
        count++;
        return future;
      }

      private int complete() {
        Preconditions.assertTrue(future.complete(count));
        return future.join();
      }
    }
  }
}
