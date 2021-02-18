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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.function.SupplierWithIOException;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.ratis.helpers.DoubleBufferEntry;
import org.apache.hadoop.ozone.om.ratis.metrics.OzoneManagerDoubleBufferMetrics;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.ratis.util.ExitUtils;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

/**
 * This class implements DoubleBuffer implementation of OMClientResponse's. In
 * DoubleBuffer it has 2 buffers one is currentBuffer and other is
 * readyBuffer. The current OM requests will be always added to currentBuffer.
 * Flush thread will be running in background, it check's if currentBuffer has
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

    public OzoneManagerDoubleBuffer build() {
      if (isRatisEnabled) {
        Preconditions.checkNotNull(rs, "When ratis is enabled, " +
                "OzoneManagerRatisSnapshot should not be null");
        Preconditions.checkNotNull(indexToTerm, "When ratis is enabled " +
            "indexToTerm should not be null");
      }
      return new OzoneManagerDoubleBuffer(mm, rs, isRatisEnabled,
          isTracingEnabled, indexToTerm);
    }
  }

  private OzoneManagerDoubleBuffer(OMMetadataManager omMetadataManager,
      OzoneManagerRatisSnapshot ozoneManagerRatisSnapShot,
      boolean isRatisEnabled, boolean isTracingEnabled,
      Function<Long, Long> indexToTerm) {
    this.currentBuffer = new ConcurrentLinkedQueue<>();
    this.readyBuffer = new ConcurrentLinkedQueue<>();

    this.isRatisEnabled = isRatisEnabled;
    this.isTracingEnabled = isTracingEnabled;
    if (!isRatisEnabled) {
      this.currentFutureQueue = new ConcurrentLinkedQueue<>();
      this.readyFutureQueue = new ConcurrentLinkedQueue<>();
    } else {
      this.currentFutureQueue = null;
      this.readyFutureQueue = null;
    }

    this.omMetadataManager = omMetadataManager;
    this.ozoneManagerRatisSnapShot = ozoneManagerRatisSnapShot;
    this.ozoneManagerDoubleBufferMetrics =
        OzoneManagerDoubleBufferMetrics.create();
    this.indexToTerm = indexToTerm;

    isRunning.set(true);
    // Daemon thread which runs in back ground and flushes transactions to DB.
    daemon = new Daemon(this::flushTransactions);
    daemon.setName("OMDoubleBufferFlushThread");
    daemon.start();

  }

  // TODO: pass the trace id further down and trace all methods of DBStore.

  /**
   * add to write batch with trace span if tracing is enabled.
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
   * flush write batch with trace span if tracing is enabled.
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
    while (isRunning.get()) {
      try {
        if (canFlush()) {
          Map<String, List<Long>> cleanupEpochs = new HashMap<>();

          setReadyBuffer();
          List<Long> flushedEpochs = null;
          try(BatchOperation batchOperation = omMetadataManager.getStore()
              .initBatchOperation()) {

            AtomicReference<String> lastTraceId = new AtomicReference<>();
            readyBuffer.iterator().forEachRemaining((entry) -> {
              try {
                OMResponse omResponse = entry.getResponse().getOMResponse();
                lastTraceId.set(omResponse.getTraceID());
                addToBatchWithTrace(omResponse,
                    (SupplierWithIOException<Void>) () -> {
                      entry.getResponse().checkAndUpdateDB(omMetadataManager,
                          batchOperation);
                      return null;
                    });

                addCleanupEntry(entry, cleanupEpochs);

              } catch (IOException ex) {
                // During Adding to RocksDB batch entry got an exception.
                // We should terminate the OM.
                terminate(ex);
              }
            });

            // Commit transaction info to DB.
            flushedEpochs = readyBuffer.stream().map(
                DoubleBufferEntry::getTrxLogIndex)
                .sorted().collect(Collectors.toList());
            long lastRatisTransactionIndex = flushedEpochs.get(
                flushedEpochs.size() - 1);
            long term = isRatisEnabled ?
                indexToTerm.apply(lastRatisTransactionIndex) : -1;

            addToBatchTransactionInfoWithTrace(lastTraceId.get(),
                lastRatisTransactionIndex,
                (SupplierWithIOException<Void>) () -> {
                  omMetadataManager.getTransactionInfoTable().putWithBatch(
                      batchOperation, TRANSACTION_INFO_KEY,
                      new TransactionInfo.Builder()
                          .setTransactionIndex(lastRatisTransactionIndex)
                          .setCurrentTerm(term).build());
                  return null;
                });

            long startTime = Time.monotonicNow();
            flushBatchWithTrace(lastTraceId.get(), readyBuffer.size(),
                (SupplierWithIOException<Void>) () -> {
                  omMetadataManager.getStore().commitBatchOperation(
                      batchOperation);
                  return null;
                });
            ozoneManagerDoubleBufferMetrics.updateFlushTime(
                Time.monotonicNow() - startTime);
          }

          // Complete futures first and then do other things. So, that
          // handler threads will be released.
          if (!isRatisEnabled) {
            // Once all entries are flushed, we can complete their future.
            readyFutureQueue.iterator().forEachRemaining((entry) -> {
              entry.complete(null);
            });

            readyFutureQueue.clear();
          }

          int flushedTransactionsSize = readyBuffer.size();
          flushedTransactionCount.addAndGet(flushedTransactionsSize);
          flushIterations.incrementAndGet();

          if (LOG.isDebugEnabled()) {
            LOG.debug("Sync Iteration {} flushed transactions in this " +
                    "iteration{}", flushIterations.get(),
                flushedTransactionsSize);
          }

          // When non-HA do the sort step here, as the sorted list is not
          // required for flush to DB. As in non-HA we want to complete
          // futures as quick as possible after flush to DB, to release rpc
          // handler threads.
          if (!isRatisEnabled) {
            flushedEpochs =
                readyBuffer.stream().map(DoubleBufferEntry::getTrxLogIndex)
                    .sorted().collect(Collectors.toList());
          }


          // Clean up committed transactions.

          cleanupCache(cleanupEpochs);

          readyBuffer.clear();

          // update the last updated index in OzoneManagerStateMachine.
          ozoneManagerRatisSnapShot.updateLastAppliedIndex(
              flushedEpochs);

          // set metrics.
          updateMetrics(flushedTransactionsSize);


        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        if (isRunning.get()) {
          final String message = "OMDoubleBuffer flush thread " +
              Thread.currentThread().getName() + " encountered Interrupted " +
              "exception while running";
          ExitUtils.terminate(1, message, ex, LOG);
        } else {
          LOG.info("OMDoubleBuffer flush thread {} is interrupted and will "
              + "exit. {}", Thread.currentThread().getName(),
                  Thread.currentThread().getName());
        }
      } catch (IOException ex) {
        terminate(ex);
      } catch (Throwable t) {
        final String s = "OMDoubleBuffer flush thread" +
            Thread.currentThread().getName() + "encountered Throwable error";
        ExitUtils.terminate(2, s, t, LOG);
      }
    }
  }


  private void addCleanupEntry(DoubleBufferEntry entry, Map<String,
      List<Long>> cleanupEpochs) {
    Class<? extends OMClientResponse> responseClass =
        entry.getResponse().getClass();
    CleanupTableInfo cleanupTableInfo =
        responseClass.getAnnotation(CleanupTableInfo.class);
    if (cleanupTableInfo != null) {
      String[] cleanupTables = cleanupTableInfo.cleanupTables();
      for (String table : cleanupTables) {
        cleanupEpochs.computeIfAbsent(table, list -> new ArrayList<>())
            .add(entry.getTrxLogIndex());
      }
    } else {
      // This is to catch early errors, when an new response class missed to
      // add CleanupTableInfo annotation.
      throw new RuntimeException("CleanupTableInfo Annotation is missing " +
          "for" + responseClass);
    }
  }



  private void cleanupCache(Map<String, List<Long>> cleanupEpochs) {
    cleanupEpochs.forEach((tableName, epochs) -> {
      Collections.sort(epochs);
      omMetadataManager.getTable(tableName).cleanupCache(epochs);
    });
  }

  /**
   * Update OzoneManagerDoubleBuffer metrics values.
   * @param flushedTransactionsSize
   */
  private void updateMetrics(
      long flushedTransactionsSize) {
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
  }

  /**
   * Stop OM DoubleBuffer flush thread.
   */
  // Ignore the sonar false positive on the InterruptedException issue
  // as this a normal flow of a shutdown.
  @SuppressWarnings("squid:S2142")
  public void stop() {
    if (isRunning.compareAndSet(true, false)) {
      LOG.info("Stopping OMDoubleBuffer flush thread");
      daemon.interrupt();
      try {
        // Wait for daemon thread to exit
        daemon.join();
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for daemon to exit.", e);
      }

      // stop metrics.
      ozoneManagerDoubleBufferMetrics.unRegister();
    } else {
      LOG.info("OMDoubleBuffer flush thread is not running.");
    }

  }

  private void terminate(IOException ex) {
    String message = "During flush to DB encountered error in " +
        "OMDoubleBuffer flush thread " + Thread.currentThread().getName();
    ExitUtils.terminate(1, message, ex, LOG);
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
   * Check can we flush transactions or not. This method wait's until
   * currentBuffer size is greater than zero, once currentBuffer size is
   * greater than zero it gets notify signal, and it returns true
   * indicating that we are ready to flush.
   *
   * @return boolean
   */
  private synchronized boolean canFlush() throws InterruptedException {
    // When transactions are added to buffer it notifies, then we check if
    // currentBuffer size once and return from this method.
    while (currentBuffer.size() == 0) {
      wait(Long.MAX_VALUE);
    }
    return true;
  }

  /**
   * Prepares the readyBuffer which is used by sync thread to flush
   * transactions to OM DB. This method swaps the currentBuffer and readyBuffer.
   */
  private synchronized void setReadyBuffer() {
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