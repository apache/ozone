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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.ratis.helpers.DoubleBufferEntry;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.utils.db.BatchOperation;

import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class OzoneManagerDoubleBuffer {

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

  private Daemon daemon;
  private final OMMetadataManager omMetadataManager;
  private final AtomicLong flushedTransactionCount = new AtomicLong(0);
  private final AtomicLong flushIterations = new AtomicLong(0);
  private volatile boolean isRunning;

  private final OzoneManagerRatisSnapshot ozoneManagerRatisSnapShot;

  public OzoneManagerDoubleBuffer(OMMetadataManager omMetadataManager,
      OzoneManagerRatisSnapshot ozoneManagerRatisSnapShot) {
    this.currentBuffer = new ConcurrentLinkedQueue<>();
    this.readyBuffer = new ConcurrentLinkedQueue<>();
    this.omMetadataManager = omMetadataManager;

    this.ozoneManagerRatisSnapShot = ozoneManagerRatisSnapShot;

    isRunning = true;
    // Daemon thread which runs in back ground and flushes transactions to DB.
    daemon = new Daemon(this::flushTransactions);
    daemon.setName("OMDoubleBufferFlushThread");
    daemon.start();


  }

  /**
   * Runs in a background thread and batches the transaction in currentBuffer
   * and commit to DB.
   */
  private void flushTransactions() {
    while(isRunning) {
      try {
        if (canFlush()) {
          setReadyBuffer();
          final BatchOperation batchOperation = omMetadataManager.getStore()
              .initBatchOperation();

          readyBuffer.iterator().forEachRemaining((entry) -> {
            try {
              entry.getResponse().addToDBBatch(omMetadataManager,
                  batchOperation);
            } catch (IOException ex) {
              // During Adding to RocksDB batch entry got an exception.
              // We should terminate the OM.
              terminate(ex);
            }
          });

          omMetadataManager.getStore().commitBatchOperation(batchOperation);
          int flushedTransactionsSize = readyBuffer.size();
          flushedTransactionCount.addAndGet(flushedTransactionsSize);
          flushIterations.incrementAndGet();

          LOG.debug("Sync Iteration {} flushed transactions in this " +
                  "iteration{}", flushIterations.get(),
              flushedTransactionsSize);

          long lastRatisTransactionIndex =
              readyBuffer.stream().map(DoubleBufferEntry::getTrxLogIndex)
              .max(Long::compareTo).get();

          readyBuffer.clear();
          // cleanup cache.
          cleanupCache(lastRatisTransactionIndex);

          // TODO: Need to revisit this logic, once we have multiple
          //  executors for volume/bucket request handling. As for now
          //  transactions are serialized this should be fine.
          // update the last updated index in OzoneManagerStateMachine.
          ozoneManagerRatisSnapShot.updateLastAppliedIndex(
              lastRatisTransactionIndex);
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        if (isRunning) {
          final String message = "OMDoubleBuffer flush thread " +
              Thread.currentThread().getName() + " encountered Interrupted " +
              "exception while running";
          ExitUtils.terminate(1, message, ex, LOG);
        } else {
          LOG.info("OMDoubleBuffer flush thread " +
              Thread.currentThread().getName() + " is interrupted and will " +
              "exit. {}", Thread.currentThread().getName());
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

  private void cleanupCache(long lastRatisTransactionIndex) {
    // As now only volume and bucket transactions are handled only called
    // cleanupCache on bucketTable.
    // TODO: After supporting all write operations we need to call
    //  cleanupCache on the tables only when buffer has entries for that table.
    omMetadataManager.getBucketTable().cleanupCache(lastRatisTransactionIndex);
    omMetadataManager.getVolumeTable().cleanupCache(lastRatisTransactionIndex);
    omMetadataManager.getUserTable().cleanupCache(lastRatisTransactionIndex);
  }

  /**
   * Stop OM DoubleBuffer flush thread.
   */
  public synchronized void stop() {
    if (isRunning) {
      LOG.info("Stopping OMDoubleBuffer flush thread");
      isRunning = false;
      daemon.interrupt();
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
  public synchronized void add(OMClientResponse response,
      long transactionIndex) {
    currentBuffer.add(new DoubleBufferEntry<>(transactionIndex, response));
    notify();
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
  }

}

