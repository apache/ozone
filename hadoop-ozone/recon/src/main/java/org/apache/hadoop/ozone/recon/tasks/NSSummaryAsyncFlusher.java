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

package org.apache.hadoop.ozone.recon.tasks;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async flusher for NSSummary maps with background thread.
 * Workers submit their maps to a queue, background thread processes them.
 */
public final class NSSummaryAsyncFlusher implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(NSSummaryAsyncFlusher.class);

  private final BlockingQueue<Map<Long, NSSummary>> flushQueue;
  private final Thread backgroundFlusher;
  private final AtomicReference<FlushState> state =
      new AtomicReference<>(FlushState.RUNNING);
  private volatile Exception failureCause = null;
  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final String taskName;

  private enum FlushState {
    RUNNING,
    STOPPING,
    STOPPED,
    FAILED
  }
  
  private NSSummaryAsyncFlusher(ReconNamespaceSummaryManager reconNamespaceSummaryManager,
                                 String taskName,
                                 int queueCapacity) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.taskName = taskName;
    this.flushQueue = new LinkedBlockingQueue<>(queueCapacity);
    
    this.backgroundFlusher = new Thread(this::flushLoop, taskName + "-AsyncFlusher");
    this.backgroundFlusher.setDaemon(true);
  }
  
  /**
   * Factory method to create and start an async flusher.
   */
  public static NSSummaryAsyncFlusher create(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      String taskName,
      int queueCapacity) {
    NSSummaryAsyncFlusher flusher = new NSSummaryAsyncFlusher(
        reconNamespaceSummaryManager, taskName, queueCapacity);
    flusher.backgroundFlusher.start();
    LOG.info("{}: Started async flusher with queue capacity {}", taskName, queueCapacity);
    return flusher;
  }
  
  /**
   * Submit a worker map for async flushing.
   * Blocks if queue is full (natural backpressure).
   * @throws IOException if the async flusher has failed
   */
  public void submitForFlush(Map<Long, NSSummary> workerMap) 
      throws InterruptedException, IOException {
    // Check if flusher has failed - reject new submissions
    if (state.get() == FlushState.FAILED) {
      throw new IOException(taskName + ": Cannot submit - async flusher has failed", 
          failureCause);
    }
    
    flushQueue.put(workerMap);
    LOG.debug("{}: Submitted map with {} entries, queue size now {}", 
        taskName, workerMap.size(), flushQueue.size());
  }
  
  /**
   * Check if the async flusher has encountered any failures.
   * Workers should call this periodically to detect failures fast.
   * @throws IOException if a failure has occurred
   */
  public void checkForFailures() throws IOException {
    if (state.get() == FlushState.FAILED && failureCause != null) {
      if (failureCause instanceof IOException) {
        throw (IOException) failureCause;
      } else {
        throw new IOException(taskName + ": Async flusher failed", failureCause);
      }
    }
  }
  
  /**
   * Background thread loop that processes flush queue.
   */
  private void flushLoop() {
    while (state.get() == FlushState.RUNNING || !flushQueue.isEmpty()) {
      try {
        // Attempt to retrieve one batch from the queue
        Map<Long, NSSummary> workerMap = flushQueue.poll(100, TimeUnit.MILLISECONDS);
        
        if (workerMap == null) {
          continue;
        }
        
        // Process this batch
        LOG.debug("{}: Background thread processing batch with {} entries", taskName, workerMap.size());
        flushWithPropagation(workerMap);
        LOG.debug("{}: Background thread finished batch", taskName);
        
      } catch (InterruptedException e) {
        // If we're stopping, ignore interrupts and keep draining the queue.
        // Otherwise, preserve interrupt and exit.
        if (state.get() == FlushState.STOPPING) {
          LOG.debug("{}: Flusher thread interrupted while stopping; continuing to drain queue",
              taskName);
          Thread.interrupted(); // clear interrupt flag
          continue;
        }
        LOG.info("{}: Flusher thread interrupted", taskName);
        Thread.currentThread().interrupt();
        break;
      } catch (IOException e) {
        // For DB write errors
        LOG.error("{}: FATAL - DB write failed, stopping async flusher. " +
            "Remaining {} batches in queue will NOT be processed. " +
            "Workers will be stopped immediately.",
            taskName, flushQueue.size(), e);
        failureCause = e;
        state.set(FlushState.FAILED);
        break;
      } catch (Exception e) {
        // Other unexpected errors are also fatal
        LOG.error("{}: FATAL - Unexpected error in flush loop, stopping async flusher. " +
            "Remaining {} batches in queue will NOT be processed. " +
            "Workers will be stopped immediately.",
            taskName, flushQueue.size(), e);
        failureCause = e;
        state.set(FlushState.FAILED);
        break;
      }
    }
    
    // Only set STOPPED if we didn't fail
    if (state.get() != FlushState.FAILED) {
      state.set(FlushState.STOPPED);
    }
    
    LOG.info("{}: Async flusher stopped with state: {}", taskName, state.get());
  }

  /**
   * Flush worker map with propagation to ancestors.
   */
  private void flushWithPropagation(Map<Long, NSSummary> workerMap) throws IOException {

    Map<Long, NSSummary> mergedMap = new HashMap<>();

    // For each object in worker map (could be either a directory or bucket)
    for (Map.Entry<Long, NSSummary> entry : workerMap.entrySet()) {
      long currentObjectId = entry.getKey();
      NSSummary delta = entry.getValue();

      // Get actual UpToDate nssummary (check merged map first, then DB)
      NSSummary existingNSSummary = mergedMap.get(currentObjectId);
      if (existingNSSummary == null) {
        existingNSSummary = reconNamespaceSummaryManager.getNSSummary(currentObjectId);
      }

      if (existingNSSummary == null) {
        // Object doesn't exist in DB yet - use delta as base (has metadata like dirName, parentId)
        existingNSSummary = delta;
      } else {
        // Object exists in DB - merge delta into it
        
        // Skip numeric merging if delta has no file data (e.g., directory skeleton with zero counts)
        if (delta.getNumOfFiles() > 0 || delta.getSizeOfFiles() > 0) {
          existingNSSummary.setNumOfFiles(
              existingNSSummary.getNumOfFiles() + delta.getNumOfFiles());
          existingNSSummary.setSizeOfFiles(
              existingNSSummary.getSizeOfFiles() + delta.getSizeOfFiles());
          existingNSSummary.setReplicatedSizeOfFiles(
              existingNSSummary.getReplicatedSizeOfFiles() + delta.getReplicatedSizeOfFiles());

          // Merge file size buckets
          int[] actualBucket = existingNSSummary.getFileSizeBucket();
          int[] deltaBucket = delta.getFileSizeBucket();
          for (int i = 0; i < actualBucket.length; i++) {
            actualBucket[i] += deltaBucket[i];
          }
          existingNSSummary.setFileSizeBucket(actualBucket);
        }

        // Merge child dirs (needed for directory relationships)
        existingNSSummary.getChildDir().addAll(delta.getChildDir());

        // Repair dirName if existing entry is missing it and delta has the value
        if (StringUtils.isEmpty(existingNSSummary.getDirName()) && StringUtils.isNotEmpty(delta.getDirName())) {
          existingNSSummary.setDirName(delta.getDirName());
        }
        // Repair parentId if existing entry is missing it and delta has the value
        if (existingNSSummary.getParentId() == 0 && delta.getParentId() != 0) {
          existingNSSummary.setParentId(delta.getParentId());
        }
      }

      // Store updated object in merged map
      mergedMap.put(currentObjectId, existingNSSummary);

      if (delta.getSizeOfFiles() > 0 || delta.getNumOfFiles() > 0) {
        // Propagate delta to ancestors (parent, grandparent, etc.)
        propagateDeltaToAncestors(existingNSSummary.getParentId(), delta, mergedMap);
      }
    }

    // Write merged map to DB
    writeToDb(mergedMap);
    LOG.debug("{}: Flush completed, wrote {} entries", taskName, mergedMap.size());
  }

  /**
   * Recursively propagate delta values up the ancestor Id.
   * Pattern: Check merged map first (for updates in this batch), then DB, for ACTUAL value.
   */
  private void propagateDeltaToAncestors(long ancestorId, NSSummary delta, 
                                          Map<Long, NSSummary> mergedMap) throws IOException {
    // Base case: reached above bucket level
    if (ancestorId == 0) {
      return;
    }
    
    // Get ACTUAL ancestor (check merged map first for most up-to-date, then DB)
    NSSummary actualAncestor = mergedMap.get(ancestorId);
    if (actualAncestor == null) {
      actualAncestor = reconNamespaceSummaryManager.getNSSummary(ancestorId);
      if (actualAncestor == null) {
        // Ancestor not in DB yet return
        return;
      }
    }
    
    // Apply DELTA to ACTUAL ancestor
    actualAncestor.setNumOfFiles(
        actualAncestor.getNumOfFiles() + delta.getNumOfFiles());
    actualAncestor.setSizeOfFiles(
        actualAncestor.getSizeOfFiles() + delta.getSizeOfFiles());
    actualAncestor.setReplicatedSizeOfFiles(
        actualAncestor.getReplicatedSizeOfFiles() + delta.getReplicatedSizeOfFiles());
    
    // Store updated ACTUAL ancestor in merged map
    mergedMap.put(ancestorId, actualAncestor);
    
    // Recursively propagate to next ancestor (grandparent, great-grandparent, etc.)
    propagateDeltaToAncestors(actualAncestor.getParentId(), delta, mergedMap);
  }
  
  /**
   * Write merged map to DB using batch operation.
   */
  private void writeToDb(Map<Long, NSSummary> mergedMap) throws IOException {
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      for (Map.Entry<Long, NSSummary> entry : mergedMap.entrySet()) {
        reconNamespaceSummaryManager.batchStoreNSSummaries(
            rdbBatchOperation, entry.getKey(), entry.getValue());
      }
      reconNamespaceSummaryManager.commitBatchOperation(rdbBatchOperation);
      LOG.debug("{}: Wrote {} entries to DB", taskName, mergedMap.size());
    } catch (IOException e) {
      LOG.error("{}: Failed to flush to DB", taskName, e);
      throw e;
    }
  }
  
  @Override
  public void close() throws IOException {
    LOG.info("{}: Shutting down async flusher", taskName);
    try {
      // Tell the background thread to stop once the queue is drained.
      state.set(FlushState.STOPPING);
      backgroundFlusher.join();
      
      // Check if there were any failures during processing
      checkForFailures();
      
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while shutting down async flusher", e);
    }
    
    LOG.info("{}: Async flusher shut down complete", taskName);
  }
}

