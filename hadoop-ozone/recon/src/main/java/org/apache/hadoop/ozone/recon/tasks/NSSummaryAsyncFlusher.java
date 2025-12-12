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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async flusher for NSSummary maps with background thread.
 * Workers submit their maps to a queue, background thread processes them.
 */
public class NSSummaryAsyncFlusher implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(NSSummaryAsyncFlusher.class);
  
  private final BlockingQueue<Map<Long, NSSummary>> flushQueue;
  private final Thread backgroundFlusher;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final String taskName;
  
  // Sentinel value to signal shutdown
  private static final Map<Long, NSSummary> SHUTDOWN_SIGNAL = new HashMap<>();
  
  public NSSummaryAsyncFlusher(ReconNamespaceSummaryManager reconNamespaceSummaryManager,
                                String taskName,
                                int queueCapacity) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.taskName = taskName;
    this.flushQueue = new LinkedBlockingQueue<>(queueCapacity);
    
    this.backgroundFlusher = new Thread(this::flushLoop, taskName + "-AsyncFlusher");
    this.backgroundFlusher.setDaemon(true);
    this.backgroundFlusher.start();
    LOG.info("{}: Started async flusher with queue capacity {}", taskName, queueCapacity);
  }
  
  /**
   * Submit a worker map for async flushing.
   * Blocks if queue is full (natural backpressure).
   */
  public void submitForFlush(Map<Long, NSSummary> workerMap) throws InterruptedException {
    flushQueue.put(workerMap);
    LOG.debug("{}: Submitted map with {} entries, queue size now {}", taskName, workerMap.size(), flushQueue.size());
  }
  
  /**
   * Background thread loop that processes flush queue.
   */
  private void flushLoop() {
    while (running.get()) {
      try {
        Map<Long, NSSummary> workerMap = flushQueue.poll(1, TimeUnit.SECONDS);
        
        if (workerMap == null) {
          continue;  // Timeout, check running flag
        }
        
        if (workerMap == SHUTDOWN_SIGNAL) {
          LOG.info("{}: Received shutdown signal", taskName);
          break;
        }
        
        // Process this batch
        LOG.debug("{}: Background thread processing batch with {} entries", taskName, workerMap.size());
        flushWithPropagation(workerMap);
        LOG.debug("{}: Background thread finished batch", taskName);
        
      } catch (InterruptedException e) {
        LOG.info("{}: Flusher thread interrupted", taskName);
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        LOG.error("{}: Error in flush loop", taskName, e);
        // Continue processing other batches
      }
    }
    LOG.info("{}: Async flusher stopped", taskName);
  }
  
  /**
   * Flush worker map with propagation to ancestors.
   */
  private void flushWithPropagation(Map<Long, NSSummary> workerMap) throws IOException {
    LOG.debug("{}: Flush starting with {} entries", taskName, workerMap.size());
    Map<Long, NSSummary> mergedMap = new HashMap<>();
    
    // For each immediate parent in worker map
    for (Map.Entry<Long, NSSummary> entry : workerMap.entrySet()) {
      long immediateParentId = entry.getKey();
      NSSummary delta = entry.getValue();
      
      // Get actual parent (check merged map first, then DB)
      NSSummary actualParent = mergedMap.get(immediateParentId);
      if (actualParent == null) {
        actualParent = reconNamespaceSummaryManager.getNSSummary(immediateParentId);
      }
      
      if (actualParent == null) {
        // Parent doesn't exist in DB yet - use delta as base (has metadata like dirName, parentId)
        actualParent = delta;
      } else {
        // Parent exists in DB - merge delta into it
        actualParent.setNumOfFiles(actualParent.getNumOfFiles() + delta.getNumOfFiles());
        actualParent.setSizeOfFiles(actualParent.getSizeOfFiles() + delta.getSizeOfFiles());
        actualParent.setReplicatedSizeOfFiles(actualParent.getReplicatedSizeOfFiles() + delta.getReplicatedSizeOfFiles());
        
        // Merge file size buckets
        int[] actualBucket = actualParent.getFileSizeBucket();
        int[] deltaBucket = delta.getFileSizeBucket();
        for (int i = 0; i < actualBucket.length; i++) {
          actualBucket[i] += deltaBucket[i];
        }
        actualParent.setFileSizeBucket(actualBucket);
        
        // Merge child dirs
        actualParent.getChildDir().addAll(delta.getChildDir());
      }

      // Store updated ACTUAL parent in merged map
      mergedMap.put(immediateParentId, actualParent);
      
      // Propagate delta to ancestors (grandparent, great-grandparent, etc.)
      propagateDeltaToAncestors(actualParent.getParentId(), delta, mergedMap);
    }
    
    // Write merged map to DB (simple batch write, no more R-M-W needed)
    writeToDb(mergedMap);
    LOG.debug("{}: Flush completed, wrote {} entries", taskName, mergedMap.size());
  }
  
  /**
   * Recursively propagate delta values up the ancestor chain.
   * Pattern: Check merged map first (for updates in this batch), then DB, for ACTUAL value.
   */
  private void propagateDeltaToAncestors(long ancestorId, NSSummary delta, 
                                          Map<Long, NSSummary> mergedMap) throws IOException {
    // Base case: reached root
    if (ancestorId == 0) {
      return;
    }
    
    // Get ACTUAL ancestor (check merged map first for most up-to-date, then DB)
    NSSummary actualAncestor = mergedMap.get(ancestorId);
    if (actualAncestor == null) {
      actualAncestor = reconNamespaceSummaryManager.getNSSummary(ancestorId);
      if (actualAncestor == null) {
        // Ancestor not in DB yet, create new
        actualAncestor = new NSSummary();
      }
    }
    
    // Apply DELTA to ACTUAL ancestor
    actualAncestor.setNumOfFiles(actualAncestor.getNumOfFiles() + delta.getNumOfFiles());
    actualAncestor.setSizeOfFiles(actualAncestor.getSizeOfFiles() + delta.getSizeOfFiles());
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
    
    // Wait for queue to drain before shutting down
    long waitStart = System.currentTimeMillis();
    while (!flushQueue.isEmpty() && (System.currentTimeMillis() - waitStart) < 10000) {
      try {
        Thread.sleep(100);  // Give background thread time to process
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    
    running.set(false);
    
    try {
      // Send shutdown signal
      flushQueue.offer(SHUTDOWN_SIGNAL, 5, TimeUnit.SECONDS);
      
      // Wait for background thread to finish
      backgroundFlusher.join(30000);  // 30 second timeout
      
      if (backgroundFlusher.isAlive()) {
        LOG.warn("{}: Background flusher did not stop gracefully, interrupting", taskName);
        backgroundFlusher.interrupt();
      }
      
      // Flush any remaining items
      int remainingCount = flushQueue.size();
      LOG.info("{}: Draining remaining {} items from queue", taskName, remainingCount);
      Map<Long, NSSummary> remaining;
      int processed = 0;
      while ((remaining = flushQueue.poll()) != null) {
        if (remaining != SHUTDOWN_SIGNAL) {
          try {
            LOG.info("{}: Draining item {} with {} entries", taskName, ++processed, remaining.size());
            flushWithPropagation(remaining);
            LOG.info("{}: Successfully flushed item {}", taskName, processed);
          } catch (Exception e) {
            LOG.error("{}: Error flushing item {} during drain", taskName, processed, e);
            // Continue draining other items
          }
        }
      }
      LOG.info("{}: Finished draining, processed {} of {} items", taskName, processed, remainingCount);
      
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while shutting down async flusher", e);
    }
    
    LOG.info("{}: Async flusher shut down complete", taskName);
  }
}

