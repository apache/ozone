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

package org.apache.hadoop.ozone.recon.api.handlers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parallel size calculator that uses ForkJoinPool for efficient subtree traversal.
 * This addresses the performance bottleneck in calculating disk usage for large
 * namespace hierarchies by leveraging work-stealing across multiple threads.
 */
public class ParallelSizeCalculator {
  
  private static final Logger LOG = LoggerFactory.getLogger(ParallelSizeCalculator.class);
  
  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final ForkJoinPool forkJoinPool;
  private final int parallelThreshold;
  private final AtomicLong totalRocksDBQueries;
  private final AtomicLong totalTasksExecuted;
  
  /**
   * Constructor for ParallelSizeCalculator.
   * @param reconNamespaceSummaryManager Manager for namespace summary operations
   * @param parallelism Number of threads in ForkJoinPool
   * @param threshold Minimum children count to trigger parallel processing
   */
  public ParallelSizeCalculator(ReconNamespaceSummaryManager reconNamespaceSummaryManager,
                                int parallelism, int threshold) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    
    // Validate parallelism parameter to prevent IllegalArgumentException
    if (parallelism <= 0) {
      int defaultParallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
      LOG.warn("Invalid parallelism value: {}. Using default of available processors: {}.", 
               parallelism, defaultParallelism);
      parallelism = defaultParallelism;
    }
    
    this.forkJoinPool = new ForkJoinPool(parallelism);
    this.parallelThreshold = threshold;
    this.totalRocksDBQueries = new AtomicLong(0);
    this.totalTasksExecuted = new AtomicLong(0);
  }
  
  /**
   * Calculate total size for a given object ID using parallel processing.
   * @param objectId The root object ID to calculate size for
   * @return Total size in bytes
   * @throws IOException If there's an error accessing the namespace summary
   */
  public long calculateTotalSize(long objectId) throws IOException {
    totalRocksDBQueries.set(0);
    totalTasksExecuted.set(0);
    
    try {
      long startTime = System.nanoTime();
      long result = forkJoinPool.invoke(new SizeCalculationTask(objectId));
      long endTime = System.nanoTime();
      
      long durationMs = (endTime - startTime) / 1_000_000;
      LOG.debug("Parallel size calculation completed in {}ms for objectId: {}, " +
                "total RocksDB queries: {}, total tasks: {}", 
                durationMs, objectId, totalRocksDBQueries.get(), totalTasksExecuted.get());
      
      return result;
    } catch (Exception e) {
      LOG.error("Error in parallel size calculation for objectId: {}", objectId, e);
      throw new IOException("Parallel size calculation failed", e);
    }
  }
  
  /**
   * Batch get NSSummary objects for better RocksDB performance.
   * @param objectIds List of object IDs to fetch
   * @return List of NSSummary objects (nulls for non-existent objects)
   */
  private List<NSSummary> batchGetNSSummaries(List<Long> objectIds) {
    List<NSSummary> summaries = new ArrayList<>();
    for (Long objectId : objectIds) {
      try {
        NSSummary summary = reconNamespaceSummaryManager.getNSSummary(objectId);
        summaries.add(summary);
        totalRocksDBQueries.incrementAndGet();
      } catch (IOException e) {
        LOG.warn("Failed to get NSSummary for objectId: {}", objectId, e);
        summaries.add(null);
      }
    }
    return summaries;
  }
  
  /**
   * RecursiveTask for calculating total size of a subtree.
   */
  private class SizeCalculationTask extends RecursiveTask<Long> {
    private final long objectId;
    
    SizeCalculationTask(long objectId) {
      this.objectId = objectId;
    }
    
    @Override
    protected Long compute() {
      totalTasksExecuted.incrementAndGet();
      
      try {
        NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
        totalRocksDBQueries.incrementAndGet();
        
        if (nsSummary == null) {
          return 0L;
        }
        
        long totalSize = nsSummary.getSizeOfFiles();
        Set<Long> childDirs = nsSummary.getChildDir();
        
        if (childDirs.isEmpty()) {
          return totalSize;
        }
        
        // Decide whether to process children in parallel or sequentially
        if (childDirs.size() >= parallelThreshold) {
          // Parallel processing using work-stealing
          List<SizeCalculationTask> childTasks = new ArrayList<>();
          for (long childId : childDirs) {
            childTasks.add(new SizeCalculationTask(childId));
          }
          
          // Fork all child tasks - they can be stolen by any thread
          invokeAll(childTasks);
          
          // Collect results from all child tasks
          for (SizeCalculationTask task : childTasks) {
            totalSize += task.join();
          }
        } else {
          // Sequential processing for small numbers of children
          for (long childId : childDirs) {
            totalSize += new SizeCalculationTask(childId).compute();
          }
        }
        
        return totalSize;
      } catch (IOException e) {
        LOG.warn("Failed to calculate size for objectId: {}", objectId, e);
        return 0L;
      }
    }
  }
  
  /**
   * Enhanced RecursiveTask that batches RocksDB queries for better performance.
   */
  private class BatchedSizeCalculationTask extends RecursiveTask<Long> {
    private final List<Long> objectIds;
    
    BatchedSizeCalculationTask(List<Long> objectIds) {
      this.objectIds = objectIds;
    }
    
    @Override
    protected Long compute() {
      totalTasksExecuted.incrementAndGet();
      
      if (objectIds.isEmpty()) {
        return 0L;
      }
      
      if (objectIds.size() == 1) {
        // Single object - use the regular computation
        return new SizeCalculationTask(objectIds.get(0)).compute();
      }
      
      // Batch fetch all NSSummaries
      List<NSSummary> summaries = batchGetNSSummaries(objectIds);
      
      long totalSize = 0L;
      List<Long> allChildIds = new ArrayList<>();
      
      // Process each summary and collect all child IDs
      for (int i = 0; i < objectIds.size(); i++) {
        NSSummary summary = summaries.get(i);
        if (summary != null) {
          totalSize += summary.getSizeOfFiles();
          allChildIds.addAll(summary.getChildDir());
        }
      }
      
      // Process all children in parallel if we have enough
      if (!allChildIds.isEmpty()) {
        if (allChildIds.size() >= parallelThreshold) {
          // Split children into batches for parallel processing
          int batchSize = Math.max(1, allChildIds.size() / forkJoinPool.getParallelism());
          List<BatchedSizeCalculationTask> childTasks = new ArrayList<>();
          
          for (int i = 0; i < allChildIds.size(); i += batchSize) {
            int end = Math.min(i + batchSize, allChildIds.size());
            childTasks.add(new BatchedSizeCalculationTask(allChildIds.subList(i, end)));
          }
          
          invokeAll(childTasks);
          
          for (BatchedSizeCalculationTask task : childTasks) {
            totalSize += task.join();
          }
        } else {
          // Sequential processing for small numbers of children
          for (long childId : allChildIds) {
            totalSize += new SizeCalculationTask(childId).compute();
          }
        }
      }
      
      return totalSize;
    }
  }
  
  /**
   * Calculate total size using batched approach for better RocksDB performance.
   * @param objectId The root object ID to calculate size for
   * @return Total size in bytes
   * @throws IOException If there's an error accessing the namespace summary
   */
  public long calculateTotalSizeBatched(long objectId) throws IOException {
    totalRocksDBQueries.set(0);
    totalTasksExecuted.set(0);
    
    try {
      long startTime = System.nanoTime();
      List<Long> rootIds = new ArrayList<>();
      rootIds.add(objectId);
      long result = forkJoinPool.invoke(new BatchedSizeCalculationTask(rootIds));
      long endTime = System.nanoTime();
      
      long durationMs = (endTime - startTime) / 1_000_000;
      LOG.debug("Batched parallel size calculation completed in {}ms for objectId: {}, " +
                "total RocksDB queries: {}, total tasks: {}", 
                durationMs, objectId, totalRocksDBQueries.get(), totalTasksExecuted.get());
      
      return result;
    } catch (Exception e) {
      LOG.error("Error in batched parallel size calculation for objectId: {}", objectId, e);
      throw new IOException("Batched parallel size calculation failed", e);
    }
  }
  
  /**
   * Get performance metrics for the last calculation.
   * @return Array containing [rocksDBQueries, tasksExecuted]
   */
  public long[] getPerformanceMetrics() {
    return new long[]{totalRocksDBQueries.get(), totalTasksExecuted.get()};
  }
  
  /**
   * Shutdown the ForkJoinPool.
   */
  public void shutdown() {
    if (!forkJoinPool.isShutdown()) {
      forkJoinPool.shutdown();
    }
  }
  
  /**
   * Get the parallelism level of the ForkJoinPool.
   * @return Number of threads in the pool
   */
  public int getParallelism() {
    return forkJoinPool.getParallelism();
  }
  
  /**
   * Get the parallel threshold setting.
   * @return Threshold for triggering parallel processing
   */
  public int getParallelThreshold() {
    return parallelThreshold;
  }

  /**
   * Get the ForkJoinPool instance for advanced operations.
   * @return The ForkJoinPool instance
   */
  public ForkJoinPool getForkJoinPool() {
    return forkJoinPool;
  }
} 