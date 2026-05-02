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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DN_METRICS_COLLECTION_MINIMUM_API_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DN_METRICS_COLLECTION_MINIMUM_API_DELAY_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DN_METRICS_COLLECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DN_METRICS_COLLECTION_TIMEOUT_DEFAULT;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.apache.hadoop.ozone.recon.api.types.DataNodeMetricsServiceResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodePendingDeletionMetrics;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.tasks.DataNodeMetricsCollectionTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for collecting and managing DataNode pending deletion metrics.
 * Collects metrics asynchronously from all datanodes and provides aggregated results.
 */
@Singleton
public class DataNodeMetricsService {
  
  private static final Logger LOG = LoggerFactory.getLogger(DataNodeMetricsService.class);
  private static final int MAX_POOL_SIZE = 500;
  private static final int KEEP_ALIVE_TIME = 5;
  private static final int POLL_INTERVAL_MS = 200;

  private final ThreadPoolExecutor executorService;
  private final ReconNodeManager reconNodeManager;
  private final boolean httpsEnabled;
  private final int minimumApiDelayMs;
  private final MetricsServiceProviderFactory metricsServiceProviderFactory;
  private final int maximumTaskTimeout;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  
  private MetricCollectionStatus currentStatus = MetricCollectionStatus.NOT_STARTED;
  private List<DatanodePendingDeletionMetrics> pendingDeletionList;
  private Long totalPendingDeletion = 0L;
  private int totalNodesQueried;
  private int totalNodesFailed;
  private AtomicLong lastCollectionEndTime = new AtomicLong(0L);

  @Inject
  public DataNodeMetricsService(
      OzoneStorageContainerManager reconSCM,
      OzoneConfiguration config,
      MetricsServiceProviderFactory metricsServiceProviderFactory) {
    
    this.reconNodeManager = (ReconNodeManager) reconSCM.getScmNodeManager();
    this.httpsEnabled = HttpConfig.getHttpPolicy(config).isHttpsEnabled();
    this.minimumApiDelayMs = (int) config.getTimeDuration(
        OZONE_RECON_DN_METRICS_COLLECTION_MINIMUM_API_DELAY,
        OZONE_RECON_DN_METRICS_COLLECTION_MINIMUM_API_DELAY_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.maximumTaskTimeout = (int) config.getTimeDuration(OZONE_RECON_DN_METRICS_COLLECTION_TIMEOUT,
        OZONE_RECON_DN_METRICS_COLLECTION_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    this.metricsServiceProviderFactory = metricsServiceProviderFactory;
    this.lastCollectionEndTime.set(-minimumApiDelayMs);
    int corePoolSize = Runtime.getRuntime().availableProcessors() * 2;
    this.executorService = new ThreadPoolExecutor(
        corePoolSize, MAX_POOL_SIZE,
        KEEP_ALIVE_TIME, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder()
            .setNameFormat("DataNodeMetricsCollector-%d")
            .build());
  }

  /**
   * Starts the metrics collection task if not already running and rate limit allows.
   */
  public void startTask() {
    // Check if already running
    if (!isRunning.compareAndSet(false, true)) {
      LOG.warn("Metrics collection already in progress, skipping");
      return;
    }
    
    // Check rate limit
    if (System.currentTimeMillis() - lastCollectionEndTime.get() < minimumApiDelayMs) {
      LOG.debug("Rate limit active, skipping collection (delay: {}ms)", minimumApiDelayMs);
      isRunning.set(false);
      return;
    }

    Set<DatanodeDetails> nodes = reconNodeManager.getNodeStats().keySet();
    if (nodes.isEmpty()) {
      LOG.warn("No datanodes found to query");
      resetState();
      currentStatus = MetricCollectionStatus.FINISHED;
      isRunning.set(false);
      return;
    }

    // Set status immediately before starting async collection
    currentStatus = MetricCollectionStatus.IN_PROGRESS;
    LOG.debug("Starting metrics collection for {} datanodes", nodes.size());
    
    // Run a collection asynchronously so status can be queried
    CompletableFuture.runAsync(() -> collectMetrics(nodes), executorService)
        .exceptionally(throwable -> {
          LOG.error("Metrics collection failed", throwable);
          synchronized (DataNodeMetricsService.this) {
            currentStatus = MetricCollectionStatus.FINISHED;
            isRunning.set(false);
          }
          return null;
        });
  }

  /**
   * Collects metrics from all datanodes. Processes completed tasks first, waits for all.
   */
  private void collectMetrics(Set<DatanodeDetails> nodes) {
    try {
      CollectionContext context = submitMetricsCollectionTasks(nodes);
      processCollectionFutures(context);
      updateFinalState(context);
    } catch (Exception e) {
      resetState();
      currentStatus = MetricCollectionStatus.FAILED;
      isRunning.set(false);
    }
  }

  /**
   * Submits metrics collection tasks for all given datanodes.
   * @return A context object containing tracking structures for the submitted futures.
   */
  private CollectionContext submitMetricsCollectionTasks(Set<DatanodeDetails> nodes) {
    // Initialize state
    List<DatanodePendingDeletionMetrics> results = new ArrayList<>(nodes.size());
    // Submit all collection tasks
    Map<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>> futures = new HashMap<>();

    long submissionTime = System.currentTimeMillis();
    for (DatanodeDetails node : nodes) {
      DataNodeMetricsCollectionTask task = new DataNodeMetricsCollectionTask(
          node, httpsEnabled, metricsServiceProviderFactory);
      DatanodePendingDeletionMetrics key = new DatanodePendingDeletionMetrics(
          node.getHostName(), node.getUuidString(), -1L); // -1 is used as placeholder/failed status
      futures.put(key, executorService.submit(task));
    }
    int totalQueried = futures.size();
    LOG.debug("Submitted {} collection tasks", totalQueried);
    return new CollectionContext(totalQueried, futures, submissionTime, results);
  }

  /**
   * Polls the submitted futures, enforcing timeouts and aggregating results until all are complete.
   */
  private void processCollectionFutures(CollectionContext context) {
    // Poll with timeout enforcement
    while (!context.futures.isEmpty()) {
      long currentTime = System.currentTimeMillis();
      Iterator<Map.Entry<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>>>
          iterator = context.futures.entrySet().iterator();
      boolean processedAny = false;
      while (iterator.hasNext()) {
        Map.Entry<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>> entry =
            iterator.next();
        DatanodePendingDeletionMetrics key = entry.getKey();
        Future<DatanodePendingDeletionMetrics> future = entry.getValue();
        // Check for timeout
        if (checkAndHandleTimeout(key, future, context, currentTime)) {
          iterator.remove();
          processedAny = true;
          continue;
        }
        // Check for completion
        if (future.isDone()) {
          handleCompletedFuture(key, future, context);
          iterator.remove();
          processedAny = true;
        }
      }
      // Sleep before the next poll only if there are remaining futures and nothing was processed
      if (!context.futures.isEmpty() && !processedAny) {
        try {
          Thread.sleep(POLL_INTERVAL_MS);
        } catch (InterruptedException e) {
          LOG.warn("Collection polling interrupted");
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  private boolean checkAndHandleTimeout(
      DatanodePendingDeletionMetrics key, Future<DatanodePendingDeletionMetrics> future,
      CollectionContext context, long currentTime) {
    long elapsedTime = currentTime - context.submissionTime;
    if (elapsedTime > maximumTaskTimeout && !future.isDone()) {
      LOG.warn("Task for datanode {} [{}] timed out after {}ms",
          key.getHostName(), key.getDatanodeUuid(), elapsedTime);
      future.cancel(true); // Interrupt the task
      context.failed++;
      context.results.add(key); // Add with -1 (failed)
      return true;
    }
    return false;
  }

  private void handleCompletedFuture(
      DatanodePendingDeletionMetrics key, Future<DatanodePendingDeletionMetrics> future,
      CollectionContext context) {
    try {
      DatanodePendingDeletionMetrics result = future.get();
      if (result.getPendingBlockSize() < 0) {
        context.failed++;
      } else {
        context.totalPending += result.getPendingBlockSize();
      }
      context.results.add(result);
      LOG.debug("Processed result from {}", key.getHostName());
    } catch (ExecutionException | InterruptedException e) {
      String errorType = e instanceof InterruptedException ? "interrupted" : "execution failed";
      LOG.error("Task {} for datanode {} [{}] failed",
          errorType, key.getHostName(), key.getDatanodeUuid(), e);
      context.failed++;
      context.results.add(key);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Atomically updates the class's shared state with the results from the collection context.
   */
  private void updateFinalState(CollectionContext context) {
    // Update shared state atomically
    synchronized (this) {
      // Sort by pendingBlockSize in descending order so highest values appear first
      context.results.sort(
          Comparator.comparingLong(DatanodePendingDeletionMetrics::getPendingBlockSize)
              .reversed()
      );
      pendingDeletionList = context.results;
      totalPendingDeletion = context.totalPending;
      totalNodesQueried = context.totalQueried;
      totalNodesFailed = context.failed;
      currentStatus = MetricCollectionStatus.FINISHED;
      isRunning.set(false);
      lastCollectionEndTime.set(System.currentTimeMillis());
    }

    LOG.debug("Metrics collection completed. Queried: {}, Failed: {}",
        context.totalQueried, context.failed);
  }

  /**
   * Resets the collection state.
   */
  private void resetState() {
    pendingDeletionList = new ArrayList<>();
    totalPendingDeletion = 0L;
    totalNodesQueried = 0;
    totalNodesFailed = 0;
  }

  public DataNodeMetricsServiceResponse getCollectedMetrics(Integer limit) {
    startTask();
    if (currentStatus == MetricCollectionStatus.FINISHED) {
      DataNodeMetricsServiceResponse.Builder dnMetricsBuilder = DataNodeMetricsServiceResponse.newBuilder();
      dnMetricsBuilder
          .setStatus(currentStatus)
          .setTotalPendingDeletionSize(totalPendingDeletion)
          .setTotalNodesQueried(totalNodesQueried)
          .setTotalNodeQueryFailures(totalNodesFailed);

      if (null == limit) {
        return dnMetricsBuilder.setPendingDeletion(pendingDeletionList).build();
      } else {
        return dnMetricsBuilder.setPendingDeletion(
            pendingDeletionList.subList(0, Math.min(limit, pendingDeletionList.size())
        )).build();
      }
    }
    return DataNodeMetricsServiceResponse.newBuilder()
        .setStatus(currentStatus)
        .build();
  }

  @PreDestroy
  public void shutdown() {
    LOG.info("Shutting down DataNodeMetricsService");
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Status of metric collection task.
   */
  public enum MetricCollectionStatus {
    NOT_STARTED, IN_PROGRESS, FINISHED, FAILED
  }

  private static class CollectionContext {
    private final int totalQueried;
    private final Map<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>> futures;
    private final List<DatanodePendingDeletionMetrics> results;
    private final long submissionTime;
    private long totalPending = 0L;
    private int failed = 0;

    CollectionContext(
        int totalQueried,
        Map<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>> futures,
        long submissionTime,
        List<DatanodePendingDeletionMetrics> results) {
      this.totalQueried = totalQueried;
      this.futures = futures;
      this.submissionTime = submissionTime;
      this.results = results;
    }
  }
}
