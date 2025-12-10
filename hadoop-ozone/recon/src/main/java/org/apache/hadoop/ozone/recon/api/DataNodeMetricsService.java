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
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DN_METRICS_COLLECTION_THREAD_COUNT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DN_METRICS_COLLECTION_THREAD_COUNT_DEFAULT;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 * The DataNodeMetricsService class is responsible for collecting and managing
 * metrics related to datanodes in an Ozone Recon environment. Specifically,
 * it gathers metrics about pending block deletions from the datanodes and
 * provides aggregated results.
 * This service tracks the status of metric collection tasks and provides
 * an interface to query the state and results of these tasks.
 * The metrics collection process involves communicating with each datanode,
 * fetching their pending deletion metrics, and aggregating the data.
 */

@Singleton
public class DataNodeMetricsService {
  private static final Logger LOG = LoggerFactory.getLogger(DataNodeMetricsService.class);
  private final ExecutorService executorService;
  private final ReconNodeManager reconNodeManager;
  private final boolean httpsEnabled;
  private final int minimumApiDelay;
  private final MetricsServiceProviderFactory metricsServiceProviderFactory;
  private MetricCollectionStatus currentStatus = MetricCollectionStatus.NOT_STARTED;
  private Long totalPendingDeletion = 0L;
  private List<DatanodePendingDeletionMetrics> pendingDeletionList;
  private int totalNodesQueried;
  private int totalNodesFailed;
  private long lastCollectionEndTime;
  private static final int REQUEST_TIMEOUT = 60000;

  @Inject
  public DataNodeMetricsService(
          OzoneStorageContainerManager reconSCM,
          OzoneConfiguration config,
          MetricsServiceProviderFactory metricsServiceProviderFactory) {
    reconNodeManager = (ReconNodeManager) reconSCM.getScmNodeManager();
    int threadCount = config.getInt(OZONE_RECON_DN_METRICS_COLLECTION_THREAD_COUNT,
        OZONE_RECON_DN_METRICS_COLLECTION_THREAD_COUNT_DEFAULT);
    minimumApiDelay = (int) config.getTimeDuration(OZONE_RECON_DN_METRICS_COLLECTION_MINIMUM_API_DELAY,
            OZONE_RECON_DN_METRICS_COLLECTION_MINIMUM_API_DELAY_DEFAULT, TimeUnit.MILLISECONDS);

    httpsEnabled = HttpConfig.getHttpPolicy(config).isHttpsEnabled();
    lastCollectionEndTime = 0;
    executorService = Executors.newFixedThreadPool(threadCount,
        new ThreadFactoryBuilder().setNameFormat("DataNodeMetricsCollectionTasksThread-%d")
            .build());
    this.metricsServiceProviderFactory = metricsServiceProviderFactory;
  }

  public void startTask() {
    if (currentStatus == MetricCollectionStatus.IN_PROGRESS) {
      LOG.warn("Metrics collection task is already in progress. Skipping new task.");
      return;
    }
    if (lastCollectionEndTime > System.currentTimeMillis() -  minimumApiDelay) {
      LOG.info("Skipping metrics collection task due to last collection time being more than {} seconds ago.",
              minimumApiDelay / 1000);
      return;
    }
    totalNodesFailed = 0;
    Set<DatanodeDetails> nodes = reconNodeManager.getNodeStats().keySet();

    if (nodes.isEmpty()) {
      LOG.warn("No datanodes found to query for metrics collection");
      initializeTaskState(0);
      currentStatus = MetricCollectionStatus.SUCCEEDED;
      return;
    }

    LOG.info("Starting metrics collection task for {} datanodes", nodes.size());
    initializeTaskState(nodes.size());
    Map<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>> dataNodeFutures =
        submitMetricsCollectionTasks(nodes);
    collectMetricsWithTimeout(dataNodeFutures, nodes.size());
    // Add any remaining unfinished tasks as failed entries
    addFailedEntries(dataNodeFutures);
    currentStatus = MetricCollectionStatus.SUCCEEDED;
    lastCollectionEndTime = System.currentTimeMillis();
    LOG.info("Metrics collection completed. Queried: {}, Failed: {}",
        totalNodesQueried, totalNodesFailed);
  }

  /**
   * Initializes the state for a new metrics collection task.
   */
  private void initializeTaskState(int nodeCount) {
    pendingDeletionList = new ArrayList<>(nodeCount);
    totalPendingDeletion = 0L;
    currentStatus = MetricCollectionStatus.IN_PROGRESS;
  }

  /**
   * Submits metrics collection tasks for all datanodes.
   */
  private Map<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>> submitMetricsCollectionTasks(
      Set<DatanodeDetails> nodes) {
    Map<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>> futures = new HashMap<>();
    for (DatanodeDetails node : nodes) {
      DataNodeMetricsCollectionTask task = new DataNodeMetricsCollectionTask(
              node, httpsEnabled, metricsServiceProviderFactory);
      DatanodePendingDeletionMetrics key = new DatanodePendingDeletionMetrics(
          node.getHostName(), node.getUuidString(), -1L);
      futures.put(key, executorService.submit(task));
    }

    totalNodesQueried = futures.size();
    LOG.debug("Submitted {} metrics collection tasks", totalNodesQueried);

    return futures;
  }

  /**
   * Collects metrics from completed tasks with a global timeout.
   * Uses iterator to safely remove entries while iterating.
   */
  private void collectMetricsWithTimeout(
      Map<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>> futures,
      int nodeCount) {
    // Calculate timeout: half of total request timeout for all nodes
    long maximumTaskRunningTimeMs = (long) REQUEST_TIMEOUT * nodeCount / 2;
    long startTime = System.currentTimeMillis();
    long pollIntervalMs = 200;
    while (!futures.isEmpty()) {
      if (hasTimedOut(startTime, maximumTaskRunningTimeMs)) {
        LOG.warn("Stopping metrics collection task due to timeout. " +
            "Remaining tasks: {}", futures.size());
        break;
      }
      processCompletedTasks(futures);
      sleepBetweenPolls(pollIntervalMs);
    }
  }

  /**
   * Checks if the task has exceeded the maximum allowed running time.
   */
  private boolean hasTimedOut(long startTime, long maximumTaskRunningTimeMs) {
    return (System.currentTimeMillis() - startTime) > maximumTaskRunningTimeMs;
  }

  /**
   * Processes all completed tasks and removes them from the futures map.
   * Uses iterator to avoid ConcurrentModificationException.
   */
  private void processCompletedTasks(
      Map<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>> futures) {
    Iterator<Map.Entry<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>>>
        iterator = futures.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>> entry =
          iterator.next();

      if (entry.getValue().isDone()) {
        processCompletedTask(entry.getKey(), entry.getValue());
        iterator.remove();
      }
    }
  }

  /**
   * Processes a single completed task and updates the metrics.
   */
  private void processCompletedTask(DatanodePendingDeletionMetrics key,
                                    Future<DatanodePendingDeletionMetrics> future) {
    try {
      DatanodePendingDeletionMetrics result = future.get(REQUEST_TIMEOUT, TimeUnit.SECONDS);
      if (result.getPendingBlockSize() < 0) {
        totalNodesFailed += 1;
      } else {
        totalPendingDeletion += result.getPendingBlockSize();
      }
      pendingDeletionList.add(result);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      logTaskFailure(key, e);
      totalNodesFailed += 1;
      // Add the key with -1 indicating failure
      pendingDeletionList.add(key);
    }
  }

  /**
   * Logs appropriate error message based on exception type.
   */
  private void logTaskFailure(DatanodePendingDeletionMetrics key, Exception e) {
    String errorType = getErrorType(e);
    LOG.error("Task {} for datanode {} [{}]: {}",
        errorType, key.getHostName(), key.getDatanodeUuid(), e.getMessage());
  }

  /**
   * Returns a human-readable error type description.
   */
  private String getErrorType(Exception e) {
    if (e instanceof ExecutionException) {
      return "execution failed";
    } else if (e instanceof InterruptedException) {
      return "interrupted";
    } else if (e instanceof TimeoutException) {
      return "timed out";
    }
    return "failed";
  }

  /**
   * Adds all remaining unfinished tasks as failed entries.
   */
  private void addFailedEntries(
      Map<DatanodePendingDeletionMetrics, Future<DatanodePendingDeletionMetrics>> futures) {
    for (DatanodePendingDeletionMetrics key : futures.keySet()) {
      pendingDeletionList.add(key);
      totalNodesFailed++;
    }
  }

  /**
   * Sleeps for the specified interval, handling interruptions gracefully.
   */
  private void sleepBetweenPolls(long intervalMs) {
    try {
      Thread.sleep(intervalMs);
    } catch (InterruptedException e) {
      LOG.warn("Polling sleep interrupted: {}", e.getMessage());
      Thread.currentThread().interrupt(); // Restore interrupt status
    }
  }

  public DataNodeMetricsServiceResponse getCollectedMetrics() {
    if (currentStatus == MetricCollectionStatus.NOT_STARTED || currentStatus == MetricCollectionStatus.IN_PROGRESS) {
      return DataNodeMetricsServiceResponse.newBuilder()
              .setStatus(MetricCollectionStatus.IN_PROGRESS)
              .build();
    }
    return DataNodeMetricsServiceResponse.newBuilder()
            .setStatus(currentStatus)
            .setPendingDeletion(pendingDeletionList)
            .setTotalPendingDeletion(totalPendingDeletion)
            .setTotalNodesQueries(totalNodesQueried)
            .setTotalNodeQueryFailures(totalNodesFailed)
            .build();
  }

  public MetricCollectionStatus getTaskStatus() {
    return currentStatus;
  }

  /**
   * Enum representing the status of a metric collection task.
   * This enum is used to describe the various stages in the lifecycle of
   * a metric collection operation.
   */
  public enum MetricCollectionStatus {
    SUCCEEDED, IN_PROGRESS, NOT_STARTED
  }

  @PreDestroy
  public void shutdown() {
    LOG.info("Shutting down DataNodeMetricsService executor...");
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
}
