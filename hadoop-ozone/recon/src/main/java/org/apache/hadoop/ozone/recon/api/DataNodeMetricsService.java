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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.DataNodeMetricsServiceResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodePendingDeletionMetrics;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for collecting and managing DataNode metrics asynchronously.
 * Uses non-blocking HTTP client for efficient concurrent metric collection.
 */
@Singleton
public class DataNodeMetricsService {

  private static final Logger LOG = LoggerFactory.getLogger(DataNodeMetricsService.class);
  private static final String BEAN_NAME = "Hadoop:service=HddsDatanode,name=BlockDeletingService";
  private static final String KEY_NAME = "TotalPendingBlockBytes";
  private static final int DEFAULT_TIMEOUT = 60;

  private final ReconNodeManager reconNodeManager;
  private final boolean httpsEnabled;
  private final long minimumApiDelayMs;
  private final MetricsServiceProvider asyncServiceProvider;
  // Immutable state holder for thread-safe access
  private final AtomicReference<MetricsState> currentState =
      new AtomicReference<>(new MetricsState());

  @Inject
  public DataNodeMetricsService(
      OzoneStorageContainerManager reconSCM,
      OzoneConfiguration config,
      MetricsServiceProviderFactory metricsServiceProviderFactory) {

    this.reconNodeManager = (ReconNodeManager) reconSCM.getScmNodeManager();
    this.httpsEnabled = HttpConfig.getHttpPolicy(config).isHttpsEnabled();
    this.minimumApiDelayMs = config.getTimeDuration(
        OZONE_RECON_DN_METRICS_COLLECTION_MINIMUM_API_DELAY,
        OZONE_RECON_DN_METRICS_COLLECTION_MINIMUM_API_DELAY_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.asyncServiceProvider = metricsServiceProviderFactory.getJmxMetricsServiceProvider();
  }

  /**
   * Starts an asynchronous metrics collection task if one is not already running.
   * Returns immediately - use getCollectedMetrics() to check status and results.
   */
  public synchronized void startTask() {
    MetricsState state = currentState.get();
    // Check if already running
    if (state.status == MetricCollectionStatus.IN_PROGRESS) {
      LOG.warn("Metrics collection task is already in progress");
      return;
    }
    // Check rate limit
    if (state.lastCollectionEndTime > System.currentTimeMillis() - minimumApiDelayMs) {
      LOG.info("Skipping metrics collection due to rate limit ({}ms)", minimumApiDelayMs);
      return;
    }
    Set<DatanodeDetails> nodes = reconNodeManager.getNodeStats().keySet();
    if (nodes.isEmpty()) {
      LOG.warn("No datanodes found to query");
      currentState.set(new MetricsState(
          MetricCollectionStatus.SUCCEEDED,
          new ArrayList<>(),
          0L, 0, 0,
          System.currentTimeMillis()
      ));
      return;
    }
    LOG.info("Starting async metrics collection for {} datanodes", nodes.size());
    // Update state to IN_PROGRESS
    currentState.set(new MetricsState(
        MetricCollectionStatus.IN_PROGRESS,
        new ArrayList<>(),
        0L, 0, 0,
        state.lastCollectionEndTime
    ));
    collectMetricsAsync(nodes);
  }

  /**
   * Collects metrics from all datanodes asynchronously using CompletableFuture.
   */
  private void collectMetricsAsync(Set<DatanodeDetails> nodes) {
    int totalNodes = nodes.size();

    // Create futures for all nodes
    List<CompletableFuture<DatanodePendingDeletionMetrics>> futures = nodes.stream()
        .map(this::collectFromSingleDataNode)
        .collect(Collectors.toList());

    // Combine all futures
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .handle((result, throwable) -> {
          // Collect all completed results (including failures marked as -1)
          List<DatanodePendingDeletionMetrics> allResults = new ArrayList<>();
          long totalPendingDeletion = 0L;
          int failedCount = 0;

          for (CompletableFuture<DatanodePendingDeletionMetrics> future : futures) {
            try {
              // getNow with default will return the result if completed, or default if not
              DatanodePendingDeletionMetrics metrics = future.get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
              if (metrics != null) {
                allResults.add(metrics);
                if (metrics.getPendingBlockSize() < 0) {
                  failedCount++;
                } else {
                  totalPendingDeletion += metrics.getPendingBlockSize();
                }
              }
            } catch (Exception e) {
              LOG.debug("Failed to get result from future", e);
              failedCount++;
            }
          }

          // Update state with results
          currentState.set(new MetricsState(
              MetricCollectionStatus.SUCCEEDED,
              allResults,
              totalPendingDeletion,
              totalNodes,
              failedCount,
              System.currentTimeMillis()
          ));

          LOG.info("Metrics collection completed. Queried: {}, Failed: {}, Total pending: {} bytes",
              totalNodes, failedCount, totalPendingDeletion);

          return null;
        });
  }

  /**
   * Collects metrics from a single datanode asynchronously.
   */
  private CompletableFuture<DatanodePendingDeletionMetrics> collectFromSingleDataNode(
      DatanodeDetails nodeDetails) {

    String jmxUrl = buildJmxUrl(nodeDetails);
    String hostname = nodeDetails.getHostName();
    String uuid = nodeDetails.getUuidString();
    return asyncServiceProvider.getMetricsAsync(jmxUrl, BEAN_NAME)
        .thenApply(metrics -> {
          try {
            if (metrics == null || metrics.isEmpty()) {
              LOG.warn("No metrics returned from datanode {}", hostname);
              return new DatanodePendingDeletionMetrics(hostname, uuid, -1L);
            }
            Map<String, Object> deletionMetrics = ReconUtils.getMetricsData(metrics, BEAN_NAME);
            long pendingBlockSize = ReconUtils.extractMetricValue(
                deletionMetrics, KEY_NAME);
            LOG.debug("Successfully collected metrics from {}: {} bytes", hostname, pendingBlockSize);
            return new DatanodePendingDeletionMetrics(hostname, uuid, pendingBlockSize);
          } catch (Exception e) {
            LOG.error("Error parsing metrics from datanode {}", hostname, e);
            return new DatanodePendingDeletionMetrics(hostname, uuid, -1L);
          }
        })
        .exceptionally(throwable -> {
          LOG.error("Failed to collect metrics from datanode {}: {}", hostname, throwable.getMessage());
          return new DatanodePendingDeletionMetrics(hostname, uuid, -1L);
        });
  }

  /**
   * Builds the JMX URL for a datanode.
   */
  private String buildJmxUrl(DatanodeDetails nodeDetails) {
    String protocol = httpsEnabled ? "https" : "http";
    Name portName = httpsEnabled ?
        DatanodeDetails.Port.Name.HTTPS : DatanodeDetails.Port.Name.HTTP;
    return String.format("%s://%s:%d/jmx",
        protocol,
        nodeDetails.getHostName(),
        nodeDetails.getPort(portName).getValue());
  }

  /**
   * Returns the current metrics state and results.
   */
  public DataNodeMetricsServiceResponse getCollectedMetrics() {
    MetricsState state = currentState.get();
    return DataNodeMetricsServiceResponse.newBuilder()
        .setStatus(state.status)
        .setPendingDeletion(state.pendingDeletionList)
        .setTotalPendingDeletion(state.totalPendingDeletion)
        .setTotalNodesQueried(state.totalNodesQueried)
        .setTotalNodeQueryFailures(state.totalNodesFailed)
        .build();
  }

  /**
   * Returns the current collection status.
   */
  public MetricCollectionStatus getTaskStatus() {
    return currentState.get().status;
  }

  @PreDestroy
  public void shutdown() {
    LOG.info("Shutting down DataNodeMetricsService...");
    asyncServiceProvider.shutdown();
  }

  /**
   * Immutable state holder for thread-safe state management.
   */
  private static class MetricsState {
    private final MetricCollectionStatus status;
    private final List<DatanodePendingDeletionMetrics> pendingDeletionList;
    private final Long totalPendingDeletion;
    private final int totalNodesQueried;
    private final int totalNodesFailed;
    private final long lastCollectionEndTime;

    MetricsState() {
      this(MetricCollectionStatus.NOT_STARTED, Collections.emptyList(), 0L, 0, 0, 0);
    }

    MetricsState(MetricCollectionStatus status,
                 List<DatanodePendingDeletionMetrics> pendingDeletionList,
                 Long totalPendingDeletion,
                 int totalNodesQueried,
                 int totalNodesFailed,
                 long lastCollectionEndTime) {
      this.status = status;
      this.pendingDeletionList = pendingDeletionList;
      this.totalPendingDeletion = totalPendingDeletion;
      this.totalNodesQueried = totalNodesQueried;
      this.totalNodesFailed = totalNodesFailed;
      this.lastCollectionEndTime = lastCollectionEndTime;
    }
  }

  /**
   * Status of metric collection task.
   */
  public enum MetricCollectionStatus {
    SUCCEEDED, IN_PROGRESS, NOT_STARTED
  }
}
