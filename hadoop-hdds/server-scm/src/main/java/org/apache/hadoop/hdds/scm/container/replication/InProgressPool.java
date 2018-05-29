/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodePoolManager;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerInfo;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * These are pools that are actively checking for replication status of the
 * containers.
 */
public final class InProgressPool {
  public static final Logger LOG =
      LoggerFactory.getLogger(InProgressPool.class);

  private final PeriodicPool pool;
  private final NodeManager nodeManager;
  private final NodePoolManager poolManager;
  private final ExecutorService executorService;
  private final Map<Long, Integer> containerCountMap;
  private final Map<UUID, Boolean> processedNodeSet;
  private final long startTime;
  private ProgressStatus status;
  private AtomicInteger nodeCount;
  private AtomicInteger nodeProcessed;
  private AtomicInteger containerProcessedCount;
  private long maxWaitTime;
  /**
   * Constructs an pool that is being processed.
   *  @param maxWaitTime - Maximum wait time in milliseconds.
   * @param pool - Pool that we are working against
   * @param nodeManager - Nodemanager
   * @param poolManager - pool manager
   * @param executorService - Shared Executor service.
   */
  InProgressPool(long maxWaitTime, PeriodicPool pool,
      NodeManager nodeManager, NodePoolManager poolManager,
                 ExecutorService executorService) {
    Preconditions.checkNotNull(pool);
    Preconditions.checkNotNull(nodeManager);
    Preconditions.checkNotNull(poolManager);
    Preconditions.checkNotNull(executorService);
    Preconditions.checkArgument(maxWaitTime > 0);
    this.pool = pool;
    this.nodeManager = nodeManager;
    this.poolManager = poolManager;
    this.executorService = executorService;
    this.containerCountMap = new ConcurrentHashMap<>();
    this.processedNodeSet = new ConcurrentHashMap<>();
    this.maxWaitTime = maxWaitTime;
    startTime = Time.monotonicNow();
  }

  /**
   * Returns periodic pool.
   *
   * @return PeriodicPool
   */
  public PeriodicPool getPool() {
    return pool;
  }

  /**
   * We are done if we have got reports from all nodes or we have
   * done waiting for the specified time.
   *
   * @return true if we are done, false otherwise.
   */
  public boolean isDone() {
    return (nodeCount.get() == nodeProcessed.get()) ||
        (this.startTime + this.maxWaitTime) > Time.monotonicNow();
  }

  /**
   * Gets the number of containers processed.
   *
   * @return int
   */
  public int getContainerProcessedCount() {
    return containerProcessedCount.get();
  }

  /**
   * Returns the start time in milliseconds.
   *
   * @return - Start Time.
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Get the number of nodes in this pool.
   *
   * @return - node count
   */
  public int getNodeCount() {
    return nodeCount.get();
  }

  /**
   * Get the number of nodes that we have already processed container reports
   * from.
   *
   * @return - Processed count.
   */
  public int getNodeProcessed() {
    return nodeProcessed.get();
  }

  /**
   * Returns the current status.
   *
   * @return Status
   */
  public ProgressStatus getStatus() {
    return status;
  }

  /**
   * Starts the reconciliation process for all the nodes in the pool.
   */
  public void startReconciliation() {
    List<DatanodeDetails> datanodeDetailsList =
        this.poolManager.getNodes(pool.getPoolName());
    if (datanodeDetailsList.size() == 0) {
      LOG.error("Datanode list for {} is Empty. Pool with no nodes ? ",
          pool.getPoolName());
      this.status = ProgressStatus.Error;
      return;
    }

    nodeProcessed = new AtomicInteger(0);
    containerProcessedCount = new AtomicInteger(0);
    nodeCount = new AtomicInteger(0);
    this.status = ProgressStatus.InProgress;
    this.getPool().setLastProcessedTime(Time.monotonicNow());
  }

  /**
   * Queues a container Report for handling. This is done in a worker thread
   * since decoding a container report might be compute intensive . We don't
   * want to block since we have asked for bunch of container reports
   * from a set of datanodes.
   *
   * @param containerReport - ContainerReport
   */
  public void handleContainerReport(DatanodeDetails datanodeDetails,
      ContainerReportsProto containerReport) {
    if (status == ProgressStatus.InProgress) {
      executorService.submit(processContainerReport(datanodeDetails,
          containerReport));
    } else {
      LOG.debug("Cannot handle container report when the pool is in {} status.",
          status);
    }
  }

  private Runnable processContainerReport(DatanodeDetails datanodeDetails,
      ContainerReportsProto reports) {
    return () -> {
      if (processedNodeSet.computeIfAbsent(datanodeDetails.getUuid(),
          (k) -> true)) {
        nodeProcessed.incrementAndGet();
        LOG.debug("Total Nodes processed : {} Node Name: {} ", nodeProcessed,
            datanodeDetails.getUuid());
        for (ContainerInfo info : reports.getReportsList()) {
          containerProcessedCount.incrementAndGet();
          LOG.debug("Total Containers processed: {} Container Name: {}",
              containerProcessedCount.get(), info.getContainerID());

          // Update the container map with count + 1 if the key exists or
          // update the map with 1. Since this is a concurrentMap the
          // computation and update is atomic.
          containerCountMap.merge(info.getContainerID(), 1, Integer::sum);
        }
      }
    };
  }

  /**
   * Filter the containers based on specific rules.
   *
   * @param predicate -- Predicate to filter by
   * @return A list of map entries.
   */
  public List<Map.Entry<Long, Integer>> filterContainer(
      Predicate<Map.Entry<Long, Integer>> predicate) {
    return containerCountMap.entrySet().stream()
        .filter(predicate).collect(Collectors.toList());
  }

  /**
   * Used only for testing, calling this will abort container report
   * processing. This is very dangerous call and should not be made by any users
   */
  @VisibleForTesting
  public void setDoneProcessing() {
    nodeProcessed.set(nodeCount.get());
  }

  /**
   * Returns the pool name.
   *
   * @return Name of the pool.
   */
  String getPoolName() {
    return pool.getPoolName();
  }

  public void finalizeReconciliation() {
    status = ProgressStatus.Done;
    //TODO: Add finalizing logic. This is where actual reconciliation happens.
  }

  /**
   * Current status of the computing replication status.
   */
  public enum ProgressStatus {
    InProgress, Done, Error
  }
}
