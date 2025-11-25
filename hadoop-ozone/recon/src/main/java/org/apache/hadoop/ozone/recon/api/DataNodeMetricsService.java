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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
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
  private MetricCollectionStatus currentStatus = MetricCollectionStatus.NOT_STARTED;
  private Long totalPendingDeletion = 0L;
  private List<DatanodePendingDeletionMetrics> pendingDeletionList;
  private final ReconNodeManager reconNodeManager;

  @Inject
  public DataNodeMetricsService(OzoneStorageContainerManager reconSCM) {
    reconNodeManager = (ReconNodeManager) reconSCM.getScmNodeManager();
  }

  public void startTask() {
    Set<DatanodeDetails> nodes = reconNodeManager.getNodeStats().keySet();
    pendingDeletionList = new ArrayList<>();
    totalPendingDeletion = 0L;
    currentStatus = MetricCollectionStatus.IN_PROGRESS;
    ExecutorService executor = Executors.newFixedThreadPool(10);
    List<Future<DatanodePendingDeletionMetrics>> futures = new ArrayList<>();
    for (DatanodeDetails node : nodes) {
      String hostName = node.getHostName();
      String uuid = node.getUuidString();
      int port = node.getPort(DatanodeDetails.Port.Name.HTTP).getValue();
      DataNodeMetricsCollectionTask task = new DataNodeMetricsCollectionTask(hostName, port, uuid);
      futures.add(executor.submit(task));
    }
    boolean hasTimedOut = false;
    for (Future<DatanodePendingDeletionMetrics> future : futures) {
      try {
        DatanodePendingDeletionMetrics result = future.get(30, TimeUnit.SECONDS);
        totalPendingDeletion += result.getPendingBlockSize();
        pendingDeletionList.add(result);
      } catch (TimeoutException e) {
        hasTimedOut = true;
        LOG.error("Task timed out after " + 30 + " seconds: {}", e.getMessage());
      } catch (Exception e) {
        System.err.println("Task failed or was interrupted: " + e.getMessage());
      }
    }
    executor.shutdownNow();
    if (hasTimedOut) {
      currentStatus = MetricCollectionStatus.FAILED;
    } else {
      currentStatus = MetricCollectionStatus.SUCCEEDED;
    }
  }

  public DataNodeMetricsServiceResponse getCollectedMetrics() {
    if (currentStatus == MetricCollectionStatus.SUCCEEDED) {
      currentStatus = MetricCollectionStatus.NOT_STARTED;
      return DataNodeMetricsServiceResponse.newBuilder()
          .setStatus(MetricCollectionStatus.SUCCEEDED)
          .setPendingDeletion(pendingDeletionList)
          .setTotalPendingDeletion(totalPendingDeletion)
          .build();
    } else {
      DataNodeMetricsServiceResponse response =  DataNodeMetricsServiceResponse.newBuilder()
          .setStatus(currentStatus)
          .build();
      currentStatus = MetricCollectionStatus.NOT_STARTED;
      return response;
    }
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
    SUCCEEDED, FAILED, IN_PROGRESS, NOT_STARTED
  }
}
