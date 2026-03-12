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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.DatanodePendingDeletionMetrics;
import org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task for collecting pending deletion metrics from a DataNode using JMX.
 * This class implements the Callable interface and retrieves pending deletion
 * information (e.g., total pending block bytes) from a DataNode by invoking its
 * JMX endpoint. The metrics are parsed and encapsulated in the
 * {@link DatanodePendingDeletionMetrics} object.
 */
public class DataNodeMetricsCollectionTask implements Callable<DatanodePendingDeletionMetrics> {

  private static final Logger LOG = LoggerFactory.getLogger(DataNodeMetricsCollectionTask.class);
  private final DatanodeDetails nodeDetails;
  private final boolean httpsEnabled;
  private final MetricsServiceProvider metricsServiceProvider;
  private static final String BEAN_NAME = "Hadoop:service=HddsDatanode,name=BlockDeletingService";
  private static final String METRICS_KEY = "TotalPendingBlockBytes";

  public DataNodeMetricsCollectionTask(
      DatanodeDetails nodeDetails,
      boolean httpsEnabled,
      MetricsServiceProviderFactory factory) {
    this.nodeDetails = nodeDetails;
    this.httpsEnabled = httpsEnabled;
    this.metricsServiceProvider = factory.getJmxMetricsServiceProvider(getJmxMetricsUrl());
  }

  @Override
  public DatanodePendingDeletionMetrics call() {
    LOG.debug("Collecting pending deletion metrics from DataNode {}", nodeDetails.getHostName());
    try {
      List<Map<String, Object>> metrics = metricsServiceProvider.getMetrics(BEAN_NAME);
      if (metrics == null || metrics.isEmpty()) {
        return new DatanodePendingDeletionMetrics(
            nodeDetails.getHostName(), nodeDetails.getUuidString(), -1L);
      }
      Map<String, Object> deletionMetrics = ReconUtils.getMetricsData(metrics, BEAN_NAME);
      long pendingBlockSize = ReconUtils.extractLongMetricValue(deletionMetrics, METRICS_KEY);

      return new DatanodePendingDeletionMetrics(
          nodeDetails.getHostName(), nodeDetails.getUuidString(), pendingBlockSize);

    } catch (Exception e) {
      LOG.error("Failed to collect metrics from DataNode {}", nodeDetails.getHostName(), e);
      return new DatanodePendingDeletionMetrics(
          nodeDetails.getHostName(), nodeDetails.getUuidString(), -1L);
    }
  }

  private String getJmxMetricsUrl() {
    String protocol = httpsEnabled ? "https" : "http";
    Name portName = httpsEnabled ?  DatanodeDetails.Port.Name.HTTPS : DatanodeDetails.Port.Name.HTTP;
    return String.format("%s://%s:%d/jmx",
        protocol,
        nodeDetails.getHostName(),
        nodeDetails.getPort(portName).getValue());
  }
}
