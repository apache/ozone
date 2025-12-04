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

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.DatanodePendingDeletionMetrics;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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
  private final int httpConnectionTimeout;
  private final int httpSocketTimeout;
  private final boolean httpsEnabled;

  public DataNodeMetricsCollectionTask(Builder builder) {
    this.nodeDetails = builder.nodeDetails;
    this.httpConnectionTimeout = builder.httpConnectionTimeout;
    this.httpSocketTimeout = builder.httpSocketTimeout;
    this.httpsEnabled = builder.httpsEnabled;
  }

  @Override
  public DatanodePendingDeletionMetrics call() {

    LOG.debug("Collecting pending deletion metrics from DataNode {}", nodeDetails.getHostName());
    try (CloseableHttpClient httpClient = HttpClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectTimeout(httpConnectionTimeout)
            .setSocketTimeout(httpSocketTimeout)
            .build())
        .build()) {

      HttpGet httpGet = new HttpGet(getJmxMetricsUrl());
      try {
        HttpEntity entity = httpClient.execute(httpGet).getEntity();
        if (entity == null) {
          LOG.warn("Empty response received from DataNode {}", nodeDetails.getHostName());
          return new DatanodePendingDeletionMetrics(
              nodeDetails.getHostName(), nodeDetails.getUuidString(), -1L);
        }

        InputStream in = entity.getContent();
        if (in == null) {
          LOG.warn("No content stream available from DataNode {}", nodeDetails.getHostName());
          return new DatanodePendingDeletionMetrics(
              nodeDetails.getHostName(), nodeDetails.getUuidString(), -1L);
        }

        byte[] responseBytes = IOUtils.toByteArray(in);
        if (responseBytes.length == 0) {
          LOG.warn("Empty response body received from DataNode {}", nodeDetails.getHostName());
          return new DatanodePendingDeletionMetrics(
              nodeDetails.getHostName(), nodeDetails.getUuidString(), -1L);
        }
        String jsonResponse = new String(responseBytes, StandardCharsets.UTF_8);
        long metrics = ReconUtils.parseMetrics(
            jsonResponse, "BlockDeletingService", "TotalPendingBlockBytes");
        return new DatanodePendingDeletionMetrics(
            nodeDetails.getHostName(), nodeDetails.getUuidString(), metrics);
      } finally {
        httpGet.releaseConnection();
      }
    } catch (ConnectTimeoutException e) {
      LOG.error("Connection timeout while collecting metrics from DataNode {}", nodeDetails.getHostName(), e);
      return new DatanodePendingDeletionMetrics(
          nodeDetails.getHostName(), nodeDetails.getUuidString(), -1L);
    } catch (SocketTimeoutException e) {
      LOG.error("Socket timeout while collecting metrics from DataNode {}", nodeDetails.getHostName(), e);
      return new DatanodePendingDeletionMetrics(
          nodeDetails.getHostName(), nodeDetails.getUuidString(), -1L);
    } catch (IOException e) {
      LOG.error("IO error while collecting metrics from DataNode {}", nodeDetails.getHostName(), e);
      return new DatanodePendingDeletionMetrics(
          nodeDetails.getHostName(), nodeDetails.getUuidString(), -1L);
    } catch (Exception e) {
      LOG.error("Unexpected error while collecting metrics from DataNode {}", nodeDetails.getHostName(), e);
      return new DatanodePendingDeletionMetrics(
          nodeDetails.getHostName(), nodeDetails.getUuidString(), -1L);
    }
  }

  private String getJmxMetricsUrl() {
    String protocol = httpsEnabled ? "https" : "http";
    Name portName = httpsEnabled ?  DatanodeDetails.Port.Name.HTTPS : DatanodeDetails.Port.Name.HTTP;
    return String.format("%s://%s:%d/jmx?qry=Hadoop:service=HddsDatanode,name=BlockDeletingService",
        protocol, nodeDetails.getHostName(), nodeDetails.getPort(portName).getValue());
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class used to construct instances of {@link DataNodeMetricsCollectionTask}.
   * Provides methods to set configuration parameters for the task.
   */
  public static class Builder {
    private DatanodeDetails nodeDetails;
    private int httpConnectionTimeout;
    private int httpSocketTimeout;
    private boolean httpsEnabled;

    public Builder setNodeDetails(DatanodeDetails nodeDetails) {
      this.nodeDetails = nodeDetails;
      return this;
    }

    public Builder setHttpConnectionTimeout(int httpConnectionTimeout) {
      this.httpConnectionTimeout = httpConnectionTimeout;
      return this;
    }

    public Builder setHttpSocketTimeout(int httpSocketTimeout) {
      this.httpSocketTimeout = httpSocketTimeout;
      return this;
    }

    public Builder setHttpsEnabled(boolean httpsEnabled) {
      this.httpsEnabled = httpsEnabled;
      return this;
    }

    public DataNodeMetricsCollectionTask build() {
      return new DataNodeMetricsCollectionTask(this);
    }
  }
}
