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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.ozone.recon.api.types.DatanodePendingDeletionMetrics;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
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
  private final String host;
  private final int port;
  private final String nodeUuid;
  private static ObjectMapper objectMapper = new ObjectMapper();
  private final int httpConnectionTimeout;
  private final int httpSocketTimeout;

  public DataNodeMetricsCollectionTask(Builder builder) {
    this.host = builder.host;
    this.port = builder.port;
    this.nodeUuid = builder.nodeUuid;

    httpConnectionTimeout = builder.httpConnectionTimeout;
    httpSocketTimeout = builder.httpSocketTimeout;
  }

  @Override
  public DatanodePendingDeletionMetrics call() throws Exception {
    LOG.debug("Collecting pending deletion metrics from DataNode {}:{}", host, port);
    try (CloseableHttpClient httpClient = HttpClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectTimeout(httpConnectionTimeout)
            .setSocketTimeout(httpSocketTimeout)
            .build())
        .build()) {

      InputStream in = httpClient.execute(new HttpGet(getJmxMetricsUrl())).getEntity().getContent();
      byte[] responseBytes = IOUtils.toByteArray(in);
      String jsonResponse = new String(responseBytes, StandardCharsets.UTF_8);
      return new DatanodePendingDeletionMetrics(host, nodeUuid,
          parseMetrics(jsonResponse, "BlockDeletingService", "TotalPendingBlockBytes"));
    }
  }

  private static long parseMetrics(String jsonResponse, String serviceName, String keyName)
      throws IOException {
    if (jsonResponse == null || jsonResponse.isEmpty()) {
      return -1L;
    }
    JsonNode root = objectMapper.readTree(jsonResponse);
    JsonNode beans = root.get("beans");
    if (beans != null && beans.isArray()) {
      // Find the bean matching the service name
      for (JsonNode bean : beans) {
        String beanName = bean.path("name").asText("");
        if (beanName.contains(serviceName)) {
          // Extract and return the metric value from the bean
          return extractMetrics(bean, keyName);
        }
      }
    }
    return -1L;
  }

  private static long extractMetrics(JsonNode beanNode, String keyName) {
    return beanNode.path(keyName).asLong(0L);
  }

  private String getJmxMetricsUrl() {
    return String.format(
        "%s://%s:%d/jmx?qry=Hadoop:service=HddsDatanode,name=BlockDeletingService", "http", host, port);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class used to construct instances of {@link DataNodeMetricsCollectionTask}.
   * Provides methods to set configuration parameters for the task.
   */
  public static class Builder {
    private String host;
    private int port;
    private String nodeUuid;
    private int httpConnectionTimeout;
    private int httpSocketTimeout;

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setNodeUuid(String nodeUuid) {
      this.nodeUuid = nodeUuid;
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

    public DataNodeMetricsCollectionTask build() {
      return new DataNodeMetricsCollectionTask(this);
    }
  }
}
