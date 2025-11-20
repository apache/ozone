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
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Callable;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

/**
 * JmxMetricsCollectorTask is a Callable implementation that retrieves specific
 * JMX metrics from a given DataNode's HTTP JMX endpoint. It fetches the metrics
 * for a given service and metric name, parses the response JSON, and extracts
 * the desired metric value.
 *
 * This task is primarily designed to collect metrics in a concurrent manner and
 * return the results wrapped in a JmxMetricsCollectorTaskResult object, which
 * contains the DataNode details and the fetched metric value.
 */
public class JmxMetricsCollectorTask implements Callable<JmxMetricsCollectorTaskResult> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final int HTTP_CONNECT_TIMEOUT_MS = 5000;
  private static final int HTTP_SOCKET_TIMEOUT_MS = 10000;

  private final DatanodeDetails datanodeDetails;
  private final String componentName;
  private final String serviceName;
  private final String keyName;

  public JmxMetricsCollectorTask(DatanodeDetails datanodeDetails,
                                 String componentName, String serviceName, String keyName) {
    this.datanodeDetails = datanodeDetails;
    this.componentName = componentName;
    this.serviceName = serviceName;
    this.keyName = keyName;
  }

  @Override
  public JmxMetricsCollectorTaskResult call() throws Exception {
    return getMetricsFromDatanode();
  }

  private JmxMetricsCollectorTaskResult getMetricsFromDatanode() throws IOException {
    if (datanodeDetails == null) {
      throw new IOException("DataNode details are null");
    }
    // Construct metrics URL for DataNode JMX endpoint
    String metricsUrl = String.format("http://%s:%d/jmx?qry=Hadoop:service=%s,name=%s",
        datanodeDetails.getIpAddress(),
        datanodeDetails.getPort(DatanodeDetails.Port.Name.HTTP).getValue(),
        componentName,
        serviceName);
    HttpURLConnection connection = null;
    try {
      // Use standard Java HttpURLConnection (compatible with all HTTP implementations)
      URL url = new URL(metricsUrl);
      connection = (HttpURLConnection) url.openConnection();

      // Set timeouts to prevent indefinite blocking
      connection.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MS);
      connection.setReadTimeout(HTTP_SOCKET_TIMEOUT_MS);
      connection.setRequestMethod("GET");

      // Check HTTP response code
      int responseCode = connection.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new IOException(String.format(
            "Failed to fetch metrics from %s: HTTP %d",
            datanodeDetails.getIpAddress(), responseCode));
      }
      // Read response body
      String jsonResponse;
      try (InputStream in = connection.getInputStream()) {
        // Use Apache Commons IO for Java compatibility (works with Java 8+)
        byte[] responseBytes = IOUtils.toByteArray(in);
        jsonResponse = new String(responseBytes, "UTF-8");
      }
      // Parse and extract metric value
      return new JmxMetricsCollectorTaskResult(datanodeDetails, parseMetrics(jsonResponse, serviceName, keyName));
    } catch (IOException e) {
      // Return -1 to indicate error (caller should handle gracefully)
      return new JmxMetricsCollectorTaskResult(datanodeDetails, -1L);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private static long parseMetrics(String jsonResponse, String serviceName, String keyName)
      throws IOException {
    if (jsonResponse == null || jsonResponse.isEmpty()) {
      return -1L;
    }
    JsonNode root = OBJECT_MAPPER.readTree(jsonResponse);
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

  /**
   * Extract a specific metric value from a JMX bean node.
   *
   * @param beanNode The JSON node representing a JMX bean
   * @param keyName The metric key to extract
   * @return The metric value, or 0L if not found
   */
  private static long extractMetrics(JsonNode beanNode, String keyName) {
    return beanNode.path(keyName).asLong(0L);
  }
}
