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

package org.apache.hadoop.ozone.recon;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.server.http.HttpConfig.getHttpPolicy;
import static org.apache.hadoop.http.HttpServer2.HTTPS_SCHEME;
import static org.apache.hadoop.http.HttpServer2.HTTP_SCHEME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersResponse;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class, used by integration tests,
 * for getting responses from Recon Endpoints.
 */
public final class TestReconEndpointUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestReconEndpointUtil.class);

  private static final String CONTAINER_ENDPOINT = "/api/v1/containers";
  private static final String OM_DB_SYNC_ENDPOINT = "/api/v1/triggerdbsync/om";

  private TestReconEndpointUtil() {
  }

  public static void triggerReconDbSyncWithOm(
      OzoneConfiguration conf) {
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(getReconWebAddress(conf))
        .append(OM_DB_SYNC_ENDPOINT);

    String response = "";
    try {
      response = makeHttpCall(conf, urlBuilder);
    } catch (Exception e) {
      LOG.error("Error getting db sync response from Recon");
    }

    if (!Strings.isNullOrEmpty(response) &&
        !response.equals("true")) {
      LOG.error("Triggering Recon DB sync with OM failed.");
    }
  }

  public static UnhealthyContainersResponse getUnhealthyContainersFromRecon(
      OzoneConfiguration conf, ContainerSchemaDefinition.UnHealthyContainerStates containerState)
      throws JsonProcessingException {
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(getReconWebAddress(conf))
        .append(CONTAINER_ENDPOINT)
        .append("/unhealthy/")
        .append(containerState);

    String containersResponse = "";
    try {
      containersResponse = makeHttpCall(conf, urlBuilder);
    } catch (Exception e) {
      LOG.error("Error getting unhealthy containers response from Recon");
    }

    final ObjectMapper objectMapper = new ObjectMapper();

    return objectMapper.readValue(containersResponse,
        UnhealthyContainersResponse.class);
  }

  public static String makeHttpCall(OzoneConfiguration conf, StringBuilder url)
      throws Exception {

    System.out.println("Connecting to Recon: " + url + " ...");
    final URLConnectionFactory connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(conf);

    boolean isSpnegoEnabled = isHTTPSEnabled(conf);
    HttpURLConnection httpURLConnection;

    try {
      httpURLConnection = (HttpURLConnection) connectionFactory.openConnection(
          new URL(url.toString()), isSpnegoEnabled);
      httpURLConnection.connect();
      int errorCode = httpURLConnection.getResponseCode();
      InputStream inputStream = httpURLConnection.getInputStream();

      if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
      }

      if (httpURLConnection.getErrorStream() != null) {
        System.out.println("Recon is being initialized. " +
                           "Please wait a moment");
        return null;
      } else {
        System.out.println("Unexpected null in http payload," +
                           " while processing request");
      }
      return null;
    } catch (ConnectException ex) {
      System.err.println("Connection Refused. Please make sure the " +
                         "Recon Server has been started.");
      return null;
    }
  }

  public static String getReconWebAddress(OzoneConfiguration conf) {
    final String protocol;
    final HttpConfig.Policy webPolicy = getHttpPolicy(conf);

    final boolean isHostDefault;
    String host;

    if (webPolicy.isHttpsEnabled()) {
      protocol = HTTPS_SCHEME;
      host = conf.get(OZONE_RECON_HTTPS_ADDRESS_KEY,
          OZONE_RECON_HTTPS_ADDRESS_DEFAULT);
      isHostDefault = getHostOnly(host).equals(
          getHostOnly(OZONE_RECON_HTTPS_ADDRESS_DEFAULT));
    } else {
      protocol = HTTP_SCHEME;
      host = conf.get(OZONE_RECON_HTTP_ADDRESS_KEY,
          OZONE_RECON_HTTP_ADDRESS_DEFAULT);
      isHostDefault = getHostOnly(host).equals(
          getHostOnly(OZONE_RECON_HTTP_ADDRESS_DEFAULT));
    }

    if (isHostDefault) {
      // Fallback to <Recon RPC host name>:<Recon http(s) address port>
      final String rpcHost =
          conf.get(OZONE_RECON_ADDRESS_KEY, OZONE_RECON_ADDRESS_DEFAULT);
      host = getHostOnly(rpcHost) + ":" + getPort(host);
    }

    return protocol + "://" + host;
  }

  public static String getHostOnly(String host) {
    return host.split(":", 2)[0];
  }

  public static String getPort(String host) {
    return host.split(":", 2)[1];
  }

  public static boolean isHTTPSEnabled(OzoneConfiguration conf) {
    return getHttpPolicy(conf) == HttpConfig.Policy.HTTPS_ONLY;
  }

}

