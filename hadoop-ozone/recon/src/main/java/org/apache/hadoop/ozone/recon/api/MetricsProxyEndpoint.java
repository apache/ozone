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

import static org.apache.hadoop.ozone.recon.spi.impl.PrometheusServiceProviderImpl.PROMETHEUS_INSTANT_QUERY_API;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.PrometheusServiceProviderImpl;

/**
 * Endpoint to fetch metrics data from Prometheus HTTP endpoint.
 */
@Path("/metrics")
public class MetricsProxyEndpoint {

  private MetricsServiceProvider metricsServiceProvider;

  @Inject
  public MetricsProxyEndpoint(
      MetricsServiceProviderFactory metricsServiceProviderFactory) {
    this.metricsServiceProvider =
        metricsServiceProviderFactory.getMetricsServiceProvider();
  }

  /**
   * Return a response from the configured metrics endpoint.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{api}")
  public void getMetricsResponse(
      @DefaultValue (PROMETHEUS_INSTANT_QUERY_API) @PathParam("api") String api,
      @Context UriInfo uriInfo,
      @Context HttpServletResponse httpServletResponse
  ) throws Exception {
    if (metricsServiceProvider != null) {
      HttpURLConnection connection = metricsServiceProvider.getMetricsResponse(
          api, uriInfo.getRequestUri().getQuery());
      InputStream inputStream;
      if (Response.Status.fromStatusCode(connection.getResponseCode())
          .getFamily() == Response.Status.Family.SUCCESSFUL) {
        inputStream = connection.getInputStream();
      } else {
        // Throw a bad gateway error if HttpResponseCode is not 2xx
        httpServletResponse.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
        inputStream = connection.getErrorStream();
      }
      try (
          OutputStream outputStream =
              httpServletResponse.getOutputStream();
          ReadableByteChannel inputChannel =
              Channels.newChannel(inputStream);
          WritableByteChannel outputChannel =
              Channels.newChannel(outputStream)
        ) {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(16 * 1024);

        while (inputChannel.read(buffer) != -1) {
          buffer.flip();
          outputChannel.write(buffer);
          buffer.compact();
        }

        buffer.flip();

        while (buffer.hasRemaining()) {
          outputChannel.write(buffer);
        }
      } finally {
        inputStream.close();
      }
    } else {
      // Throw a Bad Gateway Error
      httpServletResponse.sendError(HttpServletResponse.SC_BAD_GATEWAY,
          "Metrics endpoint is not configured. Configure " +
              PrometheusServiceProviderImpl.getEndpointConfigKey() + " and " +
              "try again later.");
    }
  }
}
