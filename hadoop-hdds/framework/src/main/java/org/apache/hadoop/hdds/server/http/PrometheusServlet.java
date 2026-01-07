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

package org.apache.hadoop.hdds.server.http;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet to publish hadoop metrics in prometheus format.
 */
public class PrometheusServlet extends HttpServlet {

  public static final String SECURITY_TOKEN = "PROMETHEUS_SECURITY_TOKEN";
  public static final String BEARER = "Bearer";

  public PrometheusMetricsSink getPrometheusSink() {
    return
        (PrometheusMetricsSink) getServletContext().getAttribute(
            BaseHttpServer.PROMETHEUS_SINK);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String securityToken =
        (String) getServletContext().getAttribute(SECURITY_TOKEN);
    if (securityToken != null) {
      String authorizationHeader = req.getHeader("Authorization");
      if (authorizationHeader == null
          || !authorizationHeader.startsWith(BEARER)
          || authorizationHeader.length() <= BEARER.length()
          || !securityToken.equals(
              authorizationHeader.substring(BEARER.length() + 1))) {
        resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
        return;
      }
    }
    PrintWriter writer = resp.getWriter();
    getPrometheusSink().writeMetrics(writer);
    writer.write("\n\n#Dropwizard metrics\n\n");
    //print out dropwizard metrics used by ratis.
    TextFormat.write004(writer,
        CollectorRegistry.defaultRegistry.metricFamilySamples());
    writer.flush();
  }
}
