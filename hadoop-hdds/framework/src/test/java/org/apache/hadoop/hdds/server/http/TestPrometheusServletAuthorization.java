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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test Prometheus Servlet.
 */
public class TestPrometheusServletAuthorization {
  @Test
  public void testValidBearerTokenAllowsPrometheusAccess() throws Exception {
    PrometheusServlet servlet = new PrometheusServlet();
    ServletContext context = mock(ServletContext.class);
    when(context.getAttribute(PrometheusServlet.SECURITY_TOKEN))
        .thenReturn("mytoken");
    PrometheusMetricsSink sink = mock(PrometheusMetricsSink.class);
    when(context.getAttribute(BaseHttpServer.PROMETHEUS_SINK))
        .thenReturn(sink);
    ServletConfig config = mock(ServletConfig.class);
    when(config.getServletContext()).thenReturn(context);
    servlet.init(config);

    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader("Authorization")).thenReturn("Bearer mytoken");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(resp.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    servlet.doGet(req, resp);
    verify(sink).writeMetrics(any());
    verify(resp, never()).setStatus(HttpServletResponse.SC_FORBIDDEN);
  }

  @ParameterizedTest(name = "Authorization header \"{0}\" must return 403")
  @ValueSource(strings = {"Bearer ", "Bearer wrongToken"})
  public void testInvalidAuthorizationHeaderReturns403(
      String authorizationHeader) throws Exception {
    PrometheusServlet servlet = new PrometheusServlet();
    ServletContext context = mock(ServletContext.class);
    when(context.getAttribute(PrometheusServlet.SECURITY_TOKEN))
        .thenReturn("mytoken");
    PrometheusMetricsSink sink = mock(PrometheusMetricsSink.class);
    when(context.getAttribute(BaseHttpServer.PROMETHEUS_SINK))
        .thenReturn(sink);
    ServletConfig config = mock(ServletConfig.class);
    when(config.getServletContext()).thenReturn(context);
    servlet.init(config);

    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader("Authorization")).thenReturn(authorizationHeader);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(resp.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    servlet.doGet(req, resp);
    verify(resp).setStatus(HttpServletResponse.SC_FORBIDDEN);
    verify(sink, never()).writeMetrics(any());
  }

  @Test
  public void testNullAuthorizationHeaderReturnsForbidden() throws Exception {
    PrometheusServlet servlet = new PrometheusServlet();
    ServletContext context = mock(ServletContext.class);
    when(context.getAttribute(PrometheusServlet.SECURITY_TOKEN))
        .thenReturn("mytoken");
    PrometheusMetricsSink sink = mock(PrometheusMetricsSink.class);
    when(context.getAttribute(BaseHttpServer.PROMETHEUS_SINK))
        .thenReturn(sink);
    ServletConfig config = mock(ServletConfig.class);
    when(config.getServletContext()).thenReturn(context);
    servlet.init(config);

    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader("Authorization")).thenReturn(null);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(resp.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    servlet.doGet(req, resp);
    verify(resp).setStatus(HttpServletResponse.SC_FORBIDDEN);
    verify(sink, never()).writeMetrics(any());
  }
}
