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
import java.util.stream.Stream;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests authorization behavior of {@link PrometheusServlet}.
 */
public class TestPrometheusServletAuthorization {

  private static final String SECURITY_TOKEN = "mytoken";
  private PrometheusServlet servlet;
  private PrometheusMetricsSink sink;

  @BeforeEach
  public void setup() throws Exception {
    sink = mock(PrometheusMetricsSink.class);
    servlet = new PrometheusServlet();
    ServletContext context = mock(ServletContext.class);
    when(context.getAttribute(PrometheusServlet.SECURITY_TOKEN))
        .thenReturn(SECURITY_TOKEN);
    when(context.getAttribute(BaseHttpServer.PROMETHEUS_SINK))
        .thenReturn(sink);
    ServletConfig config = mock(ServletConfig.class);
    when(config.getServletContext()).thenReturn(context);

    servlet.init(config);
  }

  /**
   * Prepare tests data.
   * @return Invalid authorization header values.
   */
  private static Stream<String> invalidAuthorizationHeaders() {
    return Stream.of(
        null,
        "Bearer",
        "Bearer ",
        "Bearer wrongToken"
    );
  }

  private HttpServletResponse invokeServlet(
      String authorizationHeader) throws Exception {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader("Authorization"))
        .thenReturn(authorizationHeader);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(resp.getWriter())
        .thenReturn(new PrintWriter(new StringWriter()));
    servlet.doGet(req, resp);
    return resp;
  }

  @Test
  public void testValidBearerTokenAllowsPrometheusAccess() throws Exception {
    HttpServletResponse resp =
        invokeServlet("Bearer mytoken");
    //verify writeMetrics() was called only once.
    verify(sink).writeMetrics(any());
    //verify setStatus(403) was never called.
    verify(resp, never())
        .setStatus(HttpServletResponse.SC_FORBIDDEN);
  }

  @ParameterizedTest(name = "Invalid Authorization header [{0}] must return 403")
  @MethodSource("invalidAuthorizationHeaders")
  public void testInvalidAuthorizationHeadersReturn403(
      String authorizationHeader) throws Exception {
    HttpServletResponse resp =
        invokeServlet(authorizationHeader);
    //verify setStatus(403) was called only once.
    verify(resp)
        .setStatus(HttpServletResponse.SC_FORBIDDEN);
    //verify writeMetrics() was never called.
    verify(sink, never()).writeMetrics(any());
  }
}
