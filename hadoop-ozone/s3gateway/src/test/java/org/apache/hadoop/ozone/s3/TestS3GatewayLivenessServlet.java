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

package org.apache.hadoop.ozone.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.conf.S3GatewayHealthCheckConfig;
import org.apache.hadoop.ozone.s3.S3GatewayWebAdminServer.LivenessServlet;
import org.apache.hadoop.ozone.s3.S3GatewayWebAdminServer.ReadinessServlet;
import org.junit.jupiter.api.Test;

/**
 * Tests for the S3 Gateway health endpoints and their config toggle.
 */
public class TestS3GatewayLivenessServlet {

  @Test
  public void livenessReturnsOk() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(body));

    new LivenessServlet().doGet(request, response);

    verify(response).setStatus(HttpServletResponse.SC_OK);
    assertEquals("OK", body.toString().trim());
  }

  @Test
  public void readinessReturnsOkWhenReady() throws Exception {
    StringWriter body = new StringWriter();
    HttpServletResponse response = invokeReadiness(readyProbe(true), body);
    verify(response).setStatus(HttpServletResponse.SC_OK);
    assertEquals("READY", body.toString().trim());
  }

  @Test
  public void readinessReturns503WhenNotReady() throws Exception {
    StringWriter body = new StringWriter();
    HttpServletResponse response = invokeReadiness(readyProbe(false), body);
    verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    assertEquals("NOT READY", body.toString().trim());
  }

  @Test
  public void readinessReturns503WhenProbeMissing() throws Exception {
    HttpServletResponse response = invokeReadiness(null, new StringWriter());
    verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
  }

  @Test
  public void readinessStartsNotReady() {
    OzoneConfiguration conf = new OzoneConfiguration();
    S3GatewayReadinessProbe probe = new S3GatewayReadinessProbe(conf,
        conf.getObject(S3GatewayHealthCheckConfig.class));
    try {
      assertFalse(probe.isReady());
    } finally {
      probe.close();
    }
  }

  @Test
  public void healthCheckEnabledByDefault() {
    OzoneConfiguration conf = new OzoneConfiguration();
    assertTrue(conf.getObject(S3GatewayHealthCheckConfig.class).isEnabled());
  }

  @Test
  public void healthCheckCanBeDisabled() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("ozone.s3g.health-check.enabled", false);
    assertFalse(conf.getObject(S3GatewayHealthCheckConfig.class).isEnabled());
  }

  private static S3GatewayReadinessProbe readyProbe(boolean ready) {
    S3GatewayReadinessProbe probe = mock(S3GatewayReadinessProbe.class);
    when(probe.isReady()).thenReturn(ready);
    return probe;
  }

  private static HttpServletResponse invokeReadiness(
      S3GatewayReadinessProbe probe, StringWriter body) throws Exception {
    ReadinessServlet servlet = spy(new ReadinessServlet());
    ServletContext context = mock(ServletContext.class);
    when(context.getAttribute(S3GatewayWebAdminServer.READINESS_PROBE_ATTRIBUTE))
        .thenReturn(probe);
    doReturn(context).when(servlet).getServletContext();

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(body));

    servlet.doGet(request, response);
    return response;
  }
}
