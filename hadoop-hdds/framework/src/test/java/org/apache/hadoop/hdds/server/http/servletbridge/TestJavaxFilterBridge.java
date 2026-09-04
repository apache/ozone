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

package org.apache.hadoop.hdds.server.http.servletbridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.security.Principal;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the javax->jakarta servlet filter bridge, covering the two
 * behaviours the hadoop-auth filters rely on: passing an authenticated request
 * downstream, and short-circuiting the chain when authentication is refused.
 */
class TestJavaxFilterBridge {

  private static final String AUTH_HEADER = "Authorization";

  @Test
  void authenticatedRequestIsPassedDownstreamWithPrincipal() throws Exception {
    HttpServletRequest jakartaRequest = mock(HttpServletRequest.class);
    HttpServletResponse jakartaResponse = mock(HttpServletResponse.class);
    when(jakartaRequest.getHeader(AUTH_HEADER)).thenReturn("Negotiate token");

    // A javax filter that "authenticates" by wrapping the request with a principal.
    javax.servlet.Filter delegate = new AbstractJavaxFilter() {
      @Override
      public void doFilter(javax.servlet.ServletRequest req, javax.servlet.ServletResponse resp,
          javax.servlet.FilterChain chain) throws java.io.IOException, javax.servlet.ServletException {
        javax.servlet.http.HttpServletRequest httpReq = (javax.servlet.http.HttpServletRequest) req;
        assertEquals("Negotiate token", httpReq.getHeader(AUTH_HEADER));
        javax.servlet.http.HttpServletRequestWrapper wrapped =
            new javax.servlet.http.HttpServletRequestWrapper(httpReq) {
              @Override
              public String getRemoteUser() {
                return "alice";
              }

              @Override
              public Principal getUserPrincipal() {
                return () -> "alice";
              }

              @Override
              public String getAuthType() {
                return "KERBEROS";
              }
            };
        chain.doFilter(wrapped, resp);
      }
    };

    AtomicReference<HttpServletRequest> downstream = new AtomicReference<>();
    jakarta.servlet.FilterChain jakartaChain = (req, resp) -> downstream.set((HttpServletRequest) req);

    new JavaxFilterBridge(delegate).doFilter(jakartaRequest, jakartaResponse, jakartaChain);

    HttpServletRequest seen = downstream.get();
    assertEquals("alice", seen.getRemoteUser(), "principal must be visible downstream");
    assertEquals("KERBEROS", seen.getAuthType());
    assertEquals("alice", seen.getUserPrincipal().getName());
  }

  @Test
  void refusedRequestShortCircuitsAndWritesResponse() throws Exception {
    HttpServletRequest jakartaRequest = mock(HttpServletRequest.class);
    HttpServletResponse jakartaResponse = mock(HttpServletResponse.class);
    when(jakartaRequest.getHeader(AUTH_HEADER)).thenReturn(null);

    // A javax filter that refuses the request without invoking the chain.
    javax.servlet.Filter delegate = new AbstractJavaxFilter() {
      @Override
      public void doFilter(javax.servlet.ServletRequest req, javax.servlet.ServletResponse resp,
          javax.servlet.FilterChain chain) throws java.io.IOException {
        javax.servlet.http.HttpServletResponse httpResp = (javax.servlet.http.HttpServletResponse) resp;
        httpResp.setHeader("WWW-Authenticate", "Negotiate");
        httpResp.sendError(javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED);
      }
    };

    AtomicReference<HttpServletRequest> downstream = new AtomicReference<>();
    jakarta.servlet.FilterChain jakartaChain = (req, resp) -> downstream.set((HttpServletRequest) req);

    new JavaxFilterBridge(delegate).doFilter(jakartaRequest, jakartaResponse, jakartaChain);

    assertNull(downstream.get(), "chain must not be invoked when auth is refused");
    verify(jakartaResponse).setHeader("WWW-Authenticate", "Negotiate");
    verify(jakartaResponse).sendError(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  void cookiesRoundTripAcrossNamespaces() {
    javax.servlet.http.Cookie javax = new javax.servlet.http.Cookie("hadoop.auth", "signed");
    javax.setPath("/");
    javax.setDomain("example.com");
    javax.setMaxAge(120);
    javax.setSecure(true);
    javax.setHttpOnly(true);

    jakarta.servlet.http.Cookie jakarta = ServletBridgeUtils.toJakarta(javax);
    assertEquals("hadoop.auth", jakarta.getName());
    assertEquals("signed", jakarta.getValue());
    assertEquals("/", jakarta.getPath());
    assertEquals("example.com", jakarta.getDomain());
    assertEquals(120, jakarta.getMaxAge());
    assertTrue(jakarta.getSecure());
    assertTrue(jakarta.isHttpOnly());

    javax.servlet.http.Cookie back = ServletBridgeUtils.toJavax(jakarta);
    assertEquals("hadoop.auth", back.getName());
    assertEquals("signed", back.getValue());
    assertEquals("/", back.getPath());
    assertEquals("example.com", back.getDomain());
    assertEquals(120, back.getMaxAge());
    assertTrue(back.getSecure());
    assertTrue(back.isHttpOnly());
    assertFalse(back.getName().isEmpty());
  }

  /** Minimal javax filter with no-op lifecycle so tests only override doFilter. */
  private abstract static class AbstractJavaxFilter implements javax.servlet.Filter {
    @Override
    public void init(javax.servlet.FilterConfig filterConfig) {
    }

    @Override
    public void destroy() {
    }
  }
}
