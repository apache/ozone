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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.security.Principal;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the javax->jakarta servlet filter bridge, covering the two
 * behaviours the hadoop-auth filters rely on: passing an authenticated request
 * downstream, and short-circuiting the chain when authentication is refused,
 * plus the boundary of what a delegate's forwarded request carries back into the
 * jakarta chain and the lifecycle calls Jetty makes on the bridge itself.
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

              @Override
              public boolean isUserInRole(String role) {
                return "admin".equals(role);
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
    // Roles are part of the authentication result a downstream authorization check reads, so the
    // delegate's answer -- not the unauthenticated original request's -- must be the one visible.
    assertTrue(seen.isUserInRole("admin"), "granted role must be visible downstream");
    assertFalse(seen.isUserInRole("guest"),
        "roles must come from the delegate, not be granted wholesale");
  }

  /**
   * The start-up allowlist in {@code ServletElementsFactory} admits subclasses of the four
   * bridgeable filters, because hadoop's own bridged filters are subclasses. A site-specific
   * subclass could therefore forward a request wrapper that overrides more than the principal,
   * and the bridge carries only the principal -- silently, unlike a forwarded response wrapper,
   * which fails the request loudly (see above).
   *
   * <p>This pins that boundary so the operator-facing description of it in
   * {@code ozone-default.xml} and the SecuringOzoneHTTP pages cannot drift from the bridge: the
   * principal crosses, a substituted header or added parameter does not, and an attribute the
   * delegate sets does -- because the javax view sets attributes straight on the jakarta request
   * rather than on a copy.
   */
  @Test
  void nonPrincipalOverridesOnForwardedRequestAreNotPropagated() throws Exception {
    HttpServletRequest jakartaRequest = mock(HttpServletRequest.class);
    HttpServletResponse jakartaResponse = mock(HttpServletResponse.class);
    when(jakartaRequest.getHeader(AUTH_HEADER)).thenReturn("Negotiate token");

    // A javax filter that authenticates and also rewrites a header and adds a parameter -- the
    // shape a custom subclass of an allowlisted filter could have.
    javax.servlet.Filter delegate = new AbstractJavaxFilter() {
      @Override
      public void doFilter(javax.servlet.ServletRequest req, javax.servlet.ServletResponse resp,
          javax.servlet.FilterChain chain) throws java.io.IOException, javax.servlet.ServletException {
        javax.servlet.http.HttpServletRequest httpReq = (javax.servlet.http.HttpServletRequest) req;
        httpReq.setAttribute("bridged.attribute", "kept");
        chain.doFilter(new javax.servlet.http.HttpServletRequestWrapper(httpReq) {
          @Override
          public String getRemoteUser() {
            return "alice";
          }

          @Override
          public String getHeader(String name) {
            return AUTH_HEADER.equals(name) ? "rewritten" : super.getHeader(name);
          }

          @Override
          public String getParameter(String name) {
            return "doas".equals(name) ? "bob" : super.getParameter(name);
          }
        }, resp);
      }
    };

    AtomicReference<HttpServletRequest> downstream = new AtomicReference<>();
    new JavaxFilterBridge(delegate).doFilter(jakartaRequest, jakartaResponse,
        (req, resp) -> downstream.set((HttpServletRequest) req));

    HttpServletRequest seen = downstream.get();
    assertEquals("alice", seen.getRemoteUser(), "the principal is what the bridge carries");
    assertEquals("Negotiate token", seen.getHeader(AUTH_HEADER),
        "a header the delegate's wrapper substitutes must not be propagated downstream");
    assertNull(seen.getParameter("doas"),
        "a parameter the delegate's wrapper adds must not be propagated downstream");
    verify(jakartaRequest).setAttribute("bridged.attribute", "kept");
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
  void responseWrappedByDelegateFailsRequest() {
    HttpServletRequest jakartaRequest = mock(HttpServletRequest.class);
    HttpServletResponse jakartaResponse = mock(HttpServletResponse.class);

    // A javax filter that wraps the response before forwarding the chain. The
    // bridge cannot carry that wrapper into the jakarta chain, so rather than
    // silently drop it the bridge must fail the request.
    javax.servlet.Filter delegate = new AbstractJavaxFilter() {
      @Override
      public void doFilter(javax.servlet.ServletRequest req, javax.servlet.ServletResponse resp,
          javax.servlet.FilterChain chain) throws java.io.IOException, javax.servlet.ServletException {
        javax.servlet.http.HttpServletResponseWrapper wrapped =
            new javax.servlet.http.HttpServletResponseWrapper(
                (javax.servlet.http.HttpServletResponse) resp) {
            };
        chain.doFilter(req, wrapped);
      }
    };

    AtomicReference<HttpServletResponse> downstream = new AtomicReference<>();
    jakarta.servlet.FilterChain jakartaChain =
        (req, resp) -> downstream.set((HttpServletResponse) resp);

    ServletException ex = assertThrows(ServletException.class, () ->
        new JavaxFilterBridge(delegate).doFilter(jakartaRequest, jakartaResponse, jakartaChain));
    assertTrue(ex.getMessage().contains("wrapped or replaced the response"),
        "exception must explain the dropped response wrapper");
    assertNull(downstream.get(),
        "downstream chain must not run when the delegate forwards a wrapped response");
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

  @Test
  void getCookiesSkipsNamesRejectedByJavax() {
    // jakarta.servlet 6 accepts these reserved names; the javax 3.1 Cookie
    // constructor the bridge converts into rejects them.
    jakarta.servlet.http.Cookie reservedVersion =
        new jakarta.servlet.http.Cookie("$Version", "1");
    jakarta.servlet.http.Cookie valid =
        new jakarta.servlet.http.Cookie("hadoop.auth", "signed");
    jakarta.servlet.http.Cookie reservedPath =
        new jakarta.servlet.http.Cookie("Path", "/");
    HttpServletRequest jakartaRequest = mock(HttpServletRequest.class);
    when(jakartaRequest.getCookies()).thenReturn(
        new jakarta.servlet.http.Cookie[] {reservedVersion, valid, reservedPath});

    javax.servlet.http.Cookie[] result =
        new JakartaToJavaxRequest(jakartaRequest).getCookies();

    assertEquals(1, result.length, "reserved-name cookies must be skipped");
    assertEquals("hadoop.auth", result[0].getName());
    assertEquals("signed", result[0].getValue());
  }

  /**
   * A javax filter can reach the servlet context through its request as well as through its
   * {@code FilterConfig}, and both must land on the same jakarta context. The hadoop-auth chain
   * hands the signer secret provider from {@code HttpServer2} to the filter as a context
   * <em>attribute</em>, so a request-side view that refused the call -- or that was backed by a
   * copy -- would break that handoff rather than merely omit a convenience.
   */
  @Test
  void requestServletContextSharesAttributesWithTheJakartaContext() {
    ServletContext jakartaContext = mock(ServletContext.class);
    when(jakartaContext.getAttribute(AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE))
        .thenReturn("provider");
    HttpServletRequest jakartaRequest = mock(HttpServletRequest.class);
    when(jakartaRequest.getServletContext()).thenReturn(jakartaContext);

    javax.servlet.ServletContext javaxContext =
        new JakartaToJavaxRequest(jakartaRequest).getServletContext();

    assertEquals("provider",
        javaxContext.getAttribute(AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE),
        "an attribute on the real context must be readable through the request's javax view");
    javaxContext.setAttribute("bridged.context.attribute", "written");
    verify(jakartaContext).setAttribute("bridged.context.attribute", "written");
  }

  /**
   * A javax filter that cannot initialize must surface as a jakarta
   * {@code ServletException}, which is what makes Jetty mark the context
   * unavailable and {@code HttpServer2.start()} fail. Swallowing it would let a
   * daemon come up serving requests through an un-initialized authentication
   * filter; the real case is {@code AuthenticationFilter} configured for kerberos
   * with a missing keytab.
   *
   * <p>The cause is kept, not just the message, because that is what reaches the
   * operator: {@code start()} wraps it in an {@code IOException} whose only
   * detail about the actual misconfiguration is this chain.
   */
  @Test
  void initFailureIsTranslatedToJakartaServletException() {
    javax.servlet.ServletException failure =
        new javax.servlet.ServletException("Keytab does not exist: /no/such.keytab");
    javax.servlet.Filter delegate = new AbstractJavaxFilter() {
      @Override
      public void init(javax.servlet.FilterConfig filterConfig) throws javax.servlet.ServletException {
        throw failure;
      }

      @Override
      public void doFilter(javax.servlet.ServletRequest req, javax.servlet.ServletResponse resp,
          javax.servlet.FilterChain chain) {
      }
    };

    ServletException ex = assertThrows(ServletException.class,
        () -> new JavaxFilterBridge(delegate).init(filterConfig()));

    assertEquals("Keytab does not exist: /no/such.keytab", ex.getMessage(),
        "the delegate's message must survive the namespace translation");
    assertSame(failure, ex.getCause(), "the javax exception must be kept as the cause");
  }

  /**
   * Jetty calls {@code destroy()} on the bridge, not on the delegate, so the
   * bridge has to forward it or the delegate's cleanup never runs -- for
   * {@code AuthenticationFilter} that is its signer secret provider.
   */
  @Test
  void destroyIsForwardedToDelegate() throws Exception {
    AtomicBoolean destroyed = new AtomicBoolean();
    javax.servlet.Filter delegate = new AbstractJavaxFilter() {
      @Override
      public void doFilter(javax.servlet.ServletRequest req, javax.servlet.ServletResponse resp,
          javax.servlet.FilterChain chain) {
      }

      @Override
      public void destroy() {
        destroyed.set(true);
      }
    };

    JavaxFilterBridge bridge = new JavaxFilterBridge(delegate);
    bridge.init(filterConfig());
    bridge.destroy();

    assertTrue(destroyed.get(), "delegate must be destroyed with the bridge");
  }

  /** A jakarta FilterConfig the bridge can wrap; only the context is dereferenced. */
  private static FilterConfig filterConfig() {
    FilterConfig filterConfig = mock(FilterConfig.class);
    when(filterConfig.getServletContext()).thenReturn(mock(ServletContext.class));
    return filterConfig;
  }

  /** Minimal javax filter with no-op lifecycle so tests only override doFilter. */
  private abstract static class AbstractJavaxFilter implements javax.servlet.Filter {
    @Override
    public void init(javax.servlet.FilterConfig filterConfig) throws javax.servlet.ServletException {
    }

    @Override
    public void destroy() {
    }
  }
}
