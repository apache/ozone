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

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Principal;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a {@code javax.servlet.Filter} inside a Jetty EE10 (jakarta) servlet
 * chain by wrapping the jakarta request/response as javax views for the
 * delegate, and re-presenting the delegate's authenticated request to the
 * downstream jakarta chain.
 *
 * <p>This is the single choke point that lets Ozone keep hadoop's
 * {@code javax.servlet}-based authentication filters (SPNEGO / Kerberos /
 * delegation token) as the source of truth while the rest of the HTTP stack
 * runs on jakarta. When the delegate authenticates and calls its chain, the
 * authenticated principal it established (remote user, user principal, auth
 * type) is overlaid onto the original jakarta request and passed downstream;
 * when the delegate short-circuits (for example writing a 401), the response
 * has already been written through to the jakarta response and the downstream
 * chain is not invoked.
 *
 * <p><b>Bridging scope.</b> Only the delegate's authentication result crosses
 * back into the jakarta chain: the four principal methods
 * ({@code getRemoteUser}, {@code getUserPrincipal}, {@code getAuthType},
 * {@code isUserInRole}) of the request the delegate forwards are overlaid onto
 * the original jakarta request. Any other request wrapping the delegate adds
 * (extra headers, parameters, attributes) and any response wrapper it forwards
 * are not propagated downstream. This matches hadoop's
 * {@code AuthenticationFilter} family and {@code StaticUserFilter}, which only
 * establish a principal; a javax filter that depends on wrapping the response
 * or on non-principal request overrides is not fully supported here. The bridge
 * logs a warning if the delegate forwards a response other than the javax view
 * it was given, since that wrapper is silently dropped.
 */
public class JavaxFilterBridge implements Filter {

  private static final Logger LOG =
      LoggerFactory.getLogger(JavaxFilterBridge.class);

  private final javax.servlet.Filter delegate;
  private final AtomicBoolean warnedResponseWrapped = new AtomicBoolean();

  public JavaxFilterBridge(javax.servlet.Filter delegate) {
    this.delegate = delegate;
  }

  /** The wrapped javax filter, exposed for configuration by the registrar. */
  public javax.servlet.Filter getDelegate() {
    return delegate;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    try {
      delegate.init(new JakartaToJavaxFilterConfig(filterConfig));
    } catch (javax.servlet.ServletException e) {
      throw new ServletException(e.getMessage(), e);
    }
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    if (!(request instanceof HttpServletRequest) || !(response instanceof HttpServletResponse)) {
      throw new ServletException("JavaxFilterBridge only supports HTTP requests");
    }
    final HttpServletRequest jakartaRequest = (HttpServletRequest) request;
    final HttpServletResponse jakartaResponse = (HttpServletResponse) response;

    JakartaToJavaxRequest javaxRequest = new JakartaToJavaxRequest(jakartaRequest);
    JakartaToJavaxResponse javaxResponse = new JakartaToJavaxResponse(jakartaResponse);

    javax.servlet.FilterChain javaxChain = (downstreamRequest, downstreamResponse) -> {
      // The delegate authenticated the request and may have wrapped it to carry
      // the principal. Overlay that principal onto the original jakarta request
      // and continue the jakarta chain.
      HttpServletRequest authenticated = jakartaRequest;
      if (downstreamRequest instanceof javax.servlet.http.HttpServletRequest) {
        authenticated = new AuthenticatedRequest(jakartaRequest,
            (javax.servlet.http.HttpServletRequest) downstreamRequest);
      }
      if (downstreamResponse != javaxResponse
          && warnedResponseWrapped.compareAndSet(false, true)) {
        LOG.warn("javax filter {} wrapped or replaced the response; the wrapper "
            + "is not propagated to the jakarta chain (only the authenticated "
            + "principal is bridged).", delegate.getClass().getName());
      }
      try {
        chain.doFilter(authenticated, jakartaResponse);
      } catch (ServletException e) {
        throw new javax.servlet.ServletException(e.getMessage(), e);
      }
    };

    try {
      delegate.doFilter(javaxRequest, javaxResponse, javaxChain);
    } catch (javax.servlet.ServletException e) {
      throw new ServletException(e.getMessage(), e);
    }
  }

  @Override
  public void destroy() {
    delegate.destroy();
  }

  /**
   * Jakarta request that carries the authentication result (remote user,
   * principal, auth type, roles) the delegate established on its javax request,
   * while delegating everything else to the original jakarta request.
   */
  private static final class AuthenticatedRequest extends HttpServletRequestWrapper {

    private final javax.servlet.http.HttpServletRequest authenticated;

    private AuthenticatedRequest(HttpServletRequest original,
        javax.servlet.http.HttpServletRequest authenticated) {
      super(original);
      this.authenticated = authenticated;
    }

    @Override
    public String getRemoteUser() {
      return authenticated.getRemoteUser();
    }

    @Override
    public Principal getUserPrincipal() {
      return authenticated.getUserPrincipal();
    }

    @Override
    public String getAuthType() {
      return authenticated.getAuthType();
    }

    @Override
    public boolean isUserInRole(String role) {
      return authenticated.isUserInRole(role);
    }
  }
}
