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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hdds.tracing.TracingUtil;

/**
 * Servlet filter that runs each HTTP request in a SERVER span, continuing the caller's W3C trace context.
 */
public class HttpTracingFilter implements Filter {

  /** Request attribute holding the request's span context, read by the HTTP request log. */
  public static final String SPAN_CONTEXT_ATTRIBUTE = HttpTracingFilter.class.getName() + ".spanContext";

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest httpReq = (HttpServletRequest) req;
    Span span = TracingUtil.createServerSpanFromW3cHttpHeaders(
        httpReq.getMethod() + " " + httpReq.getRequestURI(), httpReq::getHeader);
    if (span.getSpanContext().isValid()) {
      req.setAttribute(SPAN_CONTEXT_ATTRIBUTE, span.getSpanContext());
    }
    try (Scope ignored = span.makeCurrent()) {
      chain.doFilter(req, res);
      if (((HttpServletResponse) res).getStatus() >= HttpServletResponse.SC_INTERNAL_SERVER_ERROR) {
        span.setStatus(StatusCode.ERROR);
      }
    } catch (IOException | ServletException | RuntimeException e) {
      span.recordException(e).setStatus(StatusCode.ERROR);
      throw e;
    } finally {
      span.end();
    }
  }

  @Override
  public void destroy() {
  }
}
