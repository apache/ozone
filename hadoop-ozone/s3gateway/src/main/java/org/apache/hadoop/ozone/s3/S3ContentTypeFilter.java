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

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.util.Locale;

/**
 * Emit {@code application/xml} without a charset parameter on S3 responses.
 *
 * <p>AWS S3 (and Ozone before the Jersey 3.1 upgrade) returns a bare
 * {@code application/xml} Content-Type. Jersey 3.1 appends {@code ;charset=UTF-8}
 * to text-based media types when writing the response entity, and Jetty stores
 * that value verbatim, so responses would otherwise carry
 * {@code application/xml;charset=utf-8}. This wrapper strips the charset
 * parameter from {@code application/xml} Content-Type headers to preserve wire
 * compatibility for S3 clients.
 */
public class S3ContentTypeFilter implements Filter {

  private static final String APPLICATION_XML = "application/xml";

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(
      ServletRequest request, ServletResponse response, FilterChain chain
  ) throws IOException, ServletException {
    if (response instanceof HttpServletResponse) {
      chain.doFilter(request, new XmlContentTypeResponse((HttpServletResponse) response));
    } else {
      chain.doFilter(request, response);
    }
  }

  @Override
  public void destroy() {
  }

  /**
   * Reduce {@code application/xml;charset=...} Content-Type values to a bare
   * {@code application/xml}; leave every other value unchanged.
   */
  private static String normalize(String contentType) {
    if (contentType != null
        && contentType.toLowerCase(Locale.ROOT).startsWith(APPLICATION_XML)) {
      return APPLICATION_XML;
    }
    return contentType;
  }

  private static final class XmlContentTypeResponse extends HttpServletResponseWrapper {

    XmlContentTypeResponse(HttpServletResponse response) {
      super(response);
    }

    @Override
    public void setContentType(String type) {
      super.setContentType(normalize(type));
    }

    @Override
    public void setHeader(String name, String value) {
      if ("Content-Type".equalsIgnoreCase(name)) {
        super.setHeader(name, normalize(value));
      } else {
        super.setHeader(name, value);
      }
    }

    @Override
    public void addHeader(String name, String value) {
      if ("Content-Type".equalsIgnoreCase(name)) {
        super.addHeader(name, normalize(value));
      } else {
        super.addHeader(name, value);
      }
    }
  }
}
