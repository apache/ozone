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
 * <p>AWS S3 (and Ozone before the Jetty 12 upgrade) returns a bare
 * {@code application/xml} Content-Type. On Jetty 12 the servlet response tracks
 * a response character encoding ({@code _encodingFrom}); once that has been
 * promoted from {@code NOT_SET} (which happens internally, below the servlet
 * API), Jetty rebuilds the Content-Type as {@code application/xml;charset=utf-8}
 * even though the value passed to {@code setHeader} is bare. Simply stripping
 * the charset from the header value does not help, because Jetty re-appends it
 * from the tracked encoding after the wrapper runs.
 *
 * <p>This wrapper resets the response character encoding back to {@code NOT_SET}
 * (via {@code setCharacterEncoding(null)}) right before it writes an
 * {@code application/xml} Content-Type, so Jetty keeps the header bare, and it
 * also normalizes the value itself as a safeguard. It only touches
 * {@code application/xml} responses; every other Content-Type is left unchanged.
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

  private static boolean isXml(String contentType) {
    return contentType != null
        && contentType.toLowerCase(Locale.ROOT).startsWith(APPLICATION_XML);
  }

  private static final class XmlContentTypeResponse extends HttpServletResponseWrapper {

    XmlContentTypeResponse(HttpServletResponse response) {
      super(response);
    }

    /**
     * Reset the tracked response encoding so Jetty does not re-append a charset
     * to a bare {@code application/xml} Content-Type, then write the bare value.
     */
    private void putBareXml(Runnable write) {
      setCharacterEncoding(null);
      write.run();
    }

    @Override
    public void setContentType(String type) {
      if (isXml(type)) {
        putBareXml(() -> super.setContentType(APPLICATION_XML));
      } else {
        super.setContentType(type);
      }
    }

    @Override
    public void setHeader(String name, String value) {
      if ("Content-Type".equalsIgnoreCase(name) && isXml(value)) {
        putBareXml(() -> super.setHeader(name, APPLICATION_XML));
      } else {
        super.setHeader(name, value);
      }
    }

    @Override
    public void addHeader(String name, String value) {
      if ("Content-Type".equalsIgnoreCase(name) && isXml(value)) {
        putBareXml(() -> super.addHeader(name, APPLICATION_XML));
      } else {
        super.addHeader(name, value);
      }
    }
  }
}
