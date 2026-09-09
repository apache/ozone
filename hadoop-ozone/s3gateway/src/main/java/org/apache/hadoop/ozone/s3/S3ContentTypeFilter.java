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
 * Preserve the S3 Content-Type verbatim, without an auto-appended charset.
 *
 * <p>AWS S3 (and Ozone before the Jetty 12 upgrade) returns the Content-Type
 * exactly as the gateway set it: the stored object type for GET/HEAD and a bare
 * {@code application/xml} for the XML API/error responses, in both cases without
 * a charset parameter unless one was explicitly stored.
 *
 * <p>On Jetty 12 the servlet response tracks a response character encoding
 * ({@code _encodingFrom}). HttpServer2's global {@code QuotingInputFilter} runs
 * first on every request and, for a request URI with no known extension (most
 * object keys), defaults the response to {@code text/plain; charset=utf-8},
 * promoting that encoding away from {@code NOT_SET}. Jetty then appends the
 * tracked charset to any later <em>bare</em> Content-Type, so a stored
 * {@code binary/octet-stream} or a bare {@code application/xml} goes out as
 * {@code ...;charset=utf-8}. Stripping the charset from the header value does
 * not help, because Jetty re-appends it from the tracked encoding after the
 * wrapper runs.
 *
 * <p>This wrapper resets the tracked encoding back to {@code NOT_SET} (via
 * {@code setCharacterEncoding(null)}) right before it writes a Content-Type
 * that carries no {@code charset} parameter, so Jetty keeps that value bare. A
 * Content-Type that already carries an explicit {@code charset} is written
 * unchanged. In all cases the value itself is passed through verbatim, so the
 * media type and any explicit charset are preserved.
 */
public class S3ContentTypeFilter implements Filter {

  private static final String CONTENT_TYPE = "Content-Type";

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(
      ServletRequest request, ServletResponse response, FilterChain chain
  ) throws IOException, ServletException {
    if (response instanceof HttpServletResponse) {
      chain.doFilter(request, new VerbatimContentTypeResponse((HttpServletResponse) response));
    } else {
      chain.doFilter(request, response);
    }
  }

  @Override
  public void destroy() {
  }

  private static boolean hasCharset(String contentType) {
    return contentType != null
        && contentType.toLowerCase(Locale.ROOT).contains("charset=");
  }

  private static final class VerbatimContentTypeResponse extends HttpServletResponseWrapper {

    VerbatimContentTypeResponse(HttpServletResponse response) {
      super(response);
    }

    /**
     * Reset the tracked response encoding so Jetty does not append a charset to
     * a bare Content-Type, then write the value verbatim. A value that already
     * carries an explicit charset is written as-is, with no encoding reset.
     */
    private void writeContentType(String value, Runnable write) {
      if (!hasCharset(value)) {
        setCharacterEncoding(null);
      }
      write.run();
    }

    @Override
    public void setContentType(String type) {
      writeContentType(type, () -> super.setContentType(type));
    }

    @Override
    public void setHeader(String name, String value) {
      if (CONTENT_TYPE.equalsIgnoreCase(name)) {
        writeContentType(value, () -> super.setHeader(name, value));
      } else {
        super.setHeader(name, value);
      }
    }

    @Override
    public void addHeader(String name, String value) {
      if (CONTENT_TYPE.equalsIgnoreCase(name)) {
        writeContentType(value, () -> super.addHeader(name, value));
      } else {
        super.addHeader(name, value);
      }
    }
  }
}
