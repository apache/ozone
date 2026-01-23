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

import java.io.IOException;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

/**
 * Filter to accept queries with empty string content-type (ruby sdk).
 */
public class EmptyContentTypeFilter implements Filter {

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {

  }

  @Override
  public void doFilter(
      ServletRequest request, ServletResponse response, FilterChain chain
  ) throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    if ("".equals(httpRequest.getContentType())) {
      chain.doFilter(new HttpServletRequestWrapper(httpRequest) {
        @Override
        public String getContentType() {
          return null;
        }

        @Override
        public String getHeader(String name) {
          if (name.equalsIgnoreCase("Content-Type")) {
            return null;
          }
          return super.getHeader(name);
        }

        @Override
        public Enumeration<String> getHeaders(String name) {
          if ("Content-Type".equalsIgnoreCase(name)) {
            return null;
          }
          return super.getHeaders(name);
        }

        @Override
        public Enumeration<String> getHeaderNames() {
          return new EnumerationWrapper(super.getHeaderNames());
        }
      }, response);
    } else {
      chain.doFilter(request, response);
    }

  }

  @Override
  public void destroy() {

  }

  /**
   * Enumeration Wrapper which removes Content-Type from the original
   * enumeration.
   */
  public static class EnumerationWrapper implements Enumeration<String> {

    private final Enumeration<String> original;

    private String nextElement;

    public EnumerationWrapper(Enumeration<String> original) {
      this.original = original;
      step();
    }

    private void step() {
      if (original.hasMoreElements()) {
        nextElement = original.nextElement();
      } else {
        nextElement = null;
      }
      if ("Content-Type".equalsIgnoreCase(nextElement)) {
        if (original.hasMoreElements()) {
          nextElement = original.nextElement();
        } else {
          nextElement = null;
        }
      }
    }

    @Override
    public boolean hasMoreElements() {
      return nextElement != null;
    }

    @Override
    public String nextElement() {
      if (nextElement == null) {
        throw new NoSuchElementException();
      }
      String returnValue = nextElement;
      step();
      return returnValue;
    }
  }
}
