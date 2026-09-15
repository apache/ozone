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

import java.util.Enumeration;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;

/**
 * Exposes a {@link jakarta.servlet.FilterConfig} as a
 * {@code javax.servlet.FilterConfig}, backing the servlet context with a
 * {@link JakartaToJavaxServletContext} over the same jakarta context so that
 * context attributes (for example the signer secret provider) are shared.
 */
class JakartaToJavaxFilterConfig implements FilterConfig {

  private final jakarta.servlet.FilterConfig delegate;
  private final ServletContext servletContext;

  JakartaToJavaxFilterConfig(jakarta.servlet.FilterConfig delegate) {
    this.delegate = delegate;
    this.servletContext = new JakartaToJavaxServletContext(delegate.getServletContext());
  }

  @Override
  public String getFilterName() {
    return delegate.getFilterName();
  }

  @Override
  public ServletContext getServletContext() {
    return servletContext;
  }

  @Override
  public String getInitParameter(String name) {
    return delegate.getInitParameter(name);
  }

  @Override
  public Enumeration<String> getInitParameterNames() {
    return delegate.getInitParameterNames();
  }
}
