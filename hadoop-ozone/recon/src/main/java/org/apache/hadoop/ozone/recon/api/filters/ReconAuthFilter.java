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

package org.apache.hadoop.ozone.recon.api.filters;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX;
import static org.apache.hadoop.security.AuthenticationFilterInitializer.getFilterConfigMap;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilter;
import org.eclipse.jetty.servlet.FilterHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that can be applied to paths to only allow access by authenticated
 * kerberos users.
 */
@Singleton
public class ReconAuthFilter implements Filter {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconAuthFilter.class);

  private final OzoneConfiguration conf;
  private ProxyUserAuthenticationFilter hadoopAuthFilter;

  @Inject
  ReconAuthFilter(OzoneConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    hadoopAuthFilter = new ProxyUserAuthenticationFilter();

    Map<String, String> parameters = getFilterConfigMap(conf,
        OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX);
    FilterHolder filterHolder = getFilterHolder("authentication",
        AuthenticationFilter.class.getName(),
        parameters);
    hadoopAuthFilter.init(new FilterConfig() {
      @Override
      public String getFilterName() {
        return filterHolder.getName();
      }

      @Override
      public ServletContext getServletContext() {
        return filterConfig.getServletContext();
      }

      @Override
      public String getInitParameter(String s) {
        return filterHolder.getInitParameter(s);
      }

      @Override
      public Enumeration<String> getInitParameterNames() {
        return filterHolder.getInitParameterNames();
      }
    });
  }

  @Override
  public void doFilter(ServletRequest servletRequest,
      ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Filtering request to {} through authentication filter.",
          ((HttpServletRequest) servletRequest).getRequestURL());
    }

    hadoopAuthFilter.doFilter(servletRequest, servletResponse, filterChain);
  }

  private static FilterHolder getFilterHolder(String name, String classname,
                                              Map<String, String> parameters) {
    FilterHolder holder = new FilterHolder();
    holder.setName(name);
    holder.setClassName(classname);
    if (parameters != null) {
      holder.setInitParameters(parameters);
    }
    return holder;
  }

  @Override
  public void destroy() { }
}
