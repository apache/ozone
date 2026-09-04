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
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.servletbridge.JavaxFilterBridge;
import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that can be applied to paths to only allow access by authenticated
 * kerberos users.
 *
 * <p>The authentication itself is performed by hadoop's
 * {@code javax.servlet}-based {@link ProxyUserAuthenticationFilter}. This
 * jakarta filter runs it through {@link JavaxFilterBridge} so it can be
 * registered on Recon's Guice/Jetty EE10 (jakarta) servlet chain.
 */
@Singleton
public class ReconAuthFilter implements Filter {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconAuthFilter.class);

  private final OzoneConfiguration conf;
  private JavaxFilterBridge authFilterBridge;

  @Inject
  ReconAuthFilter(OzoneConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    Map<String, String> parameters = getFilterConfigMap(conf,
        OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX);
    authFilterBridge = new JavaxFilterBridge(new ProxyUserAuthenticationFilter());
    authFilterBridge.init(new FilterConfig() {
      @Override
      public String getFilterName() {
        return "authentication";
      }

      @Override
      public ServletContext getServletContext() {
        return filterConfig.getServletContext();
      }

      @Override
      public String getInitParameter(String s) {
        return parameters.get(s);
      }

      @Override
      public Enumeration<String> getInitParameterNames() {
        return Collections.enumeration(parameters.keySet());
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

    authFilterBridge.doFilter(servletRequest, servletResponse, filterChain);
  }

  @Override
  public void destroy() {
    if (authFilterBridge != null) {
      authFilterBridge.destroy();
    }
  }
}
