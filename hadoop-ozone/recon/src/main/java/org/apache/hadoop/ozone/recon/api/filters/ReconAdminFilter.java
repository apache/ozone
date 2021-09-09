/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.api.filters;

import java.io.IOException;
import java.util.Collection;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that can be applied to paths to only allow access by configured
 * admins.
 */
@Singleton
public class ReconAdminFilter implements Filter {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconAdminFilter.class);

  @Inject
  private OzoneConfiguration conf;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    LOG.info("ReconAdminFilter init.");
  }

  @Override
  public void doFilter(ServletRequest servletRequest,
      ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException {
    LOG.info("ReconAdminFilter doFilter.");

    HttpServletRequest httpServletRequest =
        (HttpServletRequest) servletRequest;
    HttpServletResponse httpServletResponse =
        (HttpServletResponse) servletResponse;

    final java.security.Principal userPrincipal =
        httpServletRequest.getUserPrincipal();
    String userPrincipalName = null;
    boolean isAdmin = false;

    if (userPrincipal != null) {
      userPrincipalName = userPrincipal.getName();
      Collection<String> admins =
          conf.getStringCollection(OzoneConfigKeys.OZONE_ADMINISTRATORS);
      if (admins.contains(userPrincipalName)) {
        isAdmin = true;
        filterChain.doFilter(httpServletRequest, httpServletResponse);
      }
    }
    LOG.info("Requester user principal '{}'. Is admin? {}", userPrincipalName,
        isAdmin);

    if (!isAdmin) {
      httpServletResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
    }

  }

  @Override
  public void destroy() {
    LOG.info("ReconAdminFilter destroy.");
  }
}
