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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Principal;
import java.util.Collection;

/**
 * Filter that can be applied to paths to only allow access by configured
 * admins.
 */
@Singleton
public class ReconAdminFilter implements Filter {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconAdminFilter.class);

  private final OzoneConfiguration conf;

  @Inject
  ReconAdminFilter(OzoneConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException { }

  @Override
  public void doFilter(ServletRequest servletRequest,
      ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException {

    HttpServletRequest httpServletRequest =
        (HttpServletRequest) servletRequest;
    HttpServletResponse httpServletResponse =
        (HttpServletResponse) servletResponse;

    final Principal userPrincipal =
        httpServletRequest.getUserPrincipal();
    boolean isAdmin = false;
    String userName = null;

    if (userPrincipal != null) {
      UserGroupInformation ugi =
          UserGroupInformation.createRemoteUser(userPrincipal.getName());
      userName = ugi.getUserName();

      if (hasPermission(ugi)) {
        isAdmin = true;
        filterChain.doFilter(httpServletRequest, httpServletResponse);
      }
    }

    if (isAdmin) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Allowing request from admin user {} to {}",
            userName, httpServletRequest.getRequestURL());
      }
    } else {
      LOG.warn("Rejecting request from non admin user {} to {}",
          userName, httpServletRequest.getRequestURL());
      httpServletResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
    }
  }

  @Override
  public void destroy() { }

  private boolean hasPermission(UserGroupInformation user) {
    Collection<String> admins =
        conf.getStringCollection(OzoneConfigKeys.OZONE_ADMINISTRATORS);
    admins.addAll(
        conf.getStringCollection(ReconConfigKeys.OZONE_RECON_ADMINISTRATORS));
    Collection<String> adminGroups =
        conf.getStringCollection(OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS);
    adminGroups.addAll(
        conf.getStringCollection(
            ReconConfigKeys.OZONE_RECON_ADMINISTRATORS_GROUPS));
    return new OzoneAdmins(admins, adminGroups).isAdmin(user);
  }
}
