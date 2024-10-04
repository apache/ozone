/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.s3secret;


import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.ozone.om.OzoneConfigUtil;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;

/**
 * Filter that disables all endpoints annotated with {@link S3AdminEndpoint}.
 * Condition is based on the value of the configuration keys for
 *   - ozone.administrators
 *   - ozone.administrators.groups
 */

@S3AdminEndpoint
@Provider
public class S3SecretAdminFilter implements ContainerRequestFilter {
  @Inject
  private OzoneConfiguration conf;

  @Override
  public void filter(ContainerRequestContext requestContext)
      throws IOException {
    final Principal userPrincipal = requestContext.getSecurityContext().getUserPrincipal();
    if (null != userPrincipal) {
      UserGroupInformation user =
          UserGroupInformation.createRemoteUser(userPrincipal.getName());
      if (!isAdmin(user)) {
        requestContext.abortWith(Response.status(Response.Status.FORBIDDEN)
            .entity("Non-Admin user accessing endpoint")
            .build());
      }
    }
  }

  /**
   * Checks if the user that is passed is an Admin.
   * @param user Stores the user to filter
   * @return true if the user is admin else false
   */
  private boolean isAdmin(UserGroupInformation user) {
    Collection<String> s3Admins;
    try {
      s3Admins = OzoneConfigUtil.getS3AdminsFromConfig(conf);
    } catch (IOException ie) {
      // We caught an exception while trying to log in using the UGI user
      // Refer to UserGroupInformation.getCurrentUser() in getS3AdminsFromConfig
      s3Admins = Collections.<String>emptyList();
    }
    Collection<String> s3AdminGroups =
        OzoneConfigUtil.getS3AdminsGroupsFromConfig(conf);
    // If there are no admins or admin groups specified, return false
    if (s3Admins.isEmpty() || s3AdminGroups.isEmpty()) {
      return false;
    }
    return new OzoneAdmins(s3Admins, s3AdminGroups).isAdmin(user);
  }
}
