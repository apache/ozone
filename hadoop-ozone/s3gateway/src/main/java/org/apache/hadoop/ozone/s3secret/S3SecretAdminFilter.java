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

package org.apache.hadoop.ozone.s3secret;

import java.io.IOException;
import java.security.Principal;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Filter that only allows admin to access endpoints annotated with {@link S3AdminEndpoint}.
 * Condition is based on the value of the configuration keys for:
 * <ul>
 *   <li>ozone.administrators</li>
 *   <li>ozone.administrators.groups</li>
 * </ul>
 */
@S3AdminEndpoint
@Provider
public class S3SecretAdminFilter implements ContainerRequestFilter {

  @Inject
  private OzoneConfiguration conf;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    final Principal userPrincipal = requestContext.getSecurityContext().getUserPrincipal();
    if (null != userPrincipal) {
      UserGroupInformation user = UserGroupInformation.createRemoteUser(userPrincipal.getName());
      if (!OzoneAdmins.isS3Admin(user, conf)) {
        requestContext.abortWith(Response.status(Status.FORBIDDEN).build());
      }
    }
  }
}
