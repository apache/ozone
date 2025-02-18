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

import static org.apache.hadoop.ozone.s3secret.S3SecretConfigKeys.OZONE_S3G_SECRET_HTTP_ENABLED_KEY;

import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Filter that disables all endpoints annotated with {@link S3SecretEnabled}.
 * Condition is based on the value of the configuration key
 * ozone.s3g.secret.http.enabled.
 */
@S3SecretEnabled
@Provider
public class S3SecretEnabledEndpointRequestFilter
    implements ContainerRequestFilter {

  @Inject
  private OzoneConfiguration ozoneConfiguration;

  @Override
  public void filter(ContainerRequestContext requestContext)
      throws IOException {
    boolean isSecretEnabled = ozoneConfiguration.getBoolean(
        OZONE_S3G_SECRET_HTTP_ENABLED_KEY, false);
    if (!isSecretEnabled) {
      requestContext.abortWith(Response.status(Response.Status.BAD_REQUEST)
          .entity("S3 Secret endpoint is disabled.")
          .build());
    }
  }
}
