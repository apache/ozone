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

package org.apache.hadoop.ozone.s3sts;

import static org.apache.hadoop.ozone.s3sts.S3STSConfigKeys.OZONE_S3G_STS_HTTP_ENABLED_KEY;

import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Filter that disables all endpoints annotated with {@link S3STSEnabled}.
 * Condition is based on the value of the configuration key
 * ozone.s3g.s3sts.http.enabled.
 */
@S3STSEnabled
@Provider
public class S3STSEnabledEndpointRequestFilter implements ContainerRequestFilter {
  @Inject
  private OzoneConfiguration ozoneConfiguration;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    boolean isSTSEnabled = ozoneConfiguration.getBoolean(
        OZONE_S3G_STS_HTTP_ENABLED_KEY, false);
    if (!isSTSEnabled) {
      requestContext.abortWith(Response.status(Response.Status.NOT_IMPLEMENTED)
          .entity("STS endpoint is disabled.")
          .type(MediaType.APPLICATION_XML_TYPE)
          .build());
    }
  }
}
