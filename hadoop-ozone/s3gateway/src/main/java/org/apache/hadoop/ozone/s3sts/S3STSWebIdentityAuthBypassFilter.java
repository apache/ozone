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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ENABLED_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.AuthorizationFilter;
import org.apache.hadoop.ozone.s3.OzoneConfigurationHolder;

/**
 * Allows unauthenticated bootstrap only for STS WebIdentity requests.
 * <p>
 * This filter is registered only in the STS JAX-RS application. It does not
 * validate or trust the JWT; OM remains the authoritative WebIdentity
 * validator and temporary credential issuer.
 */
@Provider
@PreMatching
@Priority(AuthorizationFilter.PRIORITY - 1)
public class S3STSWebIdentityAuthBypassFilter
    implements ContainerRequestFilter {

  @Inject
  private OzoneConfiguration ozoneConfiguration;

  @Override
  public void filter(ContainerRequestContext context) throws IOException {
    if (isWebIdentityEnabled()
        && S3STSWebIdentityRequestParser.isAssumeRoleWithWebIdentity(
            context)) {
      context.setProperty(AuthorizationFilter.SKIP_AWS_AUTH_PROPERTY,
          Boolean.TRUE);
    }
  }

  private boolean isWebIdentityEnabled() {
    OzoneConfiguration conf = ozoneConfiguration;
    if (conf == null) {
      conf = OzoneConfigurationHolder.configuration();
    }
    return conf != null && conf.getBoolean(OZONE_STS_WEB_IDENTITY_ENABLED,
        OZONE_STS_WEB_IDENTITY_ENABLED_DEFAULT);
  }

  @VisibleForTesting
  void setOzoneConfiguration(OzoneConfiguration conf) {
    this.ozoneConfiguration = conf;
  }
}
