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

package org.apache.hadoop.ozone.s3;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.server.http.BaseHttpServer;
import org.apache.hadoop.hdds.server.http.ServletElementsFactory;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;
import org.eclipse.jetty.servlet.ServletHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.*;
import static org.apache.hadoop.ozone.s3sts.S3STSConfigKeys.OZONE_S3G_STS_HTTP_ENABLED_KEY;
import static org.apache.hadoop.ozone.s3sts.S3STSConfigKeys.OZONE_S3G_STS_HTTP_ENABLED_KEY_DEFAULT;
import static org.apache.hadoop.ozone.s3sts.S3STSConfigKeys.OZONE_S3G_STS_HTTP_AUTH_TYPE_KEY;
import static org.apache.hadoop.ozone.s3sts.S3STSConfigKeys.OZONE_S3G_STS_HTTP_AUTH_TYPE_DEFAULT;
import static org.apache.hadoop.security.authentication.server.AuthenticationFilter.AUTH_TYPE;

/**
 * HTTP server for the S3 Gateway STS endpoint.
 */
public class S3STSHttpServer extends BaseHttpServer {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3STSHttpServer.class);

  S3STSHttpServer(MutableConfigurationSource conf, String name) throws IOException {
    super(conf, name);
    addSTSAuthentication(conf);
  }

  private void addSTSAuthentication(MutableConfigurationSource conf)
      throws IOException {
    if (conf.getBoolean(OZONE_S3G_STS_HTTP_ENABLED_KEY,
        OZONE_S3G_STS_HTTP_ENABLED_KEY_DEFAULT)) {
      String authType = conf.get(OZONE_S3G_STS_HTTP_AUTH_TYPE_KEY,
          OZONE_S3G_STS_HTTP_AUTH_TYPE_DEFAULT);

      if (UserGroupInformation.isSecurityEnabled()
          && authType.equals("kerberos")) {
        ServletHandler handler = getWebAppContext().getServletHandler();
        Map<String, String> params = new HashMap<>();

        String principalInConf = conf.get(getSpnegoPrincipal());
        if (!Strings.isNullOrEmpty(principalInConf)) {
          params.put("kerberos.principal", SecurityUtil.getServerPrincipal(
              principalInConf, conf.get(getHttpBindHostKey())));
        }
        String httpKeytab = conf.get(getKeytabFile());
        if (!Strings.isNullOrEmpty(httpKeytab)) {
          params.put("kerberos.keytab", httpKeytab);
        }
        params.put(AUTH_TYPE, "kerberos");

        FilterHolder holder = ServletElementsFactory.createFilterHolder(
            "stsAuthentication", AuthenticationFilter.class.getName(),
            params);
        FilterMapping filterMapping =
            ServletElementsFactory.createFilterMapping(
                "stsAuthentication",
                new String[]{"/sts/*"});

        handler.addFilter(holder, filterMapping);
      } else {
        LOG.error("STS Endpoint should be secured with Kerberos");
        throw new IllegalStateException(
            "STS Endpoint should be secured with Kerberos"
        );
      }
    }
  }

  @Override
  protected String getHttpAddressKey() {
    return OZONE_S3G_STS_HTTP_ADDRESS_KEY;
  }

  @Override
  protected String getHttpBindHostKey() {
    return OZONE_S3G_STS_HTTP_BIND_HOST_KEY;
  }

  @Override
  protected String getHttpsAddressKey() {
    return OZONE_S3G_STS_HTTPS_ADDRESS_KEY;
  }

  @Override
  protected String getHttpsBindHostKey() {
    return OZONE_S3G_STS_HTTPS_BIND_HOST_KEY;
  }

  @Override
  protected String getBindHostDefault() {
    return OZONE_S3G_HTTP_BIND_HOST_DEFAULT;
  }

  @Override
  protected int getHttpBindPortDefault() {
    return OZONE_S3G_STS_HTTP_BIND_PORT_DEFAULT;
  }

  @Override
  protected int getHttpsBindPortDefault() {
    return OZONE_S3G_STS_HTTPS_BIND_PORT_DEFAULT;
  }

  @Override
  protected String getKeytabFile() {
    return OZONE_S3G_KEYTAB_FILE;
  }

  @Override
  protected String getSpnegoPrincipal() {
    return OZONE_S3G_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL;
  }

  @Override
  protected String getEnabledKey() {
    return OZONE_S3G_STS_HTTP_ENABLED_KEY;
  }

  @Override
  protected String getHttpAuthType() {
    return OZONE_S3G_HTTP_AUTH_TYPE;
  }

  @Override
  protected String getHttpAuthConfigPrefix() {
    return OZONE_S3G_HTTP_AUTH_CONFIG_PREFIX;
  }
}
