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

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_AUTH_CONFIG_PREFIX;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_AUTH_TYPE;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_BIND_HOST_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_KEYTAB_FILE;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTPS_BIND_PORT_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_BIND_PORT_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_ENABLED_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL;
import static org.apache.hadoop.ozone.s3secret.S3SecretConfigKeys.OZONE_S3G_SECRET_HTTP_AUTH_TYPE_DEFAULT;
import static org.apache.hadoop.ozone.s3secret.S3SecretConfigKeys.OZONE_S3G_SECRET_HTTP_AUTH_TYPE_KEY;
import static org.apache.hadoop.ozone.s3secret.S3SecretConfigKeys.OZONE_S3G_SECRET_HTTP_ENABLED_KEY;
import static org.apache.hadoop.ozone.s3secret.S3SecretConfigKeys.OZONE_S3G_SECRET_HTTP_ENABLED_KEY_DEFAULT;
import static org.apache.hadoop.security.authentication.server.AuthenticationFilter.AUTH_TYPE;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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

/**
 * HTTP server for serving static content and Ozone-specific endpoints (/conf, etc.).
 */
class S3GatewayWebAdminServer extends BaseHttpServer {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3GatewayWebAdminServer.class);

  S3GatewayWebAdminServer(MutableConfigurationSource conf, String name) throws IOException {
    super(conf, name);
    addServlet("icon", "/favicon.ico", IconServlet.class);
    addSecretAuthentication(conf);
  }

  private void addSecretAuthentication(MutableConfigurationSource conf)
      throws IOException {

    if (conf.getBoolean(OZONE_S3G_SECRET_HTTP_ENABLED_KEY,
        OZONE_S3G_SECRET_HTTP_ENABLED_KEY_DEFAULT)) {
      String authType = conf.get(OZONE_S3G_SECRET_HTTP_AUTH_TYPE_KEY,
          OZONE_S3G_SECRET_HTTP_AUTH_TYPE_DEFAULT);

      if (UserGroupInformation.isSecurityEnabled()
          && authType.equals("kerberos")) {
        ServletHandler handler = getWebAppContext().getServletHandler();
        Map<String, String> params = new HashMap<>();

        String principalInConf =
            conf.get(OZONE_S3G_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL);
        if (!Strings.isNullOrEmpty(principalInConf)) {
          params.put("kerberos.principal", SecurityUtil.getServerPrincipal(
              principalInConf, conf.get(OZONE_S3G_WEBADMIN_HTTP_BIND_HOST_KEY)));
        }
        String httpKeytab = conf.get(OZONE_S3G_KEYTAB_FILE);
        if (!Strings.isNullOrEmpty(httpKeytab)) {
          params.put("kerberos.keytab", httpKeytab);
        }
        params.put(AUTH_TYPE, "kerberos");

        FilterHolder holder = ServletElementsFactory.createFilterHolder(
            "secretAuthentication", AuthenticationFilter.class.getName(),
            params);
        FilterMapping filterMapping =
            ServletElementsFactory.createFilterMapping(
                "secretAuthentication",
                new String[]{"/secret/*"});

        handler.addFilter(holder, filterMapping);
      } else {
        LOG.error("Secret Endpoint should be secured with Kerberos");
        throw new IllegalStateException("Secret Endpoint should be secured"
            + " with Kerberos");
      }
    }
  }

  @Override
  protected String getHttpAddressKey() {
    return OZONE_S3G_WEBADMIN_HTTP_ADDRESS_KEY;
  }

  @Override
  protected String getHttpBindHostKey() {
    return OZONE_S3G_WEBADMIN_HTTP_BIND_HOST_KEY;
  }

  @Override
  protected String getHttpsAddressKey() {
    return OZONE_S3G_WEBADMIN_HTTPS_ADDRESS_KEY;
  }

  @Override
  protected String getHttpsBindHostKey() {
    return OZONE_S3G_WEBADMIN_HTTPS_BIND_HOST_KEY;
  }

  @Override
  protected String getBindHostDefault() {
    return OZONE_S3G_HTTP_BIND_HOST_DEFAULT;
  }

  @Override
  protected int getHttpBindPortDefault() {
    return OZONE_S3G_WEBADMIN_HTTP_BIND_PORT_DEFAULT;
  }

  @Override
  protected int getHttpsBindPortDefault() {
    return OZONE_S3G_WEBADMIN_HTTPS_BIND_PORT_DEFAULT;
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
    return OZONE_S3G_WEBADMIN_HTTP_ENABLED_KEY;
  }

  @Override
  protected String getHttpAuthType() {
    return OZONE_S3G_HTTP_AUTH_TYPE;
  }

  @Override
  protected String getHttpAuthConfigPrefix() {
    return OZONE_S3G_HTTP_AUTH_CONFIG_PREFIX;
  }

  /**
   * Servlet for favicon.ico.
   */
  public static class IconServlet extends HttpServlet {
    private static final long serialVersionUID = -1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      response.setContentType("image/png");
      response.sendRedirect("/images/ozone.ico");
    }
  }
}
