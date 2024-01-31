/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
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

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_KEYTAB_FILE;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL;
import static org.apache.hadoop.ozone.s3secret.S3SecretConfigKeys.OZONE_S3G_SECRET_HTTP_AUTH_TYPE_KEY;
import static org.apache.hadoop.ozone.s3secret.S3SecretConfigKeys.OZONE_S3G_SECRET_HTTP_AUTH_TYPE_DEFAULT;
import static org.apache.hadoop.ozone.s3secret.S3SecretConfigKeys.OZONE_S3G_SECRET_HTTP_ENABLED_KEY;
import static org.apache.hadoop.ozone.s3secret.S3SecretConfigKeys.OZONE_S3G_SECRET_HTTP_ENABLED_KEY_DEFAULT;

/**
 * Http server to provide S3-compatible API.
 */
public class S3GatewayHttpServer extends BaseHttpServer {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3GatewayHttpServer.class);

  /**
   * Default offset between two filters.
   */
  public static final int FILTER_PRIORITY_DO_AFTER = 50;

  public S3GatewayHttpServer(MutableConfigurationSource conf, String name)
      throws IOException {
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
              principalInConf, conf.get(OZONE_S3G_HTTP_BIND_HOST_KEY)));
        }
        String httpKeytab = conf.get(OZONE_S3G_KEYTAB_FILE);
        if (!Strings.isNullOrEmpty(httpKeytab)) {
          params.put("kerberos.keytab", httpKeytab);
        }
        params.put(AuthenticationFilter.AUTH_TYPE, "kerberos");

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
    return S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY;
  }

  @Override
  protected String getHttpBindHostKey() {
    return OZONE_S3G_HTTP_BIND_HOST_KEY;
  }

  @Override
  protected String getHttpsAddressKey() {
    return S3GatewayConfigKeys.OZONE_S3G_HTTPS_ADDRESS_KEY;
  }

  @Override
  protected String getHttpsBindHostKey() {
    return S3GatewayConfigKeys.OZONE_S3G_HTTPS_BIND_HOST_KEY;
  }

  @Override
  protected String getBindHostDefault() {
    return S3GatewayConfigKeys.OZONE_S3G_HTTP_BIND_HOST_DEFAULT;
  }

  @Override
  protected int getHttpBindPortDefault() {
    return S3GatewayConfigKeys.OZONE_S3G_HTTP_BIND_PORT_DEFAULT;
  }

  @Override
  protected int getHttpsBindPortDefault() {
    return S3GatewayConfigKeys.OZONE_S3G_HTTPS_BIND_PORT_DEFAULT;
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
    return S3GatewayConfigKeys.OZONE_S3G_HTTP_ENABLED_KEY;
  }

  @Override
  protected String getHttpAuthType() {
    return S3GatewayConfigKeys.OZONE_S3G_HTTP_AUTH_TYPE;
  }

  @Override
  protected String getHttpAuthConfigPrefix() {
    return S3GatewayConfigKeys.OZONE_S3G_HTTP_AUTH_CONFIG_PREFIX;
  }

  /**
   * Servlet for favicon.ico.
   */
  public static class IconServlet extends HttpServlet {
    private static final long serialVersionUID = -1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
      response.setContentType("image/png");
      response.sendRedirect("/static/images/ozone.ico");
    }
  }
}
