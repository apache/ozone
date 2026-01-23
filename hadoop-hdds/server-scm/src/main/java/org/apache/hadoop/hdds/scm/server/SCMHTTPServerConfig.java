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

package org.apache.hadoop.hdds.scm.server;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * SCM HTTP Server configuration in Java style configuration class.
 */
@ConfigGroup(prefix = "hdds.scm.http.auth")
public class SCMHTTPServerConfig {

  @Config(key = "hdds.scm.http.auth.kerberos.principal",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = {ConfigTag.SECURITY},
      description = "This Kerberos principal is used when communicating to " +
          "the HTTP server of SCM.The protocol used is SPNEGO."
  )
  private String principal = "";

  @Config(key = "hdds.scm.http.auth.kerberos.keytab",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = {ConfigTag.SECURITY},
      description = "The keytab file used by SCM http server to login" +
          " as its service principal."
  )
  private String keytab = "";

  public void setKerberosPrincipal(String kerberosPrincipal) {
    this.principal = kerberosPrincipal;
  }

  public void setKerberosKeytab(String kerberosKeytab) {
    this.keytab = kerberosKeytab;
  }

  public String getKerberosPrincipal() {
    return this.principal;
  }

  public String getKerberosKeytab() {
    return this.keytab;
  }

  /**
   * This static class is required to support other classes
   * that reference the key names and also require attributes.
   * Example: SCMSecurityProtocol where the KerberosInfo references
   * the old configuration with the annotation shown below:
   * <br>
   * {@code KerberosInfo(serverPrincipal =
   *    ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY)}
   */
  public static class ConfigStrings {
    public static final String HDDS_SCM_HTTP_AUTH_CONFIG_PREFIX =
        SCMHTTPServerConfig.class.getAnnotation(ConfigGroup.class).prefix() +
            ".";

    public static final String HDDS_SCM_HTTP_AUTH_TYPE =
        HDDS_SCM_HTTP_AUTH_CONFIG_PREFIX + "type";

    public static final String HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY =
        HDDS_SCM_HTTP_AUTH_CONFIG_PREFIX + "kerberos.principal";

    public static final String HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY =
        HDDS_SCM_HTTP_AUTH_CONFIG_PREFIX + "kerberos.keytab";
  }
}
