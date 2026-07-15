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

package org.apache.hadoop.hdds.recon;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * The configuration class for the Recon service.
 */
@ConfigGroup(prefix = "ozone.recon")
public class ReconConfig {

  @Config(key = "ozone.recon.kerberos.principal",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = { ConfigTag.SECURITY, ConfigTag.RECON, ConfigTag.OZONE },
      description = "This Kerberos principal is used by the Recon service."
  )
  private String principal;

  @Config(key = "ozone.recon.kerberos.keytab.file",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = { ConfigTag.SECURITY, ConfigTag.RECON, ConfigTag.OZONE },
      description = "The keytab file used by Recon daemon to login as " +
          "its service principal."
  )
  private String keytab;

  @Config(key = "ozone.recon.security.client.datanode.container.protocol.acl",
      type = ConfigType.STRING,
      defaultValue = "*",
      tags = { ConfigTag.SECURITY, ConfigTag.RECON, ConfigTag.OZONE },
      description = "Comma separated acls (users, groups) allowing clients " +
          "accessing datanode container protocol"
  )
  private String acl;

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

  public String getAcl() {
    return acl;
  }

  public void setAcl(String acl) {
    this.acl = acl;
  }

  /**
   * Config Keys for Recon.
   */
  public static class ConfigStrings {
    public static final String OZONE_RECON_KERBEROS_PRINCIPAL_KEY =
        "ozone.recon.kerberos.principal";
    public static final String OZONE_RECON_KERBEROS_KEYTAB_FILE_KEY =
        "ozone.recon.kerberos.keytab.file";
    public static final String
        OZONE_RECON_SECURITY_CLIENT_DATANODE_CONTAINER_PROTOCOL_ACL =
        "ozone.recon.security.client.datanode.container.protocol.acl";
  }
}
