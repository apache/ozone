/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.admin.nssummary;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.server.http.HttpConfig.getHttpPolicy;
import static org.apache.hadoop.http.HttpServer2.HTTPS_SCHEME;
import static org.apache.hadoop.http.HttpServer2.HTTP_SCHEME;

/**
 * Subcommand for admin operations related to OM.
 */
@CommandLine.Command(
    name = "namespace",
    description = "Namespace Summary specific admin operations",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        SummarySubCommand.class,
        DiskUsageSubCommand.class,
        QuotaUsageSubCommand.class,
        FileSizeDistSubCommand.class
    })
@MetaInfServices(SubcommandWithParent.class)
public class NSSummaryAdmin extends GenericCli implements SubcommandWithParent {
  @CommandLine.ParentCommand
  private OzoneAdmin parent;

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  public OzoneAdmin getParent() {
    return parent;
  }

  @Override
  public Void call() throws Exception {
    GenericCli.missingSubcommand(spec);
    return null;
  }

  @Override
  public Class<?> getParentType() {
    return OzoneAdmin.class;
  }

  public boolean isFSOEnabled() {
    OzoneConfiguration conf = parent.getOzoneConf();
    return conf.getBoolean("ozone.om.enable.filesystem.paths", false)
        && conf.get("ozone.om.metadata.layout").equalsIgnoreCase("PREFIX");
  }

  /**
   * e.g. Input: "0.0.0.0:9891" -> Output: "0.0.0.0"
   */
  private String getHostOnly(String host) {
    return host.split(":", 2)[0];
  }

  /**
   * e.g. Input: "0.0.0.0:9891" -> Output: "9891"
   */
  private String getPort(String host) {
    return host.split(":", 2)[1];
  }

  public String getReconWebAddress() {
    final OzoneConfiguration conf = parent.getOzoneConf();
    final String protocol;
    final HttpConfig.Policy webPolicy = getHttpPolicy(conf);

    final boolean isHostDefault;
    String host;

    if (webPolicy.isHttpsEnabled()) {
      protocol = HTTPS_SCHEME;
      host = conf.get(OZONE_RECON_HTTPS_ADDRESS_KEY,
          OZONE_RECON_HTTPS_ADDRESS_DEFAULT);
      isHostDefault = getHostOnly(host).equals(
          getHostOnly(OZONE_RECON_HTTPS_ADDRESS_DEFAULT));
    } else {
      protocol = HTTP_SCHEME;
      host = conf.get(OZONE_RECON_HTTP_ADDRESS_KEY,
          OZONE_RECON_HTTP_ADDRESS_DEFAULT);
      isHostDefault = getHostOnly(host).equals(
          getHostOnly(OZONE_RECON_HTTP_ADDRESS_DEFAULT));
    }

    if (isHostDefault) {
      // Fallback to <Recon RPC host name>:<Recon http(s) address port>
      final String rpcHost =
          conf.get(OZONE_RECON_ADDRESS_KEY, OZONE_RECON_ADDRESS_DEFAULT);
      host = getHostOnly(rpcHost) + ":" + getPort(host);
    }

    return protocol + "://" + host;
  }

  public boolean isHTTPSEnabled() {
    OzoneConfiguration conf = parent.getOzoneConf();
    return getHttpPolicy(conf) == HttpConfig.Policy.HTTPS_ONLY;
  }

  public ConfigurationSource getOzoneConfig() {
    return parent.getOzoneConf();
  }
}
