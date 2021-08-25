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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.util.Optional;

import static org.apache.hadoop.hdds.HddsUtils.getHostName;
import static org.apache.hadoop.hdds.HddsUtils.getHostPort;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.server.http.HttpConfig.getHttpPolicy;

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

  public String getReconWebAddress() {
    OzoneConfiguration conf = parent.getOzoneConf();
    String protocolPrefix = "";
    HttpConfig.Policy webPolicy = getHttpPolicy(conf);

    String name = null;

    if (webPolicy == HttpConfig.Policy.HTTPS_ONLY) {
      protocolPrefix = "https://";
      name = conf.get(OZONE_RECON_HTTPS_ADDRESS_KEY,
          OZONE_RECON_HTTPS_ADDRESS_DEFAULT);
    } else {
      protocolPrefix = "http://";
      name = conf.get(OZONE_RECON_HTTP_ADDRESS_KEY,
          OZONE_RECON_HTTP_ADDRESS_DEFAULT);
    }

    if (StringUtils.isEmpty(name)) {
      return null;
    }

    String reconDefaultAddress = conf.get(OZONE_RECON_ADDRESS_KEY,
        OZONE_RECON_ADDRESS_DEFAULT);
    Optional<String> hostname = getHostName(reconDefaultAddress);
    if (!hostname.isPresent()) {
      throw new IllegalArgumentException("Invalid hostname for Recon: "
          + reconDefaultAddress);
    }

    int port = getHostPort(name).orElse(OZONE_RECON_DATANODE_PORT_DEFAULT);
    return protocolPrefix + hostname.get() + ":" + port;
  }

  public boolean isHTTPSEnabled() {
    OzoneConfiguration conf = parent.getOzoneConf();
    return getHttpPolicy(conf) == HttpConfig.Policy.HTTPS_ONLY;
  }

  public ConfigurationSource getOzoneConfig() {
    return parent.getOzoneConf();
  }
}
