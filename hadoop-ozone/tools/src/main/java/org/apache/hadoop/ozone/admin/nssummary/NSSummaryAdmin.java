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

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.ReconEndpointType;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.net.InetSocketAddress;

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
    if (!isFSOEnabled()) {
      System.out.println("[Warning] FSO is NOT enabled. " +
          "Namespace CLI is only designed for FSO mode.\n" +
          "To enable FSO set ozone.om.enable.filesystem.paths to true " +
          "and ozone.om.metadata.layout to PREFIX.");
    }
    return null;
  }

  @Override
  public Class<?> getParentType() {
    return OzoneAdmin.class;
  }

  private boolean isFSOEnabled() {
    OzoneConfiguration conf = parent.getOzoneConf();
    return conf.getBoolean("ozone.om.enable.filesystem.paths", true)
        && conf.get("ozone.om.metadata.layout").equalsIgnoreCase("PREFIX");
  }

  public String getReconWebAddress() {
    OzoneConfiguration conf = parent.getOzoneConf();
    String protocolPrefix = "";
    InetSocketAddress reconSocket = null;
    if (OzoneSecurityUtil.isHttpSecurityEnabled(conf)) {
      protocolPrefix = "https://";
      reconSocket = HddsUtils.getReconAddresses(conf, ReconEndpointType.WEB_HTTPS);
    } else {
      protocolPrefix = "http://";
      reconSocket = HddsUtils.getReconAddresses(conf, ReconEndpointType.WEB_HTTP);
    }
    return protocolPrefix + reconSocket.getHostName() + ":" + reconSocket.getPort();
  }
}
