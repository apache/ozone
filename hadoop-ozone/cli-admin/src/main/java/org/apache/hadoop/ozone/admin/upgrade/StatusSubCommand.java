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

package org.apache.hadoop.ozone.admin.upgrade;

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.admin.om.OmAddressOptions;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import picocli.CommandLine;

/**
 * Sub command to query the overall upgrade status of the cluster, returning information about the finalization
 * status of SCM, the datanodes and OM. The command makes a single call to OM which returns the status of the other
 * components as well as itself.
 */
@CommandLine.Command(
    name = "status",
    description = "Show status of the cluster upgrade",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class StatusSubCommand extends AbstractSubcommand implements Callable<Integer> {

  @CommandLine.Mixin
  private OmAddressOptions.OptionalServiceIdOrHostMixin omAddressOptions;

  @Override
  public Integer call() throws Exception {
    try (OzoneManagerProtocol client = getClient()) {
      OzoneManagerVersion omVersion = RpcClient.getOmVersion(client.getServiceInfo());
      if (!OzoneManagerVersion.ZDU.isSupportedBy(omVersion)) {
        err().println("OM does not support zero downtime upgrade. The cluster upgrade status should be queried with " +
            "`ozone admin scm finalizationstatus` and `ozone admin om finalizationstatus`");
        return 1;
      }
      OzoneManagerProtocolProtos.QueryUpgradeStatusResponse status = client.queryUpgradeStatus();


      out().println("Upgrade status:");
      out().println("    OM Finalized? " + status.getOmFinalized());
      out().println("    SCM Finalized? " + status.getHddsStatus().getScmFinalized());
      out().println("    Datanodes finalized: " + status.getHddsStatus().getNumDatanodesFinalized()
          + "/" + status.getHddsStatus().getNumDatanodesTotal());
    }
    return 0;
  }

  protected OzoneManagerProtocol getClient() throws Exception {
    return omAddressOptions.newClient();
  }

}
