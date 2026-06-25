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
import picocli.CommandLine;

/**
 * Handler for the ozone admin upgrade finalize command.
 */
@CommandLine.Command(
    name = "finalize",
    description = "Initiates the the process to finalize a cluster upgrade.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class FinalizeSubCommand extends AbstractSubcommand implements Callable<Integer> {

  @CommandLine.Mixin
  private OmAddressOptions.OptionalServiceIdOrHostMixin omAddressOptions;

  @Override
  public Integer call() throws Exception {
    try (OzoneManagerProtocol client = getClient()) {
      OzoneManagerVersion omVersion = RpcClient.getOmVersion(client.getServiceInfo());
      if (!OzoneManagerVersion.ZDU.isSupportedBy(omVersion)) {
        err().println("OM does not support zero downtime upgrade. The cluster should be finalized with " +
            "`ozone admin om finalizeupgrade`");
        return 1;
      }
      client.finalizeUpgrade();
      out().println("Cluster finalization has been started. Monitor progress with `ozone admin upgrade status`");
    }
    return 0;
  }

  protected OzoneManagerProtocol getClient() throws Exception {
    return omAddressOptions.newClient();
  }
}
