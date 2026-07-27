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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.admin.om.OmAddressOptions;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.QueryUpgradeStatusResponse;
import picocli.CommandLine;

/**
 * Handler for the ozone admin upgrade finalize command.
 */
@CommandLine.Command(
    name = "finalize",
    description = "Initiates the process to finalize a cluster upgrade. This command is idempotent.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class FinalizeSubCommand extends AbstractSubcommand implements Callable<Integer> {

  /** Poll cadence used by {@code --wait}. Overridable from tests via {@link #setPollIntervalMillis(long)}. */
  private long pollIntervalMillis = TimeUnit.SECONDS.toMillis(5);

  @CommandLine.Mixin
  private OmAddressOptions.OptionalServiceIdOrHostMixin omAddressOptions;

  @CommandLine.Option(names = {"--wait"},
      defaultValue = "false",
      description = "After initiating finalization, poll the cluster status until the entire cluster (OM, SCM, "
          + "and all healthy datanodes) is finalized. Interrupt with Ctrl-C to stop waiting; finalization "
          + "continues on the server.")
  private boolean wait;

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
      if (wait) {
        out().println("Cluster finalization has been started. Waiting for the cluster to finalize; "
            + "interrupt with Ctrl-C to stop waiting (finalization continues on the server).");
        return waitForFinalization(client);
      }
      out().println("Cluster finalization has been started. Monitor progress with `ozone admin upgrade status`");
    }
    return 0;
  }

  /**
   * Polls the cluster status until OM, SCM and all healthy datanodes report finalized, or the operator
   * interrupts the command. A failed status query is reported to stderr and retried on the next poll,
   * so transient RPC errors do not abort the wait. {@code --wait} is safe to re-run: if the cluster is
   * already finalized, the first poll returns done and the command exits 0.
   */
  private int waitForFinalization(OzoneManagerProtocol client) {
    while (true) {
      QueryUpgradeStatusResponse status = null;
      try {
        status = client.queryUpgradeStatus();
      } catch (Exception e) {
        err().println("Failed to query upgrade status: " + e.getMessage()
            + ". Retrying, or use `ozone admin upgrade status` to monitor progress.");
      }

      if (status != null) {
        // Finalization checks before sleeping, so an already-finalized cluster returns without waiting.
        if (status.getClusterFinalized()) {
          out().println("Finalization complete.");
          return 0;
        }

        if (isVerbose()) {
          StatusSubCommand.printVerbose(status, out());
        } else {
          StatusSubCommand.printBasic(status, out());
        }
        out().flush();
      }

      try {
        Thread.sleep(pollIntervalMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        out().println("Waiting interrupted. Use `ozone admin upgrade status` to monitor progress.");
        return 1;
      }
    }
  }

  protected OzoneManagerProtocol getClient() throws Exception {
    return omAddressOptions.newClient();
  }

  @VisibleForTesting
  void setPollIntervalMillis(long millis) {
    this.pollIntervalMillis = millis;
  }
}
