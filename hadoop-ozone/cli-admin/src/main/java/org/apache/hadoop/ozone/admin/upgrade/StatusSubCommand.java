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

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

/**
 * Sub command to query the overall upgrade status of the cluster, returning information about the finalization
 * status of SCM, the datanodes and OM.
 */
@CommandLine.Command(
    name = "status",
    description = "Show status of the cluster upgrade",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class StatusSubCommand extends ScmSubcommand {

  @Override
  public void execute(ScmClient client) throws IOException {
    String upgradeClientID = "Upgrade-Client-" + UUID.randomUUID();
    HddsProtos.UpgradeStatus status = client.queryUpgradeStatus(upgradeClientID, true);

    // Temporary output to validate the command is working.
    out().println("Update status:");
    out().println("    SCM Finalized: " + status.getScmFinalized());
    out().println("    Datanodes finalized: " + status.getNumDatanodesFinalized());
    out().println("    Total Datanodes: " + status.getNumDatanodesTotal());
    out().println("    Should Finalize: " + status.getShouldFinalize());
  }
}
