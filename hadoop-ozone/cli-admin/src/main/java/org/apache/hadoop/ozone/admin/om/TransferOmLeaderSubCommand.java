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

package org.apache.hadoop.ozone.admin.om;

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

/**
 * Handler of ozone admin om transfer command.
 */
@CommandLine.Command(
    name = "transfer",
    description = "Manually transfer the raft leadership to the target node.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class TransferOmLeaderSubCommand implements Callable<Void> {

  @CommandLine.Mixin
  private OmAddressOptions.OptionalServiceIdMixin omServiceOption;

  @CommandLine.ArgGroup(multiplicity = "1")
  private TransferOption configGroup;

  static class TransferOption {
    @CommandLine.Option(
        names = {"-n", "--newLeaderId", "--new-leader-id"},
        description = "The new leader id of OM to transfer leadership. E.g OM1."
    )
    private String omNodeId;

    @CommandLine.Option(names = {"-r", "--random"},
        description = "Randomly choose a follower to transfer leadership.")
    private boolean isRandom;
  }

  @Override
  public Void call() throws Exception {
    if (configGroup.isRandom) {
      configGroup.omNodeId = "";
    }
    try (OzoneManagerProtocol client = omServiceOption.newClient()) {
      client.transferLeadership(configGroup.omNodeId);
      System.out.println("Transfer leadership successfully to " +
          (configGroup.isRandom ? "random node" : configGroup.omNodeId) + ".");
    }
    return null;
  }
}
