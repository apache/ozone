/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.admin.scm;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;
import picocli.CommandLine.Mixin;
import java.util.concurrent.Callable;

/**
 * Handler of ozone admin scm transfer command.
 */
@CommandLine.Command(
    name = "transfer",
    description = "Manually transfer the raft leadership to the target node.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class TransferScmLeaderSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private ScmAdmin parent;

  @Mixin
  private ScmOption scmOption;

  @CommandLine.ArgGroup(multiplicity = "1")
  private TransferOption configGroup;

  static class TransferOption {
    @CommandLine.Option(
        names = {"-n", "--newLeaderId"},
        description = "The new leader id of SCM to transfer leadership. " +
            "Should be ScmId(UUID)."
    )
    private String scmId;

    @CommandLine.Option(names = {"-r", "--random"},
        description = "Randomly choose a follower to transfer leadership.")
    private boolean isRandom;
  }

  @Override
  public Void call() throws Exception {
    ScmClient client = scmOption.createScmClient(
        parent.getParent().getOzoneConf());
    if (configGroup.isRandom) {
      configGroup.scmId = "";
    }
    client.transferLeadership(configGroup.scmId);
    System.out.println("Transfer leadership successfully to " +
        (configGroup.isRandom ? "random node" : configGroup.scmId) + ".");
    return null;
  }
}
