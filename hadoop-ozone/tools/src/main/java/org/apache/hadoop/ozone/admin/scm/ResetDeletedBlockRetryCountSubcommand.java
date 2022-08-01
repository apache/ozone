/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.admin.scm;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Handler of the expired deleted blocks from SCM side.
 */
@CommandLine.Command(
    name = "resetDeletedBlockRetryCount",
    description = "Reset deleted block transactions whose retry count is -1",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ResetDeletedBlockRetryCountSubcommand extends ScmSubcommand {

  @CommandLine.ArgGroup(multiplicity = "1")
  private TransactionsOption group;

  static class TransactionsOption {
    @CommandLine.Option(names = {"-a", "--all"},
        description = "reset all expired deleted block transaction retry" +
            " count from -1 to 0.")
    private boolean resetAll;

    @CommandLine.Option(names = {"-l", "--list"},
        split = ",",
        description = "reset the only given deletedBlock transaction ID" +
            " list, e.g 100,101,102.(Separated by ',')")
    private List<Long> txList;
  }

  @CommandLine.ParentCommand
  private ScmAdmin parent;

  @Override
  public void execute(ScmClient client) throws IOException {
    int count;
    if (group.resetAll) {
      count = client.resetDeletedBlockRetryCount(new ArrayList<>());
    } else {
      if (group.txList == null || group.txList.isEmpty()) {
        System.out.println("TransactionId list should not be empty");
        return;
      }
      count = client.resetDeletedBlockRetryCount(group.txList);
    }
    System.out.println("Reset " + count + " deleted block transactions in" +
        " SCM.");
  }
}
