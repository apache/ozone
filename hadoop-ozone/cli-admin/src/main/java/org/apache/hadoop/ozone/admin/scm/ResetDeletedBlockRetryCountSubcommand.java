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

package org.apache.hadoop.ozone.admin.scm;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

/**
 * Handler of resetting expired deleted blocks from SCM side.
 */
@Deprecated
@CommandLine.Command(
    name = "reset",
    description = "DEPRECATED: Now we always retry the DeletedBlocksTxn." +
    " There is no concept of failed transactions, there is no need to reset delete block retry count.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ResetDeletedBlockRetryCountSubcommand extends ScmSubcommand {

  @CommandLine.ArgGroup(multiplicity = "1")
  private TransactionsOption group;

  static class TransactionsOption {
    @CommandLine.Option(names = {"-a", "--all"},
        description = "Reset all expired deleted block transaction retry" +
            " count from -1 to 0.")
    private boolean resetAll;

    @CommandLine.Option(names = {"-l", "--list"},
        split = ",",
        paramLabel = "txID",
        description = "Reset the only given deletedBlock transaction ID" +
            " list. Example: 100,101,102.(Separated by ',')")
    private List<Long> txList;

    @CommandLine.Option(names = {"-i", "--in"},
        description = "Use file as input, need to be JSON Array format and " +
            "contains multi \"txID\" key. Example: [{\"txID\":1},{\"txID\":2}]")
    private String fileName;
  }

  @Override
  public void execute(ScmClient client) throws IOException {
  }
}
