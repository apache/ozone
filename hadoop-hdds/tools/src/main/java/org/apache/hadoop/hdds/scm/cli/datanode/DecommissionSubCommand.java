/**
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
package org.apache.hadoop.hdds.scm.cli.datanode;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Decommission one or more datanodes.
 */
@Command(
    name = "decommission",
    description = "Decommission a datanode",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DecommissionSubCommand extends ScmSubcommand {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Parameters(description = "List of fully qualified host names")
  private List<String> hosts = new ArrayList<>();

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (hosts.size() > 0) {
      scmClient.decommissionNodes(hosts);
      System.out.println("Started decommissioning datanode(s):\n" +
          String.join("\n", hosts));
    } else {
      GenericCli.missingSubcommand(spec);
    }
  }
}
