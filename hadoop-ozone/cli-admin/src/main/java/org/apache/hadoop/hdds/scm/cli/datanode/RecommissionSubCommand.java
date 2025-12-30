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

package org.apache.hadoop.hdds.scm.cli.datanode;

import static org.apache.hadoop.hdds.scm.cli.datanode.DecommissionSubCommand.showErrors;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Recommission one or more datanodes.
 * Place decommissioned or maintenance nodes back into service.
 */
@Command(
    name = "recommission",
    description = "Return a datanode to service",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class RecommissionSubCommand extends ScmSubcommand {

  @CommandLine.Mixin
  private HostNameParameters hostNameParams;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<String> hosts = hostNameParams.getHostNames();
    List<DatanodeAdminError> errors = scmClient.recommissionNodes(hosts);
    System.out.println("Started recommissioning datanode(s):\n" +
        String.join("\n", hosts));
    showErrors(errors, "Some nodes could be recommissioned");
  }
}
