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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

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

  @CommandLine.Parameters(description = "One or more host names separated by spaces. " +
          "To read from stdin, specify '-' and supply the host names " +
          "separated by newlines.",
          arity = "1..*",
          paramLabel = "<host name>")
  private List<String> parameters = new ArrayList<>();

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<String> hosts;
    // Whether to read from stdin
    if (parameters.get(0).equals("-")) {
      hosts = new ArrayList<>();
      Scanner scanner = new Scanner(System.in, "UTF-8");
      while (scanner.hasNextLine()) {
        hosts.add(scanner.nextLine().trim());
      }
    } else {
      hosts = parameters;
    }
    List<DatanodeAdminError> errors = scmClient.recommissionNodes(hosts);
    System.out.println("Started recommissioning datanode(s):\n" +
        String.join("\n", hosts));
    if (errors.size() > 0) {
      for (DatanodeAdminError error : errors) {
        System.err.println("Error: " + error.getHostname() + ": "
            + error.getError());
      }
      // Throwing the exception will cause a non-zero exit status for the
      // command.
      throw new IOException(
          "Some nodes could be recommissioned");
    }
  }
}
