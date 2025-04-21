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

import static java.lang.System.err;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.utils.FormattingCLIUtils;
import picocli.CommandLine;

/**
 * Handler of scm status command.
 */
@CommandLine.Command(
    name = "roles",
    description = "List all SCMs, their respective Ratis server roles " +
        "and RaftPeerIds",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class GetScmRatisRolesSubcommand extends ScmSubcommand {

  @CommandLine.ParentCommand
  private ScmAdmin parent;

  @CommandLine.Option(names = { "--json" },
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  @CommandLine.Option(names = { "--table" },
       defaultValue = "false",
       description = "Format output as Table")
  private boolean table;

  private static final String SCM_ROLES_TITLE = "Storage Container Manager Roles";

  private static final List<String> SCM_ROLES_HEADER = Arrays.asList(
      "Host Name", "Ratis Port", "Role",  "Node ID", "Host Address");

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<String> peerRoles = scmClient.getScmRoles();
    if (json) {
      Map<String, Map<String, String>> scmRoles = parseScmRoles(peerRoles);
      System.out.print(
          JsonUtils.toJsonStringWithDefaultPrettyPrinter(scmRoles));
    } else if (table) {
      FormattingCLIUtils formattingCLIUtils = new FormattingCLIUtils(SCM_ROLES_TITLE);
      formattingCLIUtils.addHeaders(SCM_ROLES_HEADER);

      for (String role : peerRoles) {
        String[] roleItems = role.split(":");
        if (roleItems.length < 2) {
          err.println("Invalid response received for ScmRatisRoles.");
        }
        formattingCLIUtils.addLine(roleItems);
      }
      System.out.println(formattingCLIUtils.render());
    } else {
      for (String role: peerRoles) {
        System.out.println(role);
      }
    }
  }

  private Map<String, Map<String, String>> parseScmRoles(
      List<String> peerRoles) {
    Map<String, Map<String, String>> allRoles = new HashMap<>();
    for (String role : peerRoles) {
      Map<String, String> roleDetails = new HashMap<>();
      String[] roles = role.split(":");
      if (roles.length < 2) {
        err.println("Invalid response received for ScmRatisRoles.");
        return Collections.emptyMap();
      }
      // In case, there is no ratis, there is no ratis role.
      // This will just print the hostname with ratis port as the address
      roleDetails.put("address", roles[0].concat(":").concat(roles[1]));
      if (roles.length == 5) {
        roleDetails.put("raftPeerRole", roles[2]);
        roleDetails.put("ID", roles[3]);
        roleDetails.put("InetAddress", roles[4]);
      }
      allRoles.put(roles[0], roleDetails);
    }
    return allRoles;
  }
}
