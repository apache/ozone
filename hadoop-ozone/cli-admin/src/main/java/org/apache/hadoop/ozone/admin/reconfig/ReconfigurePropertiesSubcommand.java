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

package org.apache.hadoop.ozone.admin.reconfig;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import picocli.CommandLine.Command;

/**
 * Handler of ozone admin reconfig properties command.
 */
@Command(
    name = "properties",
    description = "List reconfigurable properties",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ReconfigurePropertiesSubcommand
    extends AbstractReconfigureSubCommand {

  @Override
  protected void executeCommand(HddsProtos.NodeType nodeType, String address) {
    try (ReconfigureProtocol reconfigProxy = ReconfigureSubCommandUtil
        .getSingleNodeReconfigureProxy(nodeType, address)) {
      String serverName = reconfigProxy.getServerName();
      List<String> properties = reconfigProxy.listReconfigureProperties();
      System.out.printf("%s: Node [%s] Reconfigurable properties:%n",
          serverName, address);
      for (String name : properties) {
        System.out.println(name);
      }
    } catch (IOException e) {
      System.out.println("An error occurred while executing the command for :"
          + address);
      throw new RuntimeException(e);
    }
  }

}
