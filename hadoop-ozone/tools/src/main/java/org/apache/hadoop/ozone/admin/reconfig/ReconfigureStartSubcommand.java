/*
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
package org.apache.hadoop.ozone.admin.reconfig;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import picocli.CommandLine.Command;

import java.io.IOException;

/**
 * Handler of ozone admin reconfig start command.
 */
@Command(
    name = "start",
    description = "Start reconfig asynchronously",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ReconfigureStartSubcommand extends AbstractReconfigureSubCommand {

  @Override
  protected void executeCommand(String address) {
    try (ReconfigureProtocol reconfigProxy = ReconfigureSubCommandUtil
        .getSingleNodeReconfigureProxy(address)) {
      String serverName = reconfigProxy.getServerName();
      reconfigProxy.startReconfigure();
      System.out.printf("%s: Started reconfiguration task on node [%s].%n",
          serverName, address);
    } catch (IOException e) {
      System.out.println("An error occurred while executing the command for :"
          + address);
      e.printStackTrace(System.out);
    }
  }
}
