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
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import picocli.CommandLine.Command;

/**
 * Handler of ozone admin reconfig status command.
 */
@Command(
    name = "status",
    description = "Check reconfig status",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ReconfigureStatusSubcommand
    extends AbstractReconfigureSubCommand {

  @Override
  protected void executeCommand(HddsProtos.NodeType nodeType, String address) {
    try (ReconfigureProtocol reconfigProxy = ReconfigureSubCommandUtil
        .getSingleNodeReconfigureProxy(nodeType, address)) {
      String serverName = reconfigProxy.getServerName();
      ReconfigurationTaskStatus status = reconfigProxy.getReconfigureStatus();
      System.out.printf("%s: Reconfiguring status for node [%s]: ",
          serverName, address);
      printReconfigurationStatus(status);
    } catch (IOException e) {
      System.out.println("An error occurred while executing the command for :"
          + address);
      e.printStackTrace(System.out);
    }
  }

  private void printReconfigurationStatus(ReconfigurationTaskStatus status) {
    if (!status.hasTask()) {
      System.out.println("no task was found.");
      return;
    }
    System.out.print("started at " + new Date(status.getStartTime()));
    if (!status.stopped()) {
      System.out.println(" and is still running.");
      return;
    }
    System.out.printf(" and finished at %s.%n", new Date(status.getEndTime()));
    if (status.getStatus() == null) {
      // Nothing to report.
      return;
    }
    for (Map.Entry<ReconfigurationUtil.PropertyChange, Optional<String>>
        result : status.getStatus().entrySet()) {
      if (!result.getValue().isPresent()) {
        System.out.printf(
            "SUCCESS: Changed property %s%n\tFrom: \"%s\"%n\tTo: \"%s\"%n",
            result.getKey().prop, result.getKey().oldVal,
            result.getKey().newVal);
      } else {
        final String errorMsg = result.getValue().get();
        System.out.printf(
            "FAILED: Change property %s%n\tFrom: \"%s\"%n\tTo: \"%s\"%n",
            result.getKey().prop, result.getKey().oldVal,
            result.getKey().newVal);
        System.out.println("\tError: " + errorMsg + ".");
      }
    }
  }

}
