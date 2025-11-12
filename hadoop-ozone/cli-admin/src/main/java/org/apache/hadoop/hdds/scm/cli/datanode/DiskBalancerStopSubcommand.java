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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import picocli.CommandLine.Command;

/**
 * Handler to stop disk balancer.
 */
@Command(
    name = "stop",
    description = "Stop DiskBalancer",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskBalancerStopSubcommand extends AbstractDiskBalancerSubCommand {

  @Override
  protected boolean executeCommand(String hostName) {
    try (DiskBalancerProtocol diskBalancerProxy = DiskBalancerSubCommandUtil
        .getSingleNodeDiskBalancerProxy(hostName)) {
      diskBalancerProxy.stopDiskBalancer();
      return true;
    } catch (IOException e) {
      System.err.printf("Error on node [%s]: %s%n", hostName, e.getMessage());
      return false;
    }
  }

  @Override
  protected void displayResults(List<String> successNodes, List<String> failedNodes) {
    if (isBatchMode()) {
      // Simpler message for batch mode
      if (!failedNodes.isEmpty()) {
        System.err.printf("Failed to stop DiskBalancer on nodes: [%s]%n",
            String.join(", ", failedNodes));
      } else {
        System.out.println("Stopped DiskBalancer on all IN_SERVICE nodes.");
      }
    } else {
      // Detailed message for specific nodes
      if (!successNodes.isEmpty()) {
        System.out.printf("Stopped DiskBalancer on nodes: [%s]%n", 
            String.join(", ", successNodes));
      }
      if (!failedNodes.isEmpty()) {
        System.err.printf("Failed to stop DiskBalancer on nodes: [%s]%n", 
            String.join(", ", failedNodes));
      }
    }
  }
}
