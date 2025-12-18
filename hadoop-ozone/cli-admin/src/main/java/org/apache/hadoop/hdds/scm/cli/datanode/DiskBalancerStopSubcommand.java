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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
  protected Object executeCommand(String hostName) throws IOException {
    DiskBalancerProtocol diskBalancerProxy = DiskBalancerSubCommandUtil
        .getSingleNodeDiskBalancerProxy(hostName);
    try {
      diskBalancerProxy.stopDiskBalancer();
      
      // Get hostname for consistent JSON output
      String dnHostname = DiskBalancerSubCommandUtil.getDatanodeHostname(hostName);
      
      Map<String, Object> result = new LinkedHashMap<>();
      result.put("datanode", dnHostname);
      result.put("action", "stop");
      result.put("status", "success");
      return result;
    } finally {
      diskBalancerProxy.close();
    }
  }

  @Override
  protected void displayResults(List<String> successNodes, List<String> failedNodes) {
    // In JSON mode, results are already written, only show summary if needed
    if (getOptions().isJson()) {
      return;
    }

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

  @Override
  protected String getActionName() {
    return "stop";
  }
}
