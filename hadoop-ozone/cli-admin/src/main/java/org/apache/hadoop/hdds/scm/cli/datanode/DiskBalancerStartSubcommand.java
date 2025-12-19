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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDiskBalancerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerConfigurationProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Handler to start disk balancer.
 */
@Command(
    name = "start",
    description = "Start DiskBalancer",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskBalancerStartSubcommand extends AbstractDiskBalancerSubCommand {

  @Option(names = {"-t", "--threshold"},
      description = "Percentage deviation from average utilization of " +
          "the disks after which a datanode will be rebalanced (for " +
          "example, '10' for 10%%).")
  private Double threshold;

  @Option(names = {"-b", "--bandwidth-in-mb"},
      description = "Maximum bandwidth for DiskBalancer per second.")
  private Long bandwidthInMB;

  @Option(names = {"-p", "--parallel-thread"},
      description = "Max parallelThread for DiskBalancer.")
  private Integer parallelThread;

  @Option(names = {"-s", "--stop-after-disk-even"},
      description = "Stop DiskBalancer automatically after disk utilization is even.",
      arity = "1")
  private Boolean stopAfterDiskEven;

  // Track nodes that are already running for display purposes (using Set to avoid duplicates)
  private final Set<String> alreadyRunningNodes = new LinkedHashSet<>();

  @Override
  protected Object executeCommand(String hostName) throws IOException {
    DiskBalancerProtocol diskBalancerProxy = DiskBalancerSubCommandUtil
        .getSingleNodeDiskBalancerProxy(hostName);
    try {
      // Check if DiskBalancer is already running before starting
      DatanodeDiskBalancerInfoProto status = diskBalancerProxy.getDiskBalancerInfo();
      String dnHostname = DiskBalancerSubCommandUtil.getDatanodeHostname(hostName);
      
      if (status.getRunningStatus() == DiskBalancerRunningStatus.RUNNING) {
        // Track this node as already running
        alreadyRunningNodes.add(dnHostname);
        
        // Return a skipped result
        return createJsonResult(dnHostname, "skipped",
            "DiskBalancer operation is already running.");
      }

      // Not running, proceed with start
      DiskBalancerConfigurationProto config = buildConfigProto();
      diskBalancerProxy.startDiskBalancer(config);

      // Return a success result
      return createJsonResult(dnHostname, "success", null);
    } finally {
      diskBalancerProxy.close();
    }
  }

  /**
   * Create a JSON result map for the start command.
   * 
   * @param hostname the datanode hostname
   * @param status the status ("success" or "skipped")
   * @param message optional message (for skipped status)
   * @return result map
   */
  private Map<String, Object> createJsonResult(String hostname, String status, String message) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("datanode", hostname);
    result.put("action", "start");
    result.put("status", status);
    if (message != null) {
      result.put("message", message);
    }
    Map<String, Object> configMap = getConfigurationMap();
    if (configMap != null && !configMap.isEmpty()) {
      result.put("configuration", configMap);
    }
    return result;
  }

  private DiskBalancerConfigurationProto buildConfigProto() {
    DiskBalancerConfigurationProto.Builder builder =
        DiskBalancerConfigurationProto.newBuilder();
    if (threshold != null) {
      builder.setThreshold(threshold);
    }
    if (bandwidthInMB != null) {
      builder.setDiskBandwidthInMB(bandwidthInMB);
    }
    if (parallelThread != null) {
      builder.setParallelThread(parallelThread);
    }
    if (stopAfterDiskEven != null) {
      builder.setStopAfterDiskEven(stopAfterDiskEven);
    }
    return builder.build();
  }

  @Override
  protected void displayResults(Set<String> successNodes,
      Set<String> failedNodes) {
    // In JSON mode, results are already written, only show summary if needed
    if (getOptions().isJson()) {
      return;
    }

    // Filter out skipped nodes from successNodes
    Set<String> actualSuccessNodes = new LinkedHashSet<>(successNodes);
    actualSuccessNodes.removeAll(alreadyRunningNodes);

    // Check if all nodes are already running (batch mode only)
    boolean allNodesAlreadyRunning = isBatchMode() && actualSuccessNodes.isEmpty() 
        && failedNodes.isEmpty() && !alreadyRunningNodes.isEmpty();

    if (allNodesAlreadyRunning) {
      System.out.println("DiskBalancer operation is already running on all IN_SERVICE and HEALTHY nodes.");
    } else {
      // Display warning for nodes that are already running (if not all)
      if (!alreadyRunningNodes.isEmpty()) {
        System.out.printf("DiskBalancer operation is already running on : [%s]%n",
            String.join(", ", alreadyRunningNodes));
      }

      if (isBatchMode()) {
        if (!failedNodes.isEmpty()) {
          System.err.printf("Failed to start DiskBalancer on nodes: [%s]%n",
              String.join(", ", failedNodes));
        }
        if (!actualSuccessNodes.isEmpty()) {
          System.out.println("Started DiskBalancer operation on all other IN_SERVICE and HEALTHY DNs.");
        }
      } else {
        if (!actualSuccessNodes.isEmpty()) {
          System.out.printf("Started DiskBalancer on nodes: [%s]%n", 
              String.join(", ", actualSuccessNodes));
        }
        if (!failedNodes.isEmpty()) {
          System.err.printf("Failed to start DiskBalancer on nodes: [%s]%n", 
              String.join(", ", failedNodes));
        }
      }
    }
  }

  @Override
  protected String getActionName() {
    return "start";
  }

  @Override
  protected Map<String, Object> getConfigurationMap() {
    Map<String, Object> configMap = new LinkedHashMap<>();
    if (threshold != null) {
      configMap.put("threshold", threshold);
    }
    if (bandwidthInMB != null) {
      configMap.put("bandwidthInMB", bandwidthInMB);
    }
    if (parallelThread != null) {
      configMap.put("parallelThread", parallelThread);
    }
    if (stopAfterDiskEven != null) {
      configMap.put("stopAfterDiskEven", stopAfterDiskEven);
    }
    return configMap.isEmpty() ? null : configMap;
  }
}
