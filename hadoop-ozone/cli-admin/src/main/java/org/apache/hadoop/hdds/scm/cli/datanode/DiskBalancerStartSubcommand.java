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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerConfigurationProto;
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

  @Option(names = {"-t", "--threshold-percentage"},
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

  @Override
  protected Object executeCommand(String hostName) throws IOException {
    DiskBalancerProtocol diskBalancerProxy = DiskBalancerSubCommandUtil
        .getSingleNodeDiskBalancerProxy(hostName);
    try {
      DiskBalancerConfigurationProto config = buildConfigProto();
      diskBalancerProxy.startDiskBalancer(config);
      
      Map<String, Object> result = new LinkedHashMap<>();
      // Format datanode string with hostname if available
      String formattedDatanode = formatDatanodeDisplayName(hostName);
      result.put("datanode", formattedDatanode);
      result.put("action", "start");
      result.put("status", "success");
      Map<String, Object> configMap = getConfigurationMap();
      if (configMap != null && !configMap.isEmpty()) {
        result.put("configuration", configMap);
      }
      return result;
    } finally {
      diskBalancerProxy.close();
    }
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
  protected void displayResults(List<String> successNodes,
      List<String> failedNodes) {
    // In JSON mode, results are already written, only show summary if needed
    if (getOptions().isJson()) {
      return;
    }

    if (isBatchMode()) {
      if (!failedNodes.isEmpty()) {
        System.err.printf("Failed to start DiskBalancer on nodes: [%s]%n",
            String.join(", ", failedNodes));
      } else {
        System.out.println("Started DiskBalancer on all IN_SERVICE nodes.");
      }
    } else {
      // Detailed message for specific nodes
      if (!successNodes.isEmpty()) {
        System.out.printf("Started DiskBalancer on nodes: [%s]%n", 
            String.join(", ", successNodes));
      }
      if (!failedNodes.isEmpty()) {
        System.err.printf("Failed to start DiskBalancer on nodes: [%s]%n", 
            String.join(", ", failedNodes));
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
