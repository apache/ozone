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
      description = "Stop DiskBalancer automatically after disk utilization is even.")
  private Boolean stopAfterDiskEven;

  @Override
  protected boolean executeCommand(String hostName) {
    try (DiskBalancerProtocol diskBalancerProxy = DiskBalancerSubCommandUtil
        .getSingleNodeDiskBalancerProxy(hostName)) {
      
      // Build configuration if any parameters are specified
      DiskBalancerConfigurationProto config = null;
      if (threshold != null || bandwidthInMB != null || 
          parallelThread != null || stopAfterDiskEven != null) {
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
        config = builder.build();
      }
      
      diskBalancerProxy.startDiskBalancer(config);
      return true;
    } catch (IOException e) {
      System.err.printf("Error on node [%s]: %s%n", hostName, e.getMessage());
      return false;
    }
  }

  @Override
  protected void displayResults(List<String> successNodes,
      List<String> failedNodes) {
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
}
