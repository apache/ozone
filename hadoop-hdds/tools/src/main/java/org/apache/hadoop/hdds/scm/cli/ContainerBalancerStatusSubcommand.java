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
package org.apache.hadoop.hdds.scm.cli;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfo;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfo;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.OzoneConsts;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handler to query status of container balancer.
 */
@Command(
    name = "status",
    description = "Check if ContainerBalancer is running or not",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ContainerBalancerStatusSubcommand extends ScmSubcommand {

  @CommandLine.Option(names = {"-v", "--verbose"},
          description = "Verbose output. Show current iteration info.")
  private boolean verbose;

  @CommandLine.Option(names = {"-H", "--history"},
      description = "Verbose output with history. Show current iteration info and history of iterations. " +
          "Works only with -v.")
  private boolean verboseWithHistory;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    ContainerBalancerStatusInfoResponseProto response = scmClient.getContainerBalancerStatusInfo();
    boolean isRunning = response.getIsRunning();
    ContainerBalancerStatusInfo balancerStatusInfo = response.getContainerBalancerStatusInfo();
    if (isRunning) {
      LocalDateTime dateTime =
          LocalDateTime.ofInstant(Instant.ofEpochSecond(balancerStatusInfo.getStartedAt()), ZoneId.systemDefault());
      System.out.println("ContainerBalancer is Running.");

      if (verbose) {
        System.out.printf("Started at: %s %s%n%n", dateTime.toLocalDate(), dateTime.toLocalTime());
        System.out.println(getConfigurationPrettyString(balancerStatusInfo.getConfiguration()));
        List<ContainerBalancerTaskIterationStatusInfo> iterationsStatusInfoList
            = balancerStatusInfo.getIterationsStatusInfoList();

        System.out.println("Current iteration info:");
        System.out.println(
            getPrettyIterationStatusInfo(iterationsStatusInfoList.get(iterationsStatusInfoList.size() - 1))
        );

        if (verboseWithHistory) {
          System.out.println("Iteration history list:");
          System.out.println(
              iterationsStatusInfoList.stream().map(this::getPrettyIterationStatusInfo)
                  .collect(Collectors.joining("\n"))
          );
        }
      }

    } else {
      System.out.println("ContainerBalancer is Not Running.");
    }
  }

  String getConfigurationPrettyString(HddsProtos.ContainerBalancerConfigurationProto configuration) {
    return String.format("Container Balancer Configuration values:%n" +
                    "%-50s %s%n" +
                    "%-50s %s%n" +
                    "%-50s %d%n" +
                    "%-50s %dGB%n" +
                    "%-50s %dGB%n" +
                    "%-50s %dGB%n" +
                    "%-50s %d%n" +
                    "%-50s %dmin%n" +
                    "%-50s %dmin%n" +
                    "%-50s %dmin%n" +
                    "%-50s %s%n" +
                    "%-50s %s%n" +
                    "%-50s %s%n" +
                    "%-50s %s%n" +
                    "%-50s %s%n", "Key", "Value", "Threshold",
            configuration.getUtilizationThreshold(), "Max Datanodes to Involve per Iteration(percent)",
            configuration.getDatanodesInvolvedMaxPercentagePerIteration(),
            "Max Size to Move per Iteration",
            configuration.getDatanodesInvolvedMaxPercentagePerIteration() / OzoneConsts.GB,
            "Max Size Entering Target per Iteration",
            configuration.getSizeEnteringTargetMax() / OzoneConsts.GB,
            "Max Size Leaving Source per Iteration",
            configuration.getSizeLeavingSourceMax() / OzoneConsts.GB,
            "Number of Iterations",
            configuration.getIterations(),
            "Time Limit for Single Container's Movement",
            Duration.ofMillis(configuration.getMoveTimeout()).toMinutes(),
            "Time Limit for Single Container's Replication",
            Duration.ofMillis(configuration.getMoveReplicationTimeout()).toMinutes(),
            "Interval between each Iteration",
            Duration.ofMillis(configuration.getBalancingIterationInterval()).toMinutes(),
            "Whether to Enable Network Topology",
            configuration.getMoveNetworkTopologyEnable(),
            "Whether to Trigger Refresh Datanode Usage Info",
            configuration.getTriggerDuBeforeMoveEnable(),
            "Container IDs to Exclude from Balancing",
            configuration.getExcludeContainers().isEmpty() ? "None" : configuration.getExcludeContainers(),
            "Datanodes Specified to be Balanced",
            configuration.getIncludeDatanodes().isEmpty() ? "None" : configuration.getIncludeDatanodes(),
            "Datanodes Excluded from Balancing",
            configuration.getExcludeDatanodes().isEmpty() ? "None" : configuration.getExcludeDatanodes());
  }

  private String getPrettyIterationStatusInfo(ContainerBalancerTaskIterationStatusInfo iterationStatusInfo) {
    int iterationNumber = iterationStatusInfo.getIterationNumber();
    String iterationResult = iterationStatusInfo.getIterationResult();
    long sizeScheduledForMove = iterationStatusInfo.getSizeScheduledForMoveGB();
    long dataSizeMovedGB = iterationStatusInfo.getDataSizeMovedGB();
    long containerMovesScheduled = iterationStatusInfo.getContainerMovesScheduled();
    long containerMovesCompleted = iterationStatusInfo.getContainerMovesCompleted();
    long containerMovesFailed = iterationStatusInfo.getContainerMovesFailed();
    long containerMovesTimeout = iterationStatusInfo.getContainerMovesTimeout();
    String enteringDataNodeList = iterationStatusInfo.getSizeEnteringNodesGBList()
            .stream().map(nodeInfo -> nodeInfo.getUuid() + " <- " + nodeInfo.getDataVolumeGB() + "\n")
            .collect(Collectors.joining());
    String leavingDataNodeList = iterationStatusInfo.getSizeLeavingNodesGBList()
            .stream().map(nodeInfo -> nodeInfo.getUuid() + " -> " + nodeInfo.getDataVolumeGB() + "\n")
            .collect(Collectors.joining());
    return String.format(
            "%-50s %s%n" +
                    "%-50s %s%n" +
                    "%-50s %s%n" +
                    "%-50s %s%n" +
                    "%-50s %s%n" +
                    "%-50s %s%n" +
                    "%-50s %s%n" +
                    "%-50s %s%n" +
                    "%-50s %s%n" +
                    "%-50s %n%s" +
                    "%-50s %n%s",
            "Key", "Value",
            "Iteration number", iterationNumber,
            "Iteration result",
            iterationResult.isEmpty() ? "IN_PROGRESS" : iterationResult,
            "Size scheduled to move", sizeScheduledForMove,
            "Moved data size", dataSizeMovedGB,
            "Scheduled to move containers", containerMovesScheduled,
            "Already moved containers", containerMovesCompleted,
            "Failed to move containers", containerMovesFailed,
            "Failed to move containers by timeout", containerMovesTimeout,
            "Entered data to nodes", enteringDataNodeList,
            "Exited data from nodes", leavingDataNodeList);
  }
}

