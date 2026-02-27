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

package org.apache.hadoop.hdds.scm.cli;

import static org.apache.hadoop.hdds.util.DurationUtil.getPrettyDuration;
import static org.apache.hadoop.util.StringUtils.byteDesc;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfoProto;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.OzoneConsts;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Handler to query status of container balancer.
 */
@Command(
    name = "status",
    description = "Check if ContainerBalancer is running or not",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ContainerBalancerStatusSubcommand extends ScmSubcommand {

  @CommandLine.Option(names = {"-H", "--history"},
      description = "Verbose output with history. Show current iteration info and history of iterations. " +
          "Works only with -v.")
  private boolean verboseWithHistory;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    ContainerBalancerStatusInfoResponseProto response = scmClient.getContainerBalancerStatusInfo();
    boolean isRunning = response.getIsRunning();
    ContainerBalancerStatusInfoProto balancerStatusInfo = response.getContainerBalancerStatusInfo();
    if (isRunning) {
      Instant startedAtInstant = Instant.ofEpochSecond(balancerStatusInfo.getStartedAt());
      LocalDateTime dateTime =
          LocalDateTime.ofInstant(startedAtInstant, ZoneId.systemDefault());
      System.out.println("ContainerBalancer is Running.");

      if (isVerbose()) {
        System.out.printf("Started at: %s %s%n",
            dateTime.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE),
            dateTime.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));
        Duration balancingDuration = Duration.between(startedAtInstant, OffsetDateTime.now());
        System.out.printf("Balancing duration: %s%n%n", getPrettyDuration(balancingDuration));
        System.out.println(getConfigurationPrettyString(balancerStatusInfo.getConfiguration()));
        List<ContainerBalancerTaskIterationStatusInfoProto> iterationsStatusInfoList
            = balancerStatusInfo.getIterationsStatusInfoList();

        System.out.println("Current iteration info:");
        ContainerBalancerTaskIterationStatusInfoProto currentIterationStatistic = iterationsStatusInfoList.stream()
            .filter(it -> it.getIterationResult().isEmpty())
            .findFirst()
            .orElse(null);
        if (currentIterationStatistic == null) {
          System.out.println("-\n");
        } else {
          System.out.println(
              getPrettyIterationStatusInfo(currentIterationStatistic)
          );
        }


        if (verboseWithHistory) {
          System.out.println("Iteration history list:");
          System.out.println(
              iterationsStatusInfoList
                  .stream()
                  .filter(it -> !it.getIterationResult().isEmpty())
                  .map(this::getPrettyIterationStatusInfo)
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
            "Container IDs to Include in Balancing",
            configuration.getIncludeContainers().isEmpty() ? "None" : configuration.getIncludeContainers(),
            "Container IDs to Exclude from Balancing",
            configuration.getExcludeContainers().isEmpty() ? "None" : configuration.getExcludeContainers(),
            "Datanodes Specified to be Balanced",
            configuration.getIncludeDatanodes().isEmpty() ? "None" : configuration.getIncludeDatanodes(),
            "Datanodes Excluded from Balancing",
            configuration.getExcludeDatanodes().isEmpty() ? "None" : configuration.getExcludeDatanodes());

  }

  private String getPrettyIterationStatusInfo(ContainerBalancerTaskIterationStatusInfoProto iterationStatusInfo) {
    int iterationNumber = iterationStatusInfo.getIterationNumber();
    String iterationResult = iterationStatusInfo.getIterationResult();
    long iterationDuration = iterationStatusInfo.getIterationDuration();
    long sizeScheduledForMove = iterationStatusInfo.getSizeScheduledForMove();
    long dataSizeMoved = iterationStatusInfo.getDataSizeMoved();
    long containerMovesScheduled = iterationStatusInfo.getContainerMovesScheduled();
    long containerMovesCompleted = iterationStatusInfo.getContainerMovesCompleted();
    long containerMovesFailed = iterationStatusInfo.getContainerMovesFailed();
    long containerMovesTimeout = iterationStatusInfo.getContainerMovesTimeout();
    String enteringDataNodeList = iterationStatusInfo.getSizeEnteringNodesList()
            .stream().map(nodeInfo -> nodeInfo.getUuid() + " <- " + byteDesc(nodeInfo.getDataVolume()) + "\n")
            .collect(Collectors.joining());
    if (enteringDataNodeList.isEmpty()) {
      enteringDataNodeList = " -\n";
    }
    String leavingDataNodeList = iterationStatusInfo.getSizeLeavingNodesList()
            .stream().map(nodeInfo -> nodeInfo.getUuid() + " -> " + byteDesc(nodeInfo.getDataVolume()) + "\n")
            .collect(Collectors.joining());
    if (leavingDataNodeList.isEmpty()) {
      leavingDataNodeList = " -\n";
    }
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
                    "%-50s %s%n" +
                    "%-50s %n%s" +
                    "%-50s %n%s",
            "Key", "Value",
            "Iteration number", iterationNumber == 0 ? "-" : iterationNumber,
            "Iteration duration", getPrettyDuration(Duration.ofSeconds(iterationDuration)),
            "Iteration result",
            iterationResult.isEmpty() ? "-" : iterationResult,
            "Size scheduled to move", byteDesc(sizeScheduledForMove),
            "Moved data size", byteDesc(dataSizeMoved),
            "Scheduled to move containers", containerMovesScheduled,
            "Already moved containers", containerMovesCompleted,
            "Failed to move containers", containerMovesFailed,
            "Failed to move containers by timeout", containerMovesTimeout,
            "Entered data to nodes", enteringDataNodeList,
            "Exited data from nodes", leavingDataNodeList);
  }

}

