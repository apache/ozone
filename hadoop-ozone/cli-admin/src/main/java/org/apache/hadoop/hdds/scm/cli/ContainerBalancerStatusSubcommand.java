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
    if (verboseWithHistory && !isVerbose()) {
      System.err.println("Warning: -H/--history has no effect without -v/--verbose.");
    }
    ContainerBalancerStatusInfoResponseProto response = scmClient.getContainerBalancerStatusInfo();
    boolean isRunning = response.getIsRunning();
    ContainerBalancerStatusInfoProto balancerStatusInfo = response.getContainerBalancerStatusInfo();
    if (isRunning) {
      System.out.println("ContainerBalancer is Running.");
    } else if (response.hasContainerBalancerStatusInfo()) {
      System.out.println("ContainerBalancer is Not Running.");
      printStopReasonAndMessage(balancerStatusInfo);
    } else {
      System.out.println("ContainerBalancer is Not Running.");
    }

    if (isVerbose() && response.hasContainerBalancerStatusInfo()) {
      printVerboseStatusInfo(balancerStatusInfo, isRunning);
    }
  }

  private void printVerboseStatusInfo(ContainerBalancerStatusInfoProto balancerStatusInfo, boolean isRunning) {
    Instant startedAtInstant = Instant.ofEpochSecond(balancerStatusInfo.getStartedAt());
    LocalDateTime startedAtDateTime =
        LocalDateTime.ofInstant(startedAtInstant, ZoneId.systemDefault());
    System.out.printf("Started at: %s %s%n",
        startedAtDateTime.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE),
        startedAtDateTime.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));

    Instant endInstant = balancerStatusInfo.hasStoppedAt()
        ? Instant.ofEpochSecond(balancerStatusInfo.getStoppedAt())
        : OffsetDateTime.now().toInstant();
    if (balancerStatusInfo.hasStoppedAt()) {
      LocalDateTime stoppedAtDateTime =
          LocalDateTime.ofInstant(endInstant, ZoneId.systemDefault());
      System.out.printf("Stopped at: %s %s%n",
          stoppedAtDateTime.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE),
          stoppedAtDateTime.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));
    }
    Duration balancingDuration = Duration.between(startedAtInstant, endInstant);
    System.out.printf("Balancing duration: %s%n%n", getPrettyDuration(balancingDuration));
    System.out.println(getConfigurationPrettyString(balancerStatusInfo.getConfiguration()));
    List<ContainerBalancerTaskIterationStatusInfoProto> iterationsStatusInfoList = 
        balancerStatusInfo.getIterationsStatusInfoList();

    ContainerBalancerTaskIterationStatusInfoProto lastIterationStatistic = null;
    if (isRunning) {
      System.out.println("Current iteration info:");
      ContainerBalancerTaskIterationStatusInfoProto currentIterationStatistic = iterationsStatusInfoList.stream()
          .filter(it -> it.getIterationResult().isEmpty())
          .findFirst()
          .orElse(null);
      if (currentIterationStatistic == null) {
        System.out.println("-");
        System.out.println();
      } else {
        System.out.println(
            getPrettyIterationStatusInfo(currentIterationStatistic)
        );
      }
    } else {
      System.out.println("Last iteration info:");
      lastIterationStatistic = iterationsStatusInfoList.stream()
          .filter(it -> !it.getIterationResult().isEmpty())
          .reduce((first, second) -> second)
          .orElse(null);
      if (lastIterationStatistic == null) {
        System.out.println("-");
        System.out.println();
      } else {
        System.out.println(
            getPrettyIterationStatusInfo(lastIterationStatistic)
        );
      }
    }

    if (verboseWithHistory) {
      System.out.println("Completed iteration history:");
      final int lastCompletedIterationNumber = lastIterationStatistic == null
          ? -1
          : lastIterationStatistic.getIterationNumber();
      String history = iterationsStatusInfoList
          .stream()
          .filter(it -> !it.getIterationResult().isEmpty())
          .filter(it -> isRunning || it.getIterationNumber() != lastCompletedIterationNumber)
          .map(this::getPrettyIterationStatusInfo)
          .collect(Collectors.joining(System.lineSeparator()));
      if (history.isEmpty()) {
        System.out.println("-");
      } else {
        System.out.println(history);
      }
      System.out.println();
    }
  }

  private void printStopReasonAndMessage(ContainerBalancerStatusInfoProto balancerStatusInfo) {
    if (balancerStatusInfo.hasStopReason()) {
      System.out.printf("Stop reason: %s%n", balancerStatusInfo.getStopReason());
    }
    if (balancerStatusInfo.hasStopMessage()) {
      System.out.printf("Message: %s%n", balancerStatusInfo.getStopMessage());
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
            configuration.getSizeMovedMaxPerIteration() / OzoneConsts.GB,
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
        .stream()
        .map(nodeInfo -> nodeInfo.getUuid() + " <- " + byteDesc(nodeInfo.getDataVolume()) + System.lineSeparator())
        .collect(Collectors.joining());
    if (enteringDataNodeList.isEmpty()) {
      enteringDataNodeList = " -" + System.lineSeparator();
    }
    String leavingDataNodeList = iterationStatusInfo.getSizeLeavingNodesList()
        .stream()
        .map(nodeInfo -> nodeInfo.getUuid() + " -> " + byteDesc(nodeInfo.getDataVolume()) + System.lineSeparator())
        .collect(Collectors.joining());
    if (leavingDataNodeList.isEmpty()) {
      leavingDataNodeList = " -" + System.lineSeparator();
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

