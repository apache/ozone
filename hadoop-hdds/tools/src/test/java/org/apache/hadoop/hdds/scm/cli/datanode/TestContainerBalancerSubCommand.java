/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli.datanode;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoResponseProto;
import org.apache.hadoop.hdds.scm.cli.ContainerBalancerStartSubcommand;
import org.apache.hadoop.hdds.scm.cli.ContainerBalancerStatusSubcommand;
import org.apache.hadoop.hdds.scm.cli.ContainerBalancerStopSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests to validate the ContainerBalancerSubCommand class includes the
 * correct output when executed against a mock client.
 */
class TestContainerBalancerSubCommand {

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private ContainerBalancerStopSubcommand stopCmd;
  private ContainerBalancerStartSubcommand startCmd;
  private ContainerBalancerStatusSubcommand statusCmd;

  private static ContainerBalancerStatusInfoResponseProto getContainerBalancerStatusInfoResponseProto(
      ContainerBalancerConfiguration config) {
    StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfoProto iteration1StatusInfo =
        StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfoProto.newBuilder()
            .setIterationNumber(1)
            .setIterationResult("ITERATION_COMPLETED")
            .setIterationDuration(400L)
            .setSizeScheduledForMove(54 * GB)
            .setDataSizeMoved(54 * GB)
            .setContainerMovesScheduled(11)
            .setContainerMovesCompleted(11)
            .setContainerMovesFailed(0)
            .setContainerMovesTimeout(0)
            .addSizeEnteringNodes(
                StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
                    .setUuid("80f6bc27-e6f3-493e-b1f4-25f810ad960d")
                    .setDataVolume(28 * GB)
                    .build()
            )
            .addSizeEnteringNodes(
                StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
                    .setUuid("701ca98e-aa1a-4b36-b817-e28ed634bba6")
                    .setDataVolume(26 * GB)
                    .build()
            )
            .addSizeLeavingNodes(
                StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
                    .setUuid("b8b9c511-c30f-4933-8938-2f272e307070")
                    .setDataVolume(25 * GB)
                    .build()
            )
            .addSizeLeavingNodes(
                StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
                    .setUuid("7bd99815-47e7-4015-bc61-ca6ef6dfd130")
                    .setDataVolume(29 * GB)
                    .build()
            )
            .build();
    StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfoProto iteration2StatusInfo =
        StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfoProto.newBuilder()
            .setIterationNumber(2)
            .setIterationResult("ITERATION_COMPLETED")
            .setIterationDuration(300L)
            .setSizeScheduledForMove(30 * GB)
            .setDataSizeMoved(30 * GB)
            .setContainerMovesScheduled(8)
            .setContainerMovesCompleted(8)
            .setContainerMovesFailed(0)
            .setContainerMovesTimeout(0)
            .addSizeEnteringNodes(
                StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
                    .setUuid("80f6bc27-e6f3-493e-b1f4-25f810ad960d")
                    .setDataVolume(20 * GB)
                    .build()
            )
            .addSizeEnteringNodes(
                StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
                    .setUuid("701ca98e-aa1a-4b36-b817-e28ed634bba6")
                    .setDataVolume(10 * GB)
                    .build()
            )
            .addSizeLeavingNodes(
                StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
                    .setUuid("b8b9c511-c30f-4933-8938-2f272e307070")
                    .setDataVolume(15 * GB)
                    .build()
            )
            .addSizeLeavingNodes(
                StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
                    .setUuid("7bd99815-47e7-4015-bc61-ca6ef6dfd130")
                    .setDataVolume(15 * GB)
                    .build()
            )
            .build();
    StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfoProto iteration3StatusInfo =
        StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfoProto.newBuilder()
            .setIterationNumber(3)
            .setIterationResult("")
            .setIterationDuration(370L)
            .setSizeScheduledForMove(48 * GB)
            .setDataSizeMoved(48 * GB)
            .setContainerMovesScheduled(5)
            .setContainerMovesCompleted(5)
            .setContainerMovesFailed(0)
            .setContainerMovesTimeout(0)
            .addSizeEnteringNodes(
                StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
                    .setUuid("80f6bc27-e6f3-493e-b1f4-25f810ad960d")
                    .setDataVolume(20 * GB)
                    .build()
            )
            .addSizeEnteringNodes(
                StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
                    .setUuid("701ca98e-aa1a-4b36-b817-e28ed634bba6")
                    .setDataVolume(28 * GB)
                    .build()
            )
            .addSizeLeavingNodes(
                StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
                    .setUuid("b8b9c511-c30f-4933-8938-2f272e307070")
                    .setDataVolume(30 * GB)
                    .build()
            )
            .addSizeLeavingNodes(
                StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
                    .setUuid("7bd99815-47e7-4015-bc61-ca6ef6dfd130")
                    .setDataVolume(18 * GB)
                    .build()
            )
            .build();
    return ContainerBalancerStatusInfoResponseProto.newBuilder()
            .setIsRunning(true)
            .setContainerBalancerStatusInfo(ContainerBalancerStatusInfoProto.newBuilder()
                .setStartedAt(OffsetDateTime.now().toEpochSecond())
                .setConfiguration(config.toProtobufBuilder().setShouldRun(true))
                .addAllIterationsStatusInfo(
                    Arrays.asList(iteration1StatusInfo, iteration2StatusInfo, iteration3StatusInfo)
                )
            )

            .build();
  }

  private static ContainerBalancerConfiguration getContainerBalancerConfiguration() {
    ContainerBalancerConfiguration config = new ContainerBalancerConfiguration();
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(20);
    config.setMaxSizeToMovePerIteration(53687091200L);
    config.setMaxSizeEnteringTarget(27917287424L);
    config.setMaxSizeLeavingSource(27917287424L);
    config.setIterations(3);
    config.setExcludeNodes("");
    config.setMoveTimeout(3900000);
    config.setMoveReplicationTimeout(3000000);
    config.setBalancingInterval(0);
    config.setIncludeNodes("");
    config.setExcludeNodes("");
    config.setNetworkTopologyEnable(false);
    config.setTriggerDuEnable(false);
    return config;
  }

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    stopCmd = new ContainerBalancerStopSubcommand();
    startCmd = new ContainerBalancerStartSubcommand();
    statusCmd = new ContainerBalancerStatusSubcommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  void testContainerBalancerStatusInfoSubcommandRunningWithoutFlags()
      throws IOException {
    ScmClient scmClient = mock(ScmClient.class);

    ContainerBalancerConfiguration config =
        getContainerBalancerConfiguration();

    ContainerBalancerStatusInfoResponseProto
        statusInfoResponseProto = getContainerBalancerStatusInfoResponseProto(config);
    //test status is running
    when(scmClient.getContainerBalancerStatusInfo()).thenReturn(statusInfoResponseProto);
    statusCmd.execute(scmClient);
    Pattern p = Pattern.compile(
        "^ContainerBalancer\\sis\\sRunning.");
    String output = outContent.toString(DEFAULT_ENCODING);
    Matcher m = p.matcher(output);
    assertTrue(m.find());

    String balancerConfigOutput =
        "Container Balancer Configuration values:\n" +
        "Key                                                Value\n" +
        "Threshold                                          10.0\n" +
        "Max Datanodes to Involve per Iteration(percent)    20\n" +
        "Max Size to Move per Iteration                     0GB\n" +
        "Max Size Entering Target per Iteration             26GB\n" +
        "Max Size Leaving Source per Iteration              26GB\n" +
        "Number of Iterations                               3\n" +
        "Time Limit for Single Container's Movement         65min\n" +
        "Time Limit for Single Container's Replication      50min\n" +
        "Interval between each Iteration                    0min\n" +
        "Whether to Enable Network Topology                 false\n" +
        "Whether to Trigger Refresh Datanode Usage Info     false\n" +
        "Container IDs to Exclude from Balancing            None\n" +
        "Datanodes Specified to be Balanced                 None\n" +
        "Datanodes Excluded from Balancing                  None";
    assertFalse(output.contains(balancerConfigOutput));

    String currentIterationOutput =
        "Current iteration info:\n" +
        "Key                                                Value\n" +
        "Iteration number                                   3\n" +
        "Iteration duration                                 1h 6m 40s\n" +
        "Iteration result                                   IN_PROGRESS\n" +
        "Size scheduled to move                             48 GB\n" +
        "Moved data size                                    48 GB\n" +
        "Scheduled to move containers                       11\n" +
        "Already moved containers                           11\n" +
        "Failed to move containers                          0\n" +
        "Failed to move containers by timeout               0\n" +
        "Entered data to nodes                              \n" +
        "80f6bc27-e6f3-493e-b1f4-25f810ad960d <- 20 GB\n" +
        "701ca98e-aa1a-4b36-b817-e28ed634bba6 <- 28 GB\n" +
        "Exited data from nodes                             \n" +
        "b8b9c511-c30f-4933-8938-2f272e307070 -> 30 GB\n" +
        "7bd99815-47e7-4015-bc61-ca6ef6dfd130 -> 18 GB";
    assertFalse(output.contains(currentIterationOutput));

    assertFalse(output.contains("Iteration history list:"));
  }

  @Test
  void testContainerBalancerStatusInfoSubcommandVerboseHistory()
      throws IOException {
    ScmClient scmClient = mock(ScmClient.class);

    ContainerBalancerConfiguration config =
        getContainerBalancerConfiguration();

    ContainerBalancerStatusInfoResponseProto
        statusInfoResponseProto = getContainerBalancerStatusInfoResponseProto(config);
    //test status is running
    when(scmClient.getContainerBalancerStatusInfo()).thenReturn(statusInfoResponseProto);
    CommandLine c = new CommandLine(statusCmd);
    c.parseArgs("--verbose", "--history");
    statusCmd.execute(scmClient);
    String output = outContent.toString(DEFAULT_ENCODING);
    Pattern p = Pattern.compile(
        "^ContainerBalancer\\sis\\sRunning.$", Pattern.MULTILINE);
    Matcher m = p.matcher(output);
    assertTrue(m.find());

    p = Pattern.compile(
        "^Started at: (\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})$", Pattern.MULTILINE);
    m = p.matcher(output);
    assertTrue(m.find());

    p = Pattern.compile(
        "^Balancing duration: \\d{1}s$", Pattern.MULTILINE);
    m = p.matcher(output);
    assertTrue(m.find());

    String balancerConfigOutput =
        "Container Balancer Configuration values:\n" +
        "Key                                                Value\n" +
        "Threshold                                          10.0\n" +
        "Max Datanodes to Involve per Iteration(percent)    20\n" +
        "Max Size to Move per Iteration                     0GB\n" +
        "Max Size Entering Target per Iteration             26GB\n" +
        "Max Size Leaving Source per Iteration              26GB\n" +
        "Number of Iterations                               3\n" +
        "Time Limit for Single Container's Movement         65min\n" +
        "Time Limit for Single Container's Replication      50min\n" +
        "Interval between each Iteration                    0min\n" +
        "Whether to Enable Network Topology                 false\n" +
        "Whether to Trigger Refresh Datanode Usage Info     false\n" +
        "Container IDs to Exclude from Balancing            None\n" +
        "Datanodes Specified to be Balanced                 None\n" +
        "Datanodes Excluded from Balancing                  None";
    assertTrue(output.contains(balancerConfigOutput));

    assertTrue(output.contains("Iteration history list:"));
    String firstHistoryIterationOutput =
        "Key                                                Value\n" +
        "Iteration number                                   3\n" +
        "Iteration duration                                 6m 10s\n" +
        "Iteration result                                   -\n" +
        "Size scheduled to move                             48 GB\n" +
        "Moved data size                                    48 GB\n" +
        "Scheduled to move containers                       5\n" +
        "Already moved containers                           5\n" +
        "Failed to move containers                          0\n" +
        "Failed to move containers by timeout               0\n" +
        "Entered data to nodes                              \n" +
        "80f6bc27-e6f3-493e-b1f4-25f810ad960d <- 20 GB\n" +
        "701ca98e-aa1a-4b36-b817-e28ed634bba6 <- 28 GB\n" +
        "Exited data from nodes                             \n" +
        "b8b9c511-c30f-4933-8938-2f272e307070 -> 30 GB\n" +
        "7bd99815-47e7-4015-bc61-ca6ef6dfd130 -> 18 GB";
    assertTrue(output.contains(firstHistoryIterationOutput));

    String secondHistoryIterationOutput =
        "Key                                                Value\n" +
        "Iteration number                                   2\n" +
        "Iteration duration                                 5m 0s\n" +
        "Iteration result                                   ITERATION_COMPLETED\n" +
        "Size scheduled to move                             30 GB\n" +
        "Moved data size                                    30 GB\n" +
        "Scheduled to move containers                       8\n" +
        "Already moved containers                           8\n" +
        "Failed to move containers                          0\n" +
        "Failed to move containers by timeout               0\n" +
        "Entered data to nodes                              \n" +
        "80f6bc27-e6f3-493e-b1f4-25f810ad960d <- 20 GB\n" +
        "701ca98e-aa1a-4b36-b817-e28ed634bba6 <- 10 GB\n" +
        "Exited data from nodes                             \n" +
        "b8b9c511-c30f-4933-8938-2f272e307070 -> 15 GB\n" +
        "7bd99815-47e7-4015-bc61-ca6ef6dfd130 -> 15 GB";
    assertTrue(output.contains(secondHistoryIterationOutput));
  }

  @Test
  void testContainerBalancerStatusInfoSubcommandVerbose()
      throws IOException {
    ScmClient scmClient = mock(ScmClient.class);

    ContainerBalancerConfiguration config =
        getContainerBalancerConfiguration();

    ContainerBalancerStatusInfoResponseProto
        statusInfoResponseProto = getContainerBalancerStatusInfoResponseProto(config);
    //test status is running
    when(scmClient.getContainerBalancerStatusInfo()).thenReturn(statusInfoResponseProto);
    CommandLine c = new CommandLine(statusCmd);
    c.parseArgs("--verbose");
    statusCmd.execute(scmClient);
    String output = outContent.toString(DEFAULT_ENCODING);
    Pattern p = Pattern.compile(
        "^ContainerBalancer\\sis\\sRunning.$", Pattern.MULTILINE);
    Matcher m = p.matcher(output);
    assertTrue(m.find());

    p = Pattern.compile(
        "^Started at: (\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})$", Pattern.MULTILINE);
    m = p.matcher(output);
    assertTrue(m.find());

    p = Pattern.compile(
        "^Balancing duration: \\d{1}s$", Pattern.MULTILINE);
    m = p.matcher(output);
    assertTrue(m.find());

    String balancerConfigOutput =
        "Container Balancer Configuration values:\n" +
        "Key                                                Value\n" +
        "Threshold                                          10.0\n" +
        "Max Datanodes to Involve per Iteration(percent)    20\n" +
        "Max Size to Move per Iteration                     0GB\n" +
        "Max Size Entering Target per Iteration             26GB\n" +
        "Max Size Leaving Source per Iteration              26GB\n" +
        "Number of Iterations                               3\n" +
        "Time Limit for Single Container's Movement         65min\n" +
        "Time Limit for Single Container's Replication      50min\n" +
        "Interval between each Iteration                    0min\n" +
        "Whether to Enable Network Topology                 false\n" +
        "Whether to Trigger Refresh Datanode Usage Info     false\n" +
        "Container IDs to Exclude from Balancing            None\n" +
        "Datanodes Specified to be Balanced                 None\n" +
        "Datanodes Excluded from Balancing                  None";
    assertTrue(output.contains(balancerConfigOutput));

    String currentIterationOutput =
        "Current iteration info:\n" +
        "Key                                                Value\n" +
        "Iteration number                                   3\n" +
        "Iteration duration                                 6m 10s\n" +
        "Iteration result                                   -\n" +
        "Size scheduled to move                             48 GB\n" +
        "Moved data size                                    48 GB\n" +
        "Scheduled to move containers                       5\n" +
        "Already moved containers                           5\n" +
        "Failed to move containers                          0\n" +
        "Failed to move containers by timeout               0\n" +
        "Entered data to nodes                              \n" +
        "80f6bc27-e6f3-493e-b1f4-25f810ad960d <- 20 GB\n" +
        "701ca98e-aa1a-4b36-b817-e28ed634bba6 <- 28 GB\n" +
        "Exited data from nodes                             \n" +
        "b8b9c511-c30f-4933-8938-2f272e307070 -> 30 GB\n" +
        "7bd99815-47e7-4015-bc61-ca6ef6dfd130 -> 18 GB";
    assertTrue(output.contains(currentIterationOutput));

    assertFalse(output.contains("Iteration history list:"));
  }

  @Test
  void testContainerBalancerStatusInfoSubcommandRunningOnStoppedBalancer()
      throws IOException {
    ScmClient scmClient = mock(ScmClient.class);

    //test status is not running
    when(scmClient.getContainerBalancerStatusInfo()).thenReturn(
        ContainerBalancerStatusInfoResponseProto.newBuilder()
            .setIsRunning(false)
            .build());

    statusCmd.execute(scmClient);
    Pattern p = Pattern.compile(
        "^ContainerBalancer\\sis\\sNot\\sRunning.");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  void testContainerBalancerStatusSubcommandNotRunning()
      throws IOException {
    ScmClient scmClient = mock(ScmClient.class);

    when(scmClient.getContainerBalancerStatusInfo()).thenReturn(
        ContainerBalancerStatusInfoResponseProto.newBuilder()
            .setIsRunning(false)
            .build());

    statusCmd.execute(scmClient);

    Pattern p = Pattern.compile(
        "^ContainerBalancer\\sis\\sNot\\sRunning.");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testContainerBalancerStopSubcommand() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    stopCmd.execute(scmClient);

    Pattern p = Pattern.compile("^Sending\\sstop\\scommand." +
        "\\sWaiting\\sfor\\sContainer\\sBalancer\\sto\\sstop...\\n" +
        "Container\\sBalancer\\sstopped.");

    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testContainerBalancerStartSubcommandWhenBalancerIsNotRunning()
      throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.startContainerBalancer(
        null, null, null, null, null, null, null, null, null, null, null, null))
        .thenReturn(
            StorageContainerLocationProtocolProtos
                .StartContainerBalancerResponseProto.newBuilder()
                .setStart(true)
                .build());
    startCmd.execute(scmClient);

    Pattern p = Pattern.compile("^Container\\sBalancer\\sstarted" +
        "\\ssuccessfully.");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testContainerBalancerStartSubcommandWhenBalancerIsRunning()
      throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.startContainerBalancer(
        null, null, null, null, null, null, null, null, null, null, null, null))
        .thenReturn(StorageContainerLocationProtocolProtos
            .StartContainerBalancerResponseProto.newBuilder()
            .setStart(false)
            .setMessage("")
            .build());
    startCmd.execute(scmClient);

    Pattern p = Pattern.compile("^Failed\\sto\\sstart\\sContainer" +
        "\\sBalancer.");

    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

}
