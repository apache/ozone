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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfo;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoResponseProto;
import org.apache.hadoop.hdds.scm.cli.ContainerBalancerStartSubcommand;
import org.apache.hadoop.hdds.scm.cli.ContainerBalancerStatusSubcommand;
import org.apache.hadoop.hdds.scm.cli.ContainerBalancerStopSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
  public void testContainerBalancerStatusInfoSubcommandRunning()
      throws IOException {
    ScmClient scmClient = mock(ScmClient.class);

    ContainerBalancerConfiguration config = new ContainerBalancerConfiguration();
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(20);
    config.setMaxSizeToMovePerIteration(53687091200L);
    config.setMaxSizeEnteringTarget(27917287424L);
    config.setMaxSizeLeavingSource(27917287424L);
    config.setIterations(2);
    config.setExcludeNodes("");
    config.setMoveTimeout(3900000);
    config.setMoveReplicationTimeout(3000000);
    config.setBalancingInterval(0);
    config.setIncludeNodes("");
    config.setExcludeNodes("");
    config.setNetworkTopologyEnable(false);
    config.setTriggerDuEnable(false);

    StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfo iteration0StatusInfo =
        StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfo.newBuilder()
            .setIterationNumber(0)
            .setIterationResult("ITERATION_COMPLETED")
            .setSizeScheduledForMoveGB(48)
            .setDataSizeMovedGB(48)
            .setContainerMovesScheduled(11)
            .setContainerMovesCompleted(11)
            .setContainerMovesFailed(0)
            .setContainerMovesTimeout(0)
            .addSizeEnteringNodesGB(
                StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
                    .setUuid("80f6bc27-e6f3-493e-b1f4-25f810ad960d")
                    .setDataVolumeGB(27)
                    .build()
            )
            .addSizeEnteringNodesGB(
                StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
                    .setUuid("701ca98e-aa1a-4b36-b817-e28ed634bba6")
                    .setDataVolumeGB(23L)
                    .build()
            )
            .addSizeLeavingNodesGB(
                StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
                    .setUuid("b8b9c511-c30f-4933-8938-2f272e307070")
                    .setDataVolumeGB(24L)
                    .build()
            )
            .addSizeLeavingNodesGB(
                StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
                    .setUuid("7bd99815-47e7-4015-bc61-ca6ef6dfd130")
                    .setDataVolumeGB(26L)
                    .build()
            )
            .build();
    StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfo iteration1StatusInfo =
        StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfo.newBuilder()
            .setIterationNumber(1)
            .setIterationResult("ITERATION_COMPLETED")
            .setSizeScheduledForMoveGB(48)
            .setDataSizeMovedGB(48)
            .setContainerMovesScheduled(11)
            .setContainerMovesCompleted(11)
            .setContainerMovesFailed(0)
            .setContainerMovesTimeout(0)
            .addSizeEnteringNodesGB(
                StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
                    .setUuid("80f6bc27-e6f3-493e-b1f4-25f810ad960d")
                    .setDataVolumeGB(27L)
                    .build()
            )
            .addSizeEnteringNodesGB(
                StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
                    .setUuid("701ca98e-aa1a-4b36-b817-e28ed634bba6")
                    .setDataVolumeGB(23L)
                    .build()
            )
            .addSizeLeavingNodesGB(
                StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
                    .setUuid("b8b9c511-c30f-4933-8938-2f272e307070")
                    .setDataVolumeGB(24L)
                    .build()
            )
            .addSizeLeavingNodesGB(
                StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
                    .setUuid("7bd99815-47e7-4015-bc61-ca6ef6dfd130")
                    .setDataVolumeGB(26L)
                    .build()
            )
            .build();
    StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfo iteration2StatusInfo =
        StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfo.newBuilder()
            .setIterationNumber(1)
            .setIterationResult("")
            .setSizeScheduledForMoveGB(48)
            .setDataSizeMovedGB(48)
            .setContainerMovesScheduled(11)
            .setContainerMovesCompleted(11)
            .setContainerMovesFailed(0)
            .setContainerMovesTimeout(0)
            .addSizeEnteringNodesGB(
                StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
                    .setUuid("80f6bc27-e6f3-493e-b1f4-25f810ad960d")
                    .setDataVolumeGB(27L)
                    .build()
            )
            .addSizeEnteringNodesGB(
                StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
                    .setUuid("701ca98e-aa1a-4b36-b817-e28ed634bba6")
                    .setDataVolumeGB(23L)
                    .build()
            )
            .addSizeLeavingNodesGB(
                StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
                    .setUuid("b8b9c511-c30f-4933-8938-2f272e307070")
                    .setDataVolumeGB(24L)
                    .build()
            )
            .addSizeLeavingNodesGB(
                StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
                    .setUuid("7bd99815-47e7-4015-bc61-ca6ef6dfd130")
                    .setDataVolumeGB(26L)
                    .build()
            )
            .build();
    ContainerBalancerStatusInfoResponseProto statusInfoResponseProto =
        ContainerBalancerStatusInfoResponseProto.newBuilder()
            .setIsRunning(true)
            .setContainerBalancerStatusInfo(ContainerBalancerStatusInfo.newBuilder()
                .setStartedAt(OffsetDateTime.now().toEpochSecond())
                .setConfiguration(config.toProtobufBuilder().setShouldRun(true))
                .addAllIterationsStatusInfo(
                    Arrays.asList(iteration0StatusInfo, iteration1StatusInfo, iteration2StatusInfo)
                )
            )

            .build();
    //test status is running
    when(scmClient.getContainerBalancerStatusInfo()).thenReturn(statusInfoResponseProto);

    statusCmd.execute(scmClient);
    Pattern p = Pattern.compile(
        "^ContainerBalancer\\sis\\sRunning.");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testContainerBalancerStatusInfoSubcommandRunningOnStoppedBalancer()
      throws IOException {
    ScmClient scmClient = mock(ScmClient.class);

    //test status is running
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
