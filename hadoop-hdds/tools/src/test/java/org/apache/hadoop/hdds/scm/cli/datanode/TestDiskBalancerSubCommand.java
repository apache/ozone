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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Unit tests to validate the DiskBalancerSubCommand class includes the
 * correct output when executed against a mock client.
 */
public class TestDiskBalancerSubCommand {

  private DiskBalancerStopSubcommand stopCmd;
  private DiskBalancerStartSubcommand startCmd;
  private DiskBalancerUpdateSubcommand updateCmd;
  private DiskBalancerReportSubcommand reportCmd;
  private DiskBalancerStatusSubcommand statusCmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  private Random random = new Random();

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    stopCmd = new DiskBalancerStopSubcommand();
    startCmd = new DiskBalancerStartSubcommand();
    updateCmd = new DiskBalancerUpdateSubcommand();
    reportCmd = new DiskBalancerReportSubcommand();
    statusCmd = new DiskBalancerStatusSubcommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testDiskBalancerReportSubcommand()
      throws IOException  {
    ScmClient scmClient = mock(ScmClient.class);

    //test report
    Mockito.when(scmClient.getDiskBalancerReport(Mockito.any(Integer.class)))
        .thenReturn(generateReport(10));

    reportCmd.execute(scmClient);

    // 2 Headers + 10 results
    assertEquals(12, newLineCount(outContent.toString(DEFAULT_ENCODING)));
  }

  @Test
  public void testDiskBalancerStatusSubcommand()
      throws IOException  {
    ScmClient scmClient = mock(ScmClient.class);

    //test status
    Mockito.when(scmClient.getDiskBalancerStatus(Mockito.any(), Mockito.any()))
        .thenReturn(generateStatus(10));

    statusCmd.execute(scmClient);

    // 2 Headers + 10 results + 1 note
    assertEquals(13, newLineCount(outContent.toString(DEFAULT_ENCODING)));
  }

  @Test
  public void testDiskBalancerStartSubcommand() throws IOException  {
    startCmd.setAllHosts(true);
    ScmClient scmClient = mock(ScmClient.class);

    // Return error
    Mockito.when(scmClient.startDiskBalancer(Mockito.any(), Mockito.any(),
            Mockito.any(), Mockito.any()))
        .thenReturn(generateError(10));

    try {
      startCmd.execute(scmClient);
    } catch (IOException e) {
      assertEquals("Some nodes could not start DiskBalancer.", e.getMessage());
    }

    // Do not return error
    Mockito.when(scmClient.startDiskBalancer(Mockito.any(), Mockito.any(),
            Mockito.any(), Mockito.any()))
        .thenReturn(generateError(0));

    try {
      startCmd.execute(scmClient);
    } catch (IOException e) {
      fail("Should not catch exception here.");
    }

    startCmd.setAllHosts(false);
  }

  @Test
  public void testDiskBalancerUpdateSubcommand() throws IOException  {
    updateCmd.setAllHosts(true);
    ScmClient scmClient = mock(ScmClient.class);

    // Return error
    Mockito.when(scmClient.updateDiskBalancerConfiguration(Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(generateError(10));

    try {
      updateCmd.execute(scmClient);
    } catch (IOException e) {
      assertEquals("Some nodes could not update DiskBalancer.", e.getMessage());
    }

    // Do not return error
    Mockito.when(scmClient.updateDiskBalancerConfiguration(Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(generateError(0));

    try {
      updateCmd.execute(scmClient);
    } catch (IOException e) {
      fail("Should not catch exception here.");
    }

    updateCmd.setAllHosts(false);
  }

  @Test
  public void testDiskBalancerStopSubcommand() throws IOException  {
    stopCmd.setAllHosts(true);
    ScmClient scmClient = mock(ScmClient.class);

    // Return error
    Mockito.when(scmClient.stopDiskBalancer(Mockito.any()))
        .thenReturn(generateError(10));

    try {
      stopCmd.execute(scmClient);
    } catch (IOException e) {
      assertEquals("Some nodes could not stop DiskBalancer.", e.getMessage());
    }

    // Do not return error
    Mockito.when(scmClient.stopDiskBalancer(Mockito.any()))
        .thenReturn(generateError(0));

    try {
      stopCmd.execute(scmClient);
    } catch (IOException e) {
      fail("Should not catch exception here.");
    }

    stopCmd.setAllHosts(false);
  }

  public static Stream<Arguments> values() {
    return Stream.of(
        Arguments.arguments(0L, 0L, 0L, 0L, 0L),  // bytesMovedMB = 0, bytesToMoveMB = 0, estimatedTimeLeft = 0
        Arguments.arguments(512L, 512L, 1L, 1L, 1L), // bytesMoved and bytesToMove < 1MB should be rounded up to 1MB
        Arguments.arguments(5242880L, 10485760L, 5L, 10L, 1L), // bytesMoved = 5MB, bytesToMove = 10MB, estTimeLeft = 1
        Arguments.arguments(13774139392L, 3229900800L, 13137L, 3081L, 6L),
        Arguments.arguments(7482638336L, 939524096L, 7136L, 896L, 2L)
    );
  }

  @ParameterizedTest
  @MethodSource("values")
  public void testDiskBalancerStatusCalculations(long bytesMoved, long bytesToMove, long bytesMovedMB,
      long bytesToMoveMB, long estTimeLeft) throws IOException {
    ScmClient scmClient = mock(ScmClient.class);

    HddsProtos.DatanodeDiskBalancerInfoProto proto =
        HddsProtos.DatanodeDiskBalancerInfoProto.newBuilder()
            .setNode(generateDatanodeDetails())
            .setCurrentVolumeDensitySum(random.nextDouble())
            .setRunningStatus(HddsProtos.DiskBalancerRunningStatus.
                valueOf(random.nextInt(2) + 1))
            .setBytesMoved(bytesMoved)
            .setBytesToMove(bytesToMove)
            .setDiskBalancerConf(
                HddsProtos.DiskBalancerConfigurationProto.newBuilder()
                    .setDiskBandwidthInMB(10)
                    .setThreshold(10.0)
                    .setParallelThread(5)
                    .build())
            .build();

    List<HddsProtos.DatanodeDiskBalancerInfoProto> resultList = Collections.singletonList(proto);
    Mockito.when(scmClient.getDiskBalancerStatus(Mockito.any(), Mockito.any())).thenReturn(resultList);

    DiskBalancerStatusSubcommand statusCmd1 = new DiskBalancerStatusSubcommand();
    statusCmd1.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING).trim();
    String[] lines = output.split("\\n");

    // Skip the header and find the data row
    String[] columns = lines[2].split("\\s+");

    assertEquals(String.valueOf(bytesMovedMB), columns[7]);
    assertEquals(String.valueOf(bytesToMoveMB), columns[8]);
    assertEquals(estTimeLeft >= 0 ? String.valueOf(estTimeLeft) : "N/A", columns[9]);
  }

  private List<DatanodeAdminError> generateError(int count) {
    List<DatanodeAdminError> result = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      result.add(new DatanodeAdminError(UUID.randomUUID().toString(),
          "ERROR"));
    }
    return result;
  }

  private List<HddsProtos.DatanodeDiskBalancerInfoProto> generateReport(
      int count) {
    List<HddsProtos.DatanodeDiskBalancerInfoProto> result = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      result.add(generateReport());
    }
    return result;
  }

  private List<HddsProtos.DatanodeDiskBalancerInfoProto> generateStatus(
      int count) {
    List<HddsProtos.DatanodeDiskBalancerInfoProto> result = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      result.add(generateStatus());
    }
    return result;
  }

  private HddsProtos.DatanodeDiskBalancerInfoProto generateReport() {
    return HddsProtos.DatanodeDiskBalancerInfoProto.newBuilder()
        .setNode(generateDatanodeDetails())
        .setCurrentVolumeDensitySum(random.nextDouble())
        .build();
  }

  private HddsProtos.DatanodeDiskBalancerInfoProto generateStatus() {
    return HddsProtos.DatanodeDiskBalancerInfoProto.newBuilder()
        .setNode(generateDatanodeDetails())
        .setCurrentVolumeDensitySum(random.nextDouble())
        .setRunningStatus(HddsProtos.DiskBalancerRunningStatus.
            valueOf(random.nextInt(2) + 1))
        .setDiskBalancerConf(
            HddsProtos.DiskBalancerConfigurationProto.newBuilder().build())
        .build();
  }

  private HddsProtos.DatanodeDetailsProto generateDatanodeDetails() {
    return HddsProtos.DatanodeDetailsProto.newBuilder()
        .setHostName(UUID.randomUUID().toString())
        .setIpAddress("1.1.1.1")
        .build();
  }

  private int newLineCount(String str) {
    int res = 0;
    String[] lines = str.split("\n");
    for (String line : lines) {
      if (line.length() != 0) {
        res++;
      }
    }
    return res;
  }
}
