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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDiskBalancerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerConfigurationProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import picocli.CommandLine;

/**
 * Unit tests to validate all DiskBalancer subcommands.
 */
public class TestDiskBalancerSubCommands {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private List<String> inServiceDatanodes;
  private DiskBalancerProtocol mockProtocol;
  private Random random = new Random();

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
    
    // Create shared list of in-service datanodes
    inServiceDatanodes = new ArrayList<>();
    inServiceDatanodes.add("host-1:19864");
    inServiceDatanodes.add("host-2:19864");
    inServiceDatanodes.add("host-3:19864");
    
    // Create shared mock protocol
    mockProtocol = mock(DiskBalancerProtocol.class);
  }
  
  /**
   * Helper class to hold all mocks needed for DiskBalancer tests.
   */
  private static class DiskBalancerMocks implements AutoCloseable {
    private final MockedConstruction<OzoneConfiguration> mockedConf;
    private final MockedConstruction<ContainerOperationClient> mockedClient;
    private final MockedStatic<DiskBalancerSubCommandUtil> mockedUtil;
    
    DiskBalancerMocks(
        MockedConstruction<OzoneConfiguration> mockedConf,
        MockedConstruction<ContainerOperationClient> mockedClient,
        MockedStatic<DiskBalancerSubCommandUtil> mockedUtil) {
      this.mockedConf = mockedConf;
      this.mockedClient = mockedClient;
      this.mockedUtil = mockedUtil;
    }
    
    @Override
    public void close() {
      if (mockedConf != null) {
        mockedConf.close();
      }
      if (mockedClient != null) {
        mockedClient.close();
      }
      if (mockedUtil != null) {
        mockedUtil.close();
      }
    }
  }
  
  /**
   * Helper method to set up all mocks needed for DiskBalancer tests.
   * Returns a DiskBalancerMocks object containing all three mocks.
   */
  private DiskBalancerMocks setupAllMocks() {
    MockedConstruction<OzoneConfiguration> mockedConf = 
        mockConstruction(OzoneConfiguration.class, (mock, context) -> {
          when(mock.getBoolean(
              eq(HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_KEY),
              eq(HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_DEFAULT)))
              .thenReturn(true);
        });
    
    MockedConstruction<ContainerOperationClient> mockedClient = 
        mockConstruction(ContainerOperationClient.class);
    
    MockedStatic<DiskBalancerSubCommandUtil> mockedUtil = 
        mockStatic(DiskBalancerSubCommandUtil.class);
    mockedUtil.when(() -> DiskBalancerSubCommandUtil
        .getAllOperableNodesClientRpcAddress(any()))
        .thenReturn(inServiceDatanodes);
    mockedUtil.when(() -> DiskBalancerSubCommandUtil
        .getSingleNodeDiskBalancerProxy(anyString()))
        .thenReturn(mockProtocol);
    // Mock getDatanodeHostAndIp(ScmClient, String) to return the address as-is for tests
    mockedUtil.when(() -> DiskBalancerSubCommandUtil
        .getDatanodeHostAndIp(any(), anyString()))
        .thenAnswer(invocation -> {
          String address = invocation.getArgument(1);
          return address; // Return address as-is for tests
        });
    // Mock extractHostIpAndPort to return test data
    mockedUtil.when(() -> DiskBalancerSubCommandUtil
        .extractHostIpAndPort(any(HddsProtos.DatanodeDetailsProto.class)))
        .thenAnswer(invocation -> {
          HddsProtos.DatanodeDetailsProto proto = invocation.getArgument(0);
          return new String[]{
              proto.getHostName(),
              proto.getIpAddress(),
              String.valueOf(19864)
          };
        });
    // Mock getDatanodeHostAndIp(String, String, int) to format the output
    // Return value is used by Mockito internally for mock setup
    mockedUtil.when(() -> {
      @SuppressWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
      String ignored = DiskBalancerSubCommandUtil
          .getDatanodeHostAndIp(anyString(), anyString(), anyInt());
      // Use the value to avoid "ignored return value" static analysis warnings.
      System.out.println(ignored);
    }).thenAnswer(invocation -> invocation.getArgument(0) + " (" +
        invocation.getArgument(1) + ":" +
        invocation.getArgument(2) + ")");
    
    return new DiskBalancerMocks(mockedConf, mockedClient, mockedUtil);
  }

  @AfterEach
  public void tearDown() {
    outContent.reset();
    errContent.reset();
  }

  // ========== DiskBalancerStartSubcommand Tests ==========

  @Test
  public void testStartDiskBalancerWithInServiceDatanodes() throws Exception {
    DiskBalancerStartSubcommand cmd = new DiskBalancerStartSubcommand();
    doNothing().when(mockProtocol).startDiskBalancer(any(DiskBalancerConfigurationProto.class));

    // Set up all required mocks
    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("--in-service-datanodes");
      cmd.call();

      String output = outContent.toString(DEFAULT_ENCODING);

      Pattern p = Pattern.compile("Started DiskBalancer on all IN_SERVICE nodes\\.");
      Matcher m = p.matcher(output);
      assertTrue(m.find());
    }
  }

  @Test
  public void testStartDiskBalancerWithConfiguration() throws Exception {
    DiskBalancerStartSubcommand cmd = new DiskBalancerStartSubcommand();
    doNothing().when(mockProtocol).startDiskBalancer(any(DiskBalancerConfigurationProto.class));

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("-t", "0.005", "-b", "100", "-p", "5", "-s", "false", "host-1");
      cmd.call();

      Pattern p = Pattern.compile("Started DiskBalancer on nodes: \\[host-1\\]");
      Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
  }

  @Test
  public void testStartDiskBalancerWithMultipleNodes() throws Exception {
    DiskBalancerStartSubcommand cmd = new DiskBalancerStartSubcommand();
    doNothing().when(mockProtocol).startDiskBalancer(any(DiskBalancerConfigurationProto.class));

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("host-1", "host-2", "host-3");
      cmd.call();

      Pattern p = Pattern.compile("Started DiskBalancer on nodes: \\[host-1, host-2, host-3\\]");
      Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
  }

  @Test
  public void testStartDiskBalancerWithStdin() throws Exception {
    DiskBalancerStartSubcommand cmd = new DiskBalancerStartSubcommand();
    doNothing().when(mockProtocol).startDiskBalancer(any(DiskBalancerConfigurationProto.class));

    String input = "host-1\nhost-2\nhost-3\n";
    System.setIn(new ByteArrayInputStream(input.getBytes(DEFAULT_ENCODING)));

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("-");
      cmd.call();

      Pattern p = Pattern.compile("Started DiskBalancer on nodes: \\[host-1, host-2, host-3\\]");
      Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
  }

  @Test
  public void testStartDiskBalancerWithJson() throws Exception {
    DiskBalancerStartSubcommand cmd = new DiskBalancerStartSubcommand();
    doNothing().when(mockProtocol).startDiskBalancer(any(DiskBalancerConfigurationProto.class));

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("--json", "-t", "0.005", "-b", "100", "host-1");
      cmd.call();

      String output = outContent.toString(DEFAULT_ENCODING);
      assertTrue(output.contains("\"datanode\""));
      assertTrue(output.contains("\"action\""));
      assertTrue(output.contains("\"status\""));
      assertTrue(output.contains("\"configuration\""));
      assertTrue(output.contains("\"threshold\""));
      assertTrue(output.contains("\"bandwidthInMB\""));
    }
  }

  @Test
  public void testStartDiskBalancerFailure() throws Exception {
    DiskBalancerStartSubcommand cmd = new DiskBalancerStartSubcommand();
    doThrow(new IOException("Connection failed")).when(mockProtocol)
        .startDiskBalancer(any(DiskBalancerConfigurationProto.class));

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("host-1");
      cmd.call();

      Pattern p = Pattern.compile("Failed to start DiskBalancer on nodes: \\[host-1\\]");
      Matcher m = p.matcher(errContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
  }

  // ========== DiskBalancerStopSubcommand Tests ==========

  @Test
  public void testStopDiskBalancerWithInServiceDatanodes() throws Exception {
    DiskBalancerStopSubcommand cmd = new DiskBalancerStopSubcommand();
    doNothing().when(mockProtocol).stopDiskBalancer();

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("--in-service-datanodes");
      cmd.call();

      Pattern p = Pattern.compile("Stopped DiskBalancer on all IN_SERVICE nodes\\.");
      Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
  }

  @Test
  public void testStopDiskBalancerWithJson() throws Exception {
    DiskBalancerStopSubcommand cmd = new DiskBalancerStopSubcommand();
    doNothing().when(mockProtocol).stopDiskBalancer();

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("--json", "host-1");
      cmd.call();

      String output = outContent.toString(DEFAULT_ENCODING);
      assertTrue(output.contains("\"datanode\""));
      assertTrue(output.contains("\"action\""));
      assertTrue(output.contains("\"stop\""));
      assertTrue(output.contains("\"status\""));
    }
  }

  @Test
  public void testStopDiskBalancerFailure() throws Exception {
    DiskBalancerStopSubcommand cmd = new DiskBalancerStopSubcommand();
    doThrow(new IOException("Stop failed")).when(mockProtocol).stopDiskBalancer();

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("host-1");
      cmd.call();

      Pattern p = Pattern.compile("Failed to stop DiskBalancer on nodes: \\[host-1\\]");
      Matcher m = p.matcher(errContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
  }

  // ========== DiskBalancerUpdateSubcommand Tests ==========

  @Test
  public void testUpdateDiskBalancerWithInServiceDatanodes() throws Exception {
    DiskBalancerUpdateSubcommand cmd = new DiskBalancerUpdateSubcommand();
    doNothing().when(mockProtocol).updateDiskBalancerConfiguration(any(DiskBalancerConfigurationProto.class));

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("--in-service-datanodes", "-t", "0.005", "-b", "100");
      cmd.call();

      Pattern p = Pattern.compile("Updated DiskBalancer configuration on all IN_SERVICE nodes\\.");
      Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
  }

  @Test
  public void testUpdateDiskBalancerWithAllParameters() throws Exception {
    DiskBalancerUpdateSubcommand cmd = new DiskBalancerUpdateSubcommand();
    doNothing().when(mockProtocol).updateDiskBalancerConfiguration(any(DiskBalancerConfigurationProto.class));

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("-t", "0.005", "-b", "100", "-p", "5", "-s", "false", "host-1");
      cmd.call();

      Pattern p = Pattern.compile("Updated DiskBalancer configuration on nodes: \\[host-1\\]");
      Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
  }

  @Test
  public void testUpdateDiskBalancerWithJson() throws Exception {
    DiskBalancerUpdateSubcommand cmd = new DiskBalancerUpdateSubcommand();
    doNothing().when(mockProtocol).updateDiskBalancerConfiguration(any(DiskBalancerConfigurationProto.class));

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("--json", "-t", "0.005", "-b", "100", "host-1");
      cmd.call();

      String output = outContent.toString(DEFAULT_ENCODING);
      assertTrue(output.contains("\"datanode\""));
      assertTrue(output.contains("\"action\""));
      assertTrue(output.contains("\"update\""));
      assertTrue(output.contains("\"configuration\""));
    }
  }

  @Test
  public void testUpdateDiskBalancerFailure() throws Exception {
    DiskBalancerUpdateSubcommand cmd = new DiskBalancerUpdateSubcommand();
    doThrow(new IOException("Update failed")).when(mockProtocol)
        .updateDiskBalancerConfiguration(any(DiskBalancerConfigurationProto.class));

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("-t", "0.005", "host-1");
      cmd.call();

      Pattern p = Pattern.compile("Failed to update DiskBalancer configuration on nodes: \\[host-1\\]");
      Matcher m = p.matcher(errContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
  }

  // ========== DiskBalancerStatusSubcommand Tests ==========

  @Test
  public void testStatusDiskBalancerWithInServiceDatanodes() throws Exception {
    DiskBalancerStatusSubcommand cmd = new DiskBalancerStatusSubcommand();
    
    // Generate random status protos matching the in-service datanodes (host-1, host-2, host-3)
    DatanodeDiskBalancerInfoProto statusProto1 = generateRandomStatusProto("host-1");
    DatanodeDiskBalancerInfoProto statusProto2 = generateRandomStatusProto("host-2");
    DatanodeDiskBalancerInfoProto statusProto3 = generateRandomStatusProto("host-3");
    
    when(mockProtocol.getDiskBalancerInfo())
        .thenReturn(statusProto1, statusProto2, statusProto3);

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("--in-service-datanodes");
      cmd.call();

      String output = outContent.toString(DEFAULT_ENCODING);
      assertTrue(output.contains("Status result"));
      assertTrue(output.contains("host-1"));
      assertTrue(output.contains("host-2"));
      assertTrue(output.contains("host-3"));
    }
  }

  @Test
  public void testStatusDiskBalancerWithJson() throws Exception {
    DiskBalancerStatusSubcommand cmd = new DiskBalancerStatusSubcommand();
    
    DatanodeDiskBalancerInfoProto statusProto = generateRandomStatusProto("host-1");
    
    when(mockProtocol.getDiskBalancerInfo())
        .thenReturn(statusProto);

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("--json", "host-1");
      cmd.call();

      String output = outContent.toString(DEFAULT_ENCODING);
      assertTrue(output.contains("\"datanode\""));
      assertTrue(output.contains("\"status\""));
      assertTrue(output.contains("\"threshold\""));
      assertTrue(output.contains("\"bandwidthInMB\""));
      assertTrue(output.contains("\"threads\""));
      assertTrue(output.contains("\"stopAfterDiskEven\""));
    }
  }

  @Test
  public void testStatusDiskBalancerWithMultipleNodes() throws Exception {
    DiskBalancerStatusSubcommand cmd = new DiskBalancerStatusSubcommand();
    
    DatanodeDiskBalancerInfoProto statusProto1 = generateRandomStatusProto("host-1");
    DatanodeDiskBalancerInfoProto statusProto2 = generateRandomStatusProto("host-2");
    
    when(mockProtocol.getDiskBalancerInfo())
        .thenReturn(statusProto1, statusProto2);

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("host-1", "host-2");
      cmd.call();

      String output = outContent.toString(DEFAULT_ENCODING);
      assertTrue(output.contains("host-1"));
      assertTrue(output.contains("host-2"));
    }
  }

  @Test
  public void testStatusDiskBalancerFailure() throws Exception {
    DiskBalancerStatusSubcommand cmd = new DiskBalancerStatusSubcommand();
    
    doThrow(new IOException("Status query failed")).when(mockProtocol)
        .getDiskBalancerInfo();

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("host-1");
      cmd.call();

      Pattern p = Pattern.compile("Failed to get DiskBalancer status from nodes: \\[host-1\\]");
      Matcher m = p.matcher(errContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
  }

  @Test
  public void testStatusDiskBalancerWithStdin() throws Exception {
    DiskBalancerStatusSubcommand cmd = new DiskBalancerStatusSubcommand();
    
    DatanodeDiskBalancerInfoProto statusProto1 = generateRandomStatusProto("host-1");
    DatanodeDiskBalancerInfoProto statusProto2 = generateRandomStatusProto("host-2");
    
    when(mockProtocol.getDiskBalancerInfo())
        .thenReturn(statusProto1, statusProto2);

    String input = "host-1\nhost-2\n";
    System.setIn(new ByteArrayInputStream(input.getBytes(DEFAULT_ENCODING)));

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("-");
      cmd.call();

      String output = outContent.toString(DEFAULT_ENCODING);
      assertTrue(output.contains("Status result"));
      assertTrue(output.contains("host-1"));
      assertTrue(output.contains("host-2"));
    }
  }

  // ========== DiskBalancerReportSubcommand Tests ==========

  @Test
  public void testReportDiskBalancerWithInServiceDatanodes() throws Exception {
    DiskBalancerReportSubcommand cmd = new DiskBalancerReportSubcommand();
    
    // Generate random report protos matching the in-service datanodes (host-1, host-2, host-3)
    DatanodeDiskBalancerInfoProto reportProto1 = generateRandomReportProto("host-1");
    DatanodeDiskBalancerInfoProto reportProto2 = generateRandomReportProto("host-2");
    DatanodeDiskBalancerInfoProto reportProto3 = generateRandomReportProto("host-3");

    when(mockProtocol.getDiskBalancerInfo())
        .thenReturn(reportProto1, reportProto2, reportProto3);

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("--in-service-datanodes");
      cmd.call();

      String output = outContent.toString(DEFAULT_ENCODING);
      assertTrue(output.contains("Report result"));
      assertTrue(output.contains("host-1"));
      assertTrue(output.contains("host-2"));
      assertTrue(output.contains("host-3"));
    }
  }

  @Test
  public void testReportDiskBalancerWithJson() throws Exception {
    DiskBalancerReportSubcommand cmd = new DiskBalancerReportSubcommand();

    DatanodeDiskBalancerInfoProto reportProto = generateRandomReportProto("host-1");

    when(mockProtocol.getDiskBalancerInfo())
        .thenReturn(reportProto);

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("--json", "host-1");
      cmd.call();

      String output = outContent.toString(DEFAULT_ENCODING);
      assertTrue(output.contains("\"datanode\""));
      assertTrue(output.contains("\"volumeDensity\""));
    }
  }

  @Test
  public void testReportDiskBalancerWithMultipleNodes() throws Exception {
    DiskBalancerReportSubcommand cmd = new DiskBalancerReportSubcommand();

    DatanodeDiskBalancerInfoProto reportProto1 = generateRandomReportProto("host-1");
    DatanodeDiskBalancerInfoProto reportProto2 = generateRandomReportProto("host-2");

    when(mockProtocol.getDiskBalancerInfo())
        .thenReturn(reportProto1, reportProto2);

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("host-1", "host-2");
      cmd.call();

      String output = outContent.toString(DEFAULT_ENCODING);
      assertTrue(output.contains("host-1"));
      assertTrue(output.contains("host-2"));
    }
  }

  @Test
  public void testReportDiskBalancerWithStdin() throws Exception {
    DiskBalancerReportSubcommand cmd = new DiskBalancerReportSubcommand();

    DatanodeDiskBalancerInfoProto reportProto1 = generateRandomReportProto("host-1");
    DatanodeDiskBalancerInfoProto reportProto2 = generateRandomReportProto("host-2");

    when(mockProtocol.getDiskBalancerInfo())
        .thenReturn(reportProto1, reportProto2);

    String input = "host-1\nhost-2\n";
    System.setIn(new ByteArrayInputStream(input.getBytes(DEFAULT_ENCODING)));

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("-");
      cmd.call();

      String output = outContent.toString(DEFAULT_ENCODING);
      assertTrue(output.contains("Report result"));
      assertTrue(output.contains("host-1"));
      assertTrue(output.contains("host-2"));
    }
  }

  @Test
  public void testReportDiskBalancerFailure() throws Exception {
    DiskBalancerReportSubcommand cmd = new DiskBalancerReportSubcommand();

    doThrow(new IOException("Report query failed")).when(mockProtocol)
        .getDiskBalancerInfo();

    try (DiskBalancerMocks mocks = setupAllMocks()) {

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("host-1");
      cmd.call();

      Pattern p = Pattern.compile("Failed to get DiskBalancer report from nodes: \\[host-1\\]");
      Matcher m = p.matcher(errContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private DatanodeDiskBalancerInfoProto createStatusProto(String hostname,
      DiskBalancerRunningStatus status, double threshold, long bandwidthInMB,
      int parallelThread, long successMove, long failureMove,
      long bytesMoved, long bytesToMove) {
    DatanodeDetailsProto nodeProto = DatanodeDetailsProto.newBuilder()
        .setHostName(hostname)
        .setIpAddress("127.0.0.1")
        .addPorts(HddsProtos.Port.newBuilder()
            .setName("CLIENT_RPC")
            .setValue(19864)
            .build())
        .build();

    DiskBalancerConfigurationProto configProto = DiskBalancerConfigurationProto.newBuilder()
        .setThreshold(threshold)
        .setDiskBandwidthInMB(bandwidthInMB)
        .setParallelThread(parallelThread)
        .setStopAfterDiskEven(true)
        .build();

    return DatanodeDiskBalancerInfoProto.newBuilder()
        .setNode(nodeProto)
        .setCurrentVolumeDensitySum(0.0)
        .setRunningStatus(status)
        .setDiskBalancerConf(configProto)
        .setSuccessMoveCount(successMove)
        .setFailureMoveCount(failureMove)
        .setBytesMoved(bytesMoved)
        .setBytesToMove(bytesToMove)
        .build();
  }

  /**
   * Generates a random status proto for a given hostname.
   * @param hostname the hostname
   * @return DatanodeDiskBalancerInfoProto with random status values
   */
  private DatanodeDiskBalancerInfoProto generateRandomStatusProto(String hostname) {
    DiskBalancerRunningStatus[] statuses = DiskBalancerRunningStatus.values();
    DiskBalancerRunningStatus status = statuses[random.nextInt(statuses.length)];
    double threshold = 0.001 + random.nextDouble() * 0.1;
    long bandwidthInMB = 10L + random.nextInt(990);
    int parallelThread = 1 + random.nextInt(20);
    long successMove = random.nextInt(1000);
    long failureMove = random.nextInt(100);
    long bytesMoved = random.nextLong() % (10L * 1024 * 1024 * 1024);
    long bytesToMove = random.nextLong() % (10L * 1024 * 1024 * 1024);

    return createStatusProto(hostname, status, threshold, bandwidthInMB,
        parallelThread, successMove, failureMove, bytesMoved, bytesToMove);
  }

  /**
   * Generates a random report proto for a given hostname.
   * @param hostname the hostname
   * @return DatanodeDiskBalancerInfoProto with random volume density
   */
  private DatanodeDiskBalancerInfoProto generateRandomReportProto(String hostname) {
    double volumeDensity = random.nextDouble() * 0.1;
    DatanodeDetailsProto nodeProto = DatanodeDetailsProto.newBuilder()
        .setHostName(hostname)
        .setIpAddress("127.0.0.1")
        .addPorts(HddsProtos.Port.newBuilder()
            .setName("CLIENT_RPC")
            .setValue(19864)
            .build())
        .build();

    return DatanodeDiskBalancerInfoProto.newBuilder()
        .setNode(nodeProto)
        .setCurrentVolumeDensitySum(volumeDensity)
        .build();
  }
}

