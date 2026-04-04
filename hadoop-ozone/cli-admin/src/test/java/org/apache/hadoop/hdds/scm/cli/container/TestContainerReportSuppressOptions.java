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

package org.apache.hadoop.hdds.scm.cli.container;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerListResult;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import picocli.CommandLine;

/**
 * Tests the suppress/unsuppress options in container report.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestContainerReportSuppressOptions {

  private ScmClient scmClient;
  private List<ContainerInfo> containers;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws IOException {
    scmClient = mock(ScmClient.class);
    MockDatanodeDetails.createDatanodeDetails(DatanodeID.randomID());
    containers = new ArrayList<>();

    containers.add(createEmptyContainer(1L)); // EMPTY container (0 keys, no replicas)
    containers.add(createMissingContainer(2L)); // MISSING container (has keys, no replicas)

    // Mock RM report
    when(scmClient.getReplicationManagerReport()).thenAnswer(inv -> createMockReport());

    // Mock listContainer
    when(scmClient.listContainer(anyLong(), anyInt(), eq(null), eq(null), eq(null), eq(true)))
        .thenAnswer(inv -> listSuppressedContainers());
    when(scmClient.listContainer(anyLong(), anyInt(), eq(null), eq(null), eq(null), eq(false)))
        .thenAnswer(inv -> listNonSuppressedContainers());

    // Mock suppress/unsuppress
    doReturn(Collections.emptyList()).when(scmClient).suppressContainers(anyList(), eq(true));
    doReturn(Collections.emptyList()).when(scmClient).suppressContainers(anyList(), eq(false));

    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  /**
   * Test container report shows empty and missing containers.
   */
  @Test
  @Order(1)
  public void testReportShowsEmptyAndMissingContainers() throws IOException {
    ReportSubcommand reportCmd = new ReportSubcommand();
    CommandLine c = new CommandLine(reportCmd);
    c.parseArgs();
    reportCmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("EMPTY: 1"));
    assertTrue(output.contains("MISSING: 1"));
  }

  /**
   * Test suppress missing container and check report.
   */
  @Test
  @Order(2)
  public void testSuppressMissingContainer() throws IOException {
    outContent.reset();
    ReportSubcommand reportCmd = new ReportSubcommand();
    CommandLine c = new CommandLine(reportCmd);
    c.parseArgs("--suppress", "2");
    reportCmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Suppressed container: 2"));

    containers.get(1).setSuppressed(true);

    outContent.reset();
    reportCmd = new ReportSubcommand();
    c = new CommandLine(reportCmd);
    c.parseArgs();
    reportCmd.execute(scmClient);

    output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("EMPTY: 1"));
    assertTrue(output.contains("MISSING: 0"));
  }

  /**
   * Test list suppressed containers from container list command.
   */
  @Test
  @Order(3)
  public void testListSuppressedContainers() throws IOException {
    containers.get(1).setSuppressed(true);

    ListSubcommand listCmd = new ListSubcommand();
    CommandLine c = new CommandLine(listCmd);
    c.parseArgs("--suppressed=true");

    outContent.reset();
    listCmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("\"containerID\" : 2"));
    assertFalse(output.contains("\"containerID\" : 1"));
  }

  /**
   * Test unsuppress missing container and check report.
   */
  @Test
  @Order(4)
  public void testUnsuppressContainer() throws IOException {
    containers.get(1).setSuppressed(true);

    outContent.reset();
    ReportSubcommand reportCmd = new ReportSubcommand();
    CommandLine c = new CommandLine(reportCmd);
    c.parseArgs("--unsuppress", "2");
    reportCmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Unsuppressed container: 2"));

    containers.get(1).setSuppressed(false);

    outContent.reset();
    reportCmd = new ReportSubcommand();
    c = new CommandLine(reportCmd);
    c.parseArgs();
    reportCmd.execute(scmClient);

    output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("MISSING: 1"));
  }

  /**
   * Test list unsuppressed containers from container list command.
   */
  @Test
  @Order(5)
  public void testListNonSuppressedContainers() throws IOException {
    containers.get(1).setSuppressed(false);

    // List only non-suppressed containers
    ListSubcommand listCmd = new ListSubcommand();
    CommandLine c = new CommandLine(listCmd);
    c.parseArgs("--suppressed=false");

    outContent.reset();
    listCmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("\"containerID\" : 1"));
    assertTrue(output.contains("\"containerID\" : 2"));
    assertFalse(output.contains("\"suppressed\" : true"));
  }
  
  /**
   * Create mock RM report based on current container states.
   */
  private ReplicationManagerReport createMockReport() {
    ReplicationManagerReport report = new ReplicationManagerReport(100);

    for (ContainerInfo container : containers) {
      if (container.isSuppressed()) {
        // Suppressed containers are filtered from the report
        continue;
      }

      // Determine health state based on container properties
      ContainerHealthState healthState;
      if (container.getNumberOfKeys() == 0) {
        healthState = ContainerHealthState.EMPTY;
        report.incrementAndSample(healthState, container);
      } else {
        healthState = ContainerHealthState.MISSING;
        report.incrementAndSample(healthState, container);
      }

      // Also increment lifecycle state
      report.increment(container.getState());
    }

    report.setComplete();
    return report;
  }

  /**
   * Create an EMPTY container (0 keys, no replicas).
   */
  private ContainerInfo createEmptyContainer(long containerID) {
    return new ContainerInfo.Builder()
        .setContainerID(containerID)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
        .setState(CLOSED)
        .setOwner("TestOwner")
        .setNumberOfKeys(0)  // Empty - 0 keys
        .setPipelineID(PipelineID.randomId())
        .build();
  }

  /**
   * Create a MISSING container (has keys but no replicas).
   */
  private ContainerInfo createMissingContainer(long containerID) {
    return new ContainerInfo.Builder()
        .setContainerID(containerID)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
        .setState(CLOSED)
        .setOwner("TestOwner")
        .setNumberOfKeys(100)  // Has keys
        .setPipelineID(PipelineID.randomId())
        .build();
  }

  private ContainerListResult listSuppressedContainers() {
    List<ContainerInfo> suppressed = new ArrayList<>();
    for (ContainerInfo container : containers) {
      if (container.isSuppressed()) {
        suppressed.add(container);
      }
    }
    return new ContainerListResult(suppressed, suppressed.size());
  }

  private ContainerListResult listNonSuppressedContainers() {
    List<ContainerInfo> nonSuppressed = new ArrayList<>();
    for (ContainerInfo container : containers) {
      if (!container.isSuppressed()) {
        nonSuppressed.add(container);
      }
    }
    return new ContainerListResult(nonSuppressed, nonSuppressed.size());
  }
}
