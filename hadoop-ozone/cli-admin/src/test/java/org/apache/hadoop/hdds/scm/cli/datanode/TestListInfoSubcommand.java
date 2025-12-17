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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import picocli.CommandLine;

/**
 * Unit tests to validate the TestListInfoSubCommand class includes the
 * correct output when executed against a mock client.
 */
public class TestListInfoSubcommand {

  private ListInfoSubcommand cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private final ObjectMapper mapper = new ObjectMapper();
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    cmd = new ListInfoSubcommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testDataNodeOperationalStateAndHealthIncludedInOutput()
      throws Exception {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.queryNode(any(), any(), any(), any())).thenAnswer(invocation -> getNodeDetails());
    when(scmClient.listPipelines()).thenReturn(new ArrayList<>());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs();
    cmd.execute(scmClient);

    // The output should contain a string like:
    // <other lines>
    // Operational State: <STATE>
    // <other lines>
    Pattern p = Pattern.compile(
        "^Operational State:\\s+IN_SERVICE$", Pattern.MULTILINE);
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
    // Should also have a node with the state DECOMMISSIONING
    p = Pattern.compile(
        "^Operational State:\\s+DECOMMISSIONING$", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
    for (HddsProtos.NodeState state : HddsProtos.NodeState.values()) {
      p = Pattern.compile(
          "^Health State:\\s+" + state + "$", Pattern.MULTILINE);
      m = p.matcher(outContent.toString(DEFAULT_ENCODING));
      assertTrue(m.find());
    }
    // Ensure the nodes are ordered by health state HEALTHY,
    // HEALTHY_READONLY, STALE, DEAD
    p = Pattern.compile(".+HEALTHY.+STALE.+DEAD.+HEALTHY_READONLY.+",
        Pattern.DOTALL);

    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    // ----- with JSON flag -----
    outContent.reset();

    CommandLine cmdWithJsonFlag = new CommandLine(cmd);
    cmdWithJsonFlag.parseArgs("--json");
    cmd.execute(scmClient);

    String jsonOutput = outContent.toString(DEFAULT_ENCODING);
    JsonNode root;
    try {
      root = mapper.readTree(jsonOutput);
    } catch (IOException e) {
      fail("Invalid JSON output:\n" + jsonOutput + "\nError: " + e.getMessage());
      return;
    }

    assertTrue(root.isArray(), "JSON output should be an array");
    assertEquals(4, root.size(), "Expected 4 nodes in JSON output");

    List<String> healthStates = new ArrayList<>();
    List<String> operationalStates = new ArrayList<>();
    for (JsonNode node : root) {
      healthStates.add(node.get("healthState").asText());
      operationalStates.add(node.get("opState").asText());
    }

    // Check expected operational states are present
    List<String> expectedOpStates = Arrays.asList("IN_SERVICE", "DECOMMISSIONING");
    for (String expectedState : expectedOpStates) {
      assertTrue(operationalStates.contains(expectedState),
          "Expected operational state: " + expectedState + " but not found");
    }

    // Check all expected health states are present
    for (HddsProtos.NodeState state : HddsProtos.NodeState.values()) {
      assertTrue(healthStates.contains(state.toString()),
          "Expected health state: " + state + " but not found");
    }

    // Check order: HEALTHY -> STALE -> DEAD -> HEALTHY_READONLY
    List<String> expectedOrder = Arrays.asList("HEALTHY", "STALE", "DEAD", "HEALTHY_READONLY");
    int lastIndex = -1;
    for (String state : healthStates) {
      int index = expectedOrder.indexOf(state);
      assertTrue(index >= lastIndex, "Health states not in expected order: " + healthStates);
      lastIndex = index;
    }
  }

  @Test
  public void testDataNodeByUuidOutput()
      throws Exception {
    List<HddsProtos.Node> nodes = getNodeDetails();

    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.queryNode(any()))
        .thenAnswer(invocation -> nodes.get(0));
    when(scmClient.listPipelines())
        .thenReturn(new ArrayList<>());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--id", nodes.get(0).getNodeID().getUuid());
    cmd.execute(scmClient);

    Pattern p = Pattern.compile(
        "^Operational State:\\s+IN_SERVICE$", Pattern.MULTILINE);
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile(nodes.get(0).getNodeID().getUuid().toString(),
        Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    outContent.reset();

    // ----- with JSON flag -----
    CommandLine cmdWithJson = new CommandLine(cmd);
    cmdWithJson.parseArgs("--id", nodes.get(0).getNodeID().getUuid(), "--json");
    cmd.execute(scmClient);
    
    String jsonOutput = outContent.toString(DEFAULT_ENCODING);
    JsonNode root;
    try {
      root = mapper.readTree(jsonOutput);
    } catch (IOException e) {
      fail("Invalid JSON output:\n" + jsonOutput + "\nError: " + e.getMessage());
      return;
    }

    assertTrue(root.isArray(), "JSON output should be an array");
    assertEquals(1, root.size(), "Expected 1 node in JSON output");

    JsonNode node = root.get(0);
    String opState = node.get("opState").asText();
    String uuid = node.get("id").asText();

    assertEquals("IN_SERVICE", opState, "Expected opState IN_SERVICE but got: " + opState);
    assertEquals(nodes.get(0).getNodeID().getUuid(), uuid,
        "Expected UUID " + nodes.get(0).getNodeID().getUuid() + " but got: " + uuid);
  }

  @ParameterizedTest
  @CsvSource({
      "true, --most-used, descending",
      "false, --least-used, ascending"
  })
  public void testUsedOrderingAndOutput(
      boolean mostUsed,
      String cliFlag,
      String orderDirection) throws Exception {

    ScmClient scmClient = mock(ScmClient.class);
    List<HddsProtos.DatanodeUsageInfoProto> usageList = new ArrayList<>();
    List<HddsProtos.Node> nodeList = getNodeDetails();

    for (int i = 0; i < 4; i++) {
      long remaining = 1000 - (100 * i);
      usageList.add(HddsProtos.DatanodeUsageInfoProto.newBuilder()
          .setNode(nodeList.get(i).getNodeID())
          .setRemaining(remaining)
          .setCapacity(1000)
          .build());

      when(scmClient.queryNode(UUID.fromString(nodeList.get(i).getNodeID().getUuid())))
          .thenReturn(nodeList.get(i));
    }

    if (mostUsed) {
      Collections.reverse(usageList); // For most-used only
    }

    when(scmClient.getDatanodeUsageInfo(mostUsed, Integer.MAX_VALUE)).thenReturn(usageList);
    when(scmClient.listPipelines()).thenReturn(new ArrayList<>());

    // ----- JSON output test -----
    CommandLine c = new CommandLine(cmd);
    c.parseArgs(cliFlag, "--json");
    cmd.execute(scmClient);

    String jsonOutput = outContent.toString(DEFAULT_ENCODING);
    JsonNode root = new ObjectMapper().readTree(jsonOutput);

    assertTrue(root.isArray(), "JSON output should be an array");
    assertEquals(4, root.size(), "Expected 4 nodes in JSON output");

    for (JsonNode node : root) {
      assertTrue(node.has("used"), "Missing 'used'");
      assertTrue(node.has("capacity"), "Missing 'capacity'");
      assertTrue(node.has("percentUsed"), "Missing 'percentUsed'");
    }

    validateOrdering(root, orderDirection);

    outContent.reset();

    // ----- Text output test -----
    c = new CommandLine(cmd);
    c.parseArgs(cliFlag);
    cmd.execute(scmClient);

    String textOutput = outContent.toString(DEFAULT_ENCODING);
    validateOrderingFromTextOutput(textOutput, orderDirection);
  }

  @ParameterizedTest(name = "{0} and {1} should be mutually exclusive")
  @CsvSource({
      "--most-used, --node-id",
      "--most-used, --ip",
      "--most-used, --hostname",
      "--least-used, --node-id",
      "--least-used, --ip",
      "--least-used, --hostname"
  })
  public void testNodeSelectionAndUsageSortingAreMutuallyExclusive(String sortingFlag, String selectionFlag) {
    CommandLine c = new CommandLine(cmd);
    
    List<HddsProtos.Node> nodes = getNodeDetails();
    String nodeSelectionValue;
    if ("--node-id".equals(selectionFlag)) {
      nodeSelectionValue = nodes.get(0).getNodeID().getUuid();
    } else if ("--ip".equals(selectionFlag)) {
      nodeSelectionValue = "192.168.1.100";
    } else {
      nodeSelectionValue = "host-one";
    } 
    
    CommandLine.MutuallyExclusiveArgsException thrown = assertThrows(
        CommandLine.MutuallyExclusiveArgsException.class,
        () -> c.parseArgs(sortingFlag, selectionFlag, nodeSelectionValue),
        () -> String.format("Expected MutuallyExclusiveArgsException when combining %s and %s",
            sortingFlag, selectionFlag)
    );
    
    String expectedErrorMessagePart = "mutually exclusive";
    assertTrue(thrown.getMessage().contains(expectedErrorMessagePart),
        "Exception message should contain '" + expectedErrorMessagePart + "' but was: " + thrown.getMessage());
  }

  @Test
  public void testVolumeCounters() throws Exception {
    ScmClient scmClient = mock(ScmClient.class);
    List<HddsProtos.Node> nodes = getNodeDetails();

    // Create nodes with volume counts
    List<HddsProtos.Node> nodesWithVolumeCounts = new ArrayList<>();
    for (int i = 0; i < nodes.size(); i++) {
      HddsProtos.Node originalNode = nodes.get(i);
      HddsProtos.Node nodeWithVolumes = HddsProtos.Node.newBuilder(originalNode)
          .setTotalVolumeCount(10 + i)
          .setHealthyVolumeCount(8 + i)
          .build();
      nodesWithVolumeCounts.add(nodeWithVolumes);
    }

    when(scmClient.queryNode(any(), any(), any(), any())).thenReturn(nodesWithVolumeCounts);
    when(scmClient.listPipelines()).thenReturn(new ArrayList<>());

    // ----- JSON output test -----
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--json");
    cmd.execute(scmClient);
    JsonNode root = mapper.readTree(outContent.toString(DEFAULT_ENCODING));
    
    assertTrue(root.isArray(), "JSON output should be an array");
    assertEquals(4, root.size(), "Expected 4 nodes in JSON output");

    for (JsonNode node : root) {
      assertTrue(node.has("totalVolumeCount"), "JSON should include totalVolumeCount field");
      assertTrue(node.has("healthyVolumeCount"), "JSON should include healthyVolumeCount field");
    }

    outContent.reset();
    
    // ----- Text output test -----
    c = new CommandLine(cmd);
    c.parseArgs();
    cmd.execute(scmClient);
    String output = outContent.toString(DEFAULT_ENCODING);
    
    assertTrue(output.contains("Total volume count:"), "Should display total volume count");
    assertTrue(output.contains("Healthy volume count:"), "Should display healthy volume count");
  }

  private void validateOrdering(JsonNode root, String orderDirection) {
    for (int i = 0; i < root.size() - 1; i++) {
      long usedCurrent = root.get(i).get("used").asLong();
      long capacityCurrent = root.get(i).get("capacity").asLong();
      long usedNext = root.get(i + 1).get("used").asLong();
      long capacityNext = root.get(i + 1).get("capacity").asLong();
      double ratio1 = (capacityCurrent == 0) ? 0.0 : (double) usedCurrent / capacityCurrent;
      double ratio2 = (capacityNext == 0) ? 0.0 : (double) usedNext / capacityNext;

      if ("ascending".equals(orderDirection)) {
        assertTrue(ratio1 <= ratio2, "Expected ascending order, got: " + ratio1 + " > " + ratio2);
      } else {
        assertTrue(ratio1 >= ratio2, "Expected descending order, got: " + ratio1 + " < " + ratio2);
      }
    }
  }

  private void validateOrderingFromTextOutput(String output, String orderDirection) {
    Pattern usedPattern = Pattern.compile("Used: (\\d+)");
    Pattern capacityPattern = Pattern.compile("Capacity: (\\d+)");
    Matcher usedMatcher = usedPattern.matcher(output);
    Matcher capacityMatcher = capacityPattern.matcher(output);

    List<Double> usageRatios = new ArrayList<>();
    while (usedMatcher.find() && capacityMatcher.find()) {
      long used = Long.parseLong(usedMatcher.group(1));
      long capacity = Long.parseLong(capacityMatcher.group(1));
      double usage = (capacity == 0) ? 0.0 : (double) used / capacity;
      usageRatios.add(usage);
    }

    for (int i = 0; i < usageRatios.size() - 1; i++) {
      double ratio1 = usageRatios.get(i);
      double ratio2 = usageRatios.get(i + 1);
      if ("ascending".equals(orderDirection)) {
        assertTrue(ratio1 <= ratio2, "Expected ascending order, got: " + ratio1 + " > " + ratio2);
      } else {
        assertTrue(ratio1 >= ratio2, "Expected descending order, got: " + ratio1 + " < " + ratio2);
      }
    }
  }

  private List<HddsProtos.Node> getNodeDetails() {
    List<HddsProtos.Node> nodes = new ArrayList<>();

    for (int i = 0; i < 4; i++) {
      HddsProtos.DatanodeDetailsProto.Builder dnd =
          HddsProtos.DatanodeDetailsProto.newBuilder();
      dnd.setHostName("host" + i);
      dnd.setIpAddress("1.2.3." + i + 1);
      dnd.setNetworkLocation("/default");
      dnd.setNetworkName("host" + i);
      dnd.setUuid(UUID.randomUUID().toString());

      HddsProtos.Node.Builder builder  = HddsProtos.Node.newBuilder();
      if (i == 0) {
        builder.addNodeOperationalStates(
            HddsProtos.NodeOperationalState.IN_SERVICE);
        builder.addNodeStates(HddsProtos.NodeState.STALE);
      } else if (i == 1) {
        builder.addNodeOperationalStates(
            HddsProtos.NodeOperationalState.DECOMMISSIONING);
        builder.addNodeStates(HddsProtos.NodeState.DEAD);
      } else if (i == 2) {
        builder.addNodeOperationalStates(
            HddsProtos.NodeOperationalState.IN_SERVICE);
        builder.addNodeStates(HddsProtos.NodeState.HEALTHY_READONLY);
      } else {
        builder.addNodeOperationalStates(
            HddsProtos.NodeOperationalState.IN_SERVICE);
        builder.addNodeStates(HddsProtos.NodeState.HEALTHY);
      }
      builder.setNodeID(dnd.build());
      nodes.add(builder.build());
    }
    return nodes;
  }
}
