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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.mockito.Mockito;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

/**
 * Unit tests to validate the DecommissionSubCommand class includes the
 * correct output when executed against a mock client.
 */
public class TestDecommissionStatusSubCommand {

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private DecommissionStatusSubCommand cmd;
  private DecommissionSubCommand decomCmd;

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    cmd = new DecommissionStatusSubCommand();
    decomCmd = new DecommissionSubCommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testSuccessWhenDecommissionStatus() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    Mockito.when(scmClient.queryNode(any(), any(), any(), any()))
        .thenAnswer(invocation -> getNodeDetails(2));

    CommandLine decom = new CommandLine(decomCmd);
    decom.parseArgs("host0", "host1");
    decomCmd.execute(scmClient);
    cmd.execute(scmClient);

    Pattern p = Pattern.compile("Decommission\\sStatus:\\s" +
            "DECOMMISSIONING\\s-\\s2\\snodes\n");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("^host0$", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("^host1$", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("Decommission\\sStatus:\\s" +
        "DECOMMISSIONED\\s-\\s\\d\\snodes\n");
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    Assert.assertFalse(m.find());
  }

  @Test
  public void testNoNodesWhenDecommissionStatus() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    // No nodes in decommissioning. No error is printed
    Mockito.when(scmClient.queryNode(any(), any(), any(), any()))
        .thenReturn(new ArrayList<>());

    cmd.execute(scmClient);
    Pattern p = Pattern.compile("Decommission\\sStatus:\\s" +
        "DECOMMISSIONING\\s-\\s0\\snodes\n");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testDecommissionedWhenDecommissionStatus() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    Mockito.when(scmClient.queryNode(any(), any(), any(), any()))
        .thenAnswer(invocation -> getNodeDetails(2));

    CommandLine decom = new CommandLine(decomCmd);
    decom.parseArgs("host1", "host2");
    decomCmd.execute(scmClient);
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--decommissioned");
    cmd.execute(scmClient);

    Pattern p = Pattern.compile("Decommission\\sStatus:\\s" +
        "DECOMMISSIONING\\s-\\s2\\snodes\n");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("Decommission\\sStatus:\\s" +
        "DECOMMISSIONED\\s-\\s\\d\\snodes\n");
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  private List<HddsProtos.Node> getNodeDetails(int n) {
    List<HddsProtos.Node> nodes = new ArrayList<>();

    for (int i = 0; i < n; i++) {
      HddsProtos.DatanodeDetailsProto.Builder dnd =
          HddsProtos.DatanodeDetailsProto.newBuilder();
      dnd.setHostName("host" + i);
      dnd.setIpAddress("1.2.3." + i + 1);
      dnd.setNetworkLocation("/default");
      dnd.setNetworkName("host" + i);
      dnd.addPorts(HddsProtos.Port.newBuilder()
          .setName("ratis").setValue(5678).build());
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
