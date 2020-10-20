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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;

/**
 * Unit tests to validate the the TestListInfoSubCommand class includes the
 * correct output when executed against a mock client.
 */
public class TestListInfoSubcommand {

  private ListInfoSubcommand cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @Before
  public void setup() {
    cmd = new ListInfoSubcommand();
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @After
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testDataNodeOperationalStateIncludedInOutput() throws Exception {
    ScmClient scmClient = mock(ScmClient.class);
    Mockito.when(scmClient.queryNode(any(HddsProtos.NodeOperationalState.class),
        any(HddsProtos.NodeState.class), any(HddsProtos.QueryScope.class),
        Mockito.anyString()))
        .thenAnswer(invocation -> getNodeDetails());
    Mockito.when(scmClient.listPipelines())
        .thenReturn(new ArrayList<>());

    cmd.execute(scmClient);

    // The output should contain a string like:
    // <other lines>
    // Operational State: <STATE>
    // <other lines>
    Pattern p = Pattern.compile(
        "^Operational State:\\s+IN_SERVICE$", Pattern.MULTILINE);
    Matcher m = p.matcher(outContent.toString());
    assertTrue(m.find());
    // Should also have a node with the state DECOMMISSIONING
    p = Pattern.compile(
        "^Operational State:\\s+DECOMMISSIONING$", Pattern.MULTILINE);
    m = p.matcher(outContent.toString());
    assertTrue(m.find());
  }

  private List<HddsProtos.Node> getNodeDetails() {
    List<HddsProtos.Node> nodes = new ArrayList<>();

    for (int i=0; i<2; i++) {
      HddsProtos.DatanodeDetailsProto.Builder dnd =
          HddsProtos.DatanodeDetailsProto.newBuilder();
      dnd.setHostName("host" + i);
      dnd.setIpAddress("1.2.3." + i+1);
      dnd.setNetworkLocation("/default");
      dnd.setNetworkName("host" + i);
      dnd.addPorts(HddsProtos.Port.newBuilder()
          .setName("ratis").setValue(5678).build());
      dnd.setUuid(UUID.randomUUID().toString());

      HddsProtos.Node.Builder builder  = HddsProtos.Node.newBuilder();
      if (i == 0) {
        builder.addNodeOperationalStates(
            HddsProtos.NodeOperationalState.IN_SERVICE);
      } else {
        builder.addNodeOperationalStates(
            HddsProtos.NodeOperationalState.DECOMMISSIONING);
      }
      builder.addNodeStates(HddsProtos.NodeState.HEALTHY);
      builder.setNodeID(dnd.build());
      nodes.add(builder.build());
    }
    return nodes;
  }
}