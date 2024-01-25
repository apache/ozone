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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests to validate the DecommissionStatusSubCommand class includes the
 * correct output when executed against a mock client.
 */
public class TestDecommissionStatusSubCommand {

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private DecommissionStatusSubCommand cmd;
  private List<HddsProtos.Node> nodes = getNodeDetails(2);
  private Map<String, List<ContainerID>> containerOnDecom = getContainersOnDecomNodes();
  private static HttpServer httpServer;
  private OzoneConfiguration conf = new OzoneConfiguration();

  @BeforeAll
  public static void setupScmHttp() throws Exception {
    httpServer = HttpServer.create(new InetSocketAddress(15000), 0);
    httpServer.createContext("/jmx", new HttpHandler() {
      public void handle(HttpExchange exchange) throws IOException {
        byte[] response = ("{  \"beans\" : [ {    " +
            "\"name\" : \"Hadoop:service=StorageContainerManager,name=NodeDecommissionMetrics\",    " +
            "\"modelerType\" : \"NodeDecommissionMetrics\",    \"DecommissioningMaintenanceNodesTotal\" : 0,    " +
            "\"RecommissionNodesTotal\" : 0,    \"PipelinesWaitingToCloseTotal\" : 0,    " +
            "\"ContainersUnderReplicatedTotal\" : 0,    \"ContainersUnClosedTotal\" : 0,    " +
            "\"ContainersSufficientlyReplicatedTotal\" : 0  } ]}").getBytes();
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
      }
    });
    httpServer.start();
  }
  @AfterAll
  public static void shutdownScmHttp() {
    if (httpServer != null) {
      httpServer.stop(0);
    }
  }

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    cmd = new DecommissionStatusSubCommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
    HttpConfig.Policy policy = HttpConfig.Policy.HTTP_ONLY;
    conf.set(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, policy.name());
    conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "localhost:15000");
    conf.set(ScmConfigKeys.OZONE_SCM_HTTP_BIND_HOST_KEY, "localhost");
    cmd.setParent(conf);
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testSuccessWhenDecommissionStatus() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.queryNode(any(), any(), any(), any()))
        .thenAnswer(invocation -> nodes); // 2 nodes decommissioning
    when(scmClient.getContainersOnDecomNode(any())).thenReturn(containerOnDecom);

    cmd.execute(scmClient);
    Pattern p = Pattern.compile("Decommission\\sStatus:\\s" +
            "DECOMMISSIONING\\s-\\s2\\snode\\(s\\)\n");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    // Both DN details are shown
    p = Pattern.compile("Datanode:\\s.*host0\\)");
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
    p = Pattern.compile("host0.*[\r\n].*UnderReplicated.*UnClosed", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
    p = Pattern.compile("Datanode:\\s.*host1\\)");
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
    p = Pattern.compile("host1.*[\r\n].*UnderReplicated.*UnClosed", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testNoNodesWhenDecommissionStatus() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    // No nodes in decommissioning. No error is printed
    when(scmClient.queryNode(any(), any(), any(), any()))
        .thenReturn(new ArrayList<>());
    when(scmClient.getContainersOnDecomNode(any())).thenReturn(new HashMap<>());
    cmd.execute(scmClient);

    Pattern p = Pattern.compile("Decommission\\sStatus:\\s" +
        "DECOMMISSIONING\\s-\\s0\\snode\\(s\\)\n");
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    // no host details are shown
    p = Pattern.compile("Datanode:\\s.*host0\\)", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertFalse(m.find());
    p = Pattern.compile("Datanode:\\s.*host1.\\)", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertFalse(m.find());
  }

  @Test
  public void testIdOptionDecommissionStatusSuccess() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.queryNode(any(), any(), any(), any()))
        .thenAnswer(invocation -> nodes); // 2 nodes decommissioning
    when(scmClient.getContainersOnDecomNode(any())).thenReturn(containerOnDecom);

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--id", nodes.get(0).getNodeID().getUuid());
    cmd.execute(scmClient); // check status of host0

    Pattern p = Pattern.compile("Datanode:\\s.*host0\\)", Pattern.MULTILINE);
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
    p = Pattern.compile("host0.*[\r\n].*UnderReplicated.*UnClosed", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    // as uuid of only host0 is passed, host1 should NOT be displayed
    p = Pattern.compile("Datanode:\\s.*host1.\\)", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertFalse(m.find());
    p = Pattern.compile("host1.*[\r\n].*UnderReplicated.*UnClosed", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertFalse(m.find());
  }

  @Test
  public void testIdOptionDecommissionStatusFail() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.queryNode(any(), any(), any(), any()))
        .thenAnswer(invocation -> nodes.subList(0, 1)); // host0 decommissioning
    when(scmClient.getContainersOnDecomNode(DatanodeDetails.getFromProtoBuf(nodes.get(0).getNodeID())))
        .thenReturn(containerOnDecom);
    when(scmClient.getContainersOnDecomNode(DatanodeDetails.getFromProtoBuf(nodes.get(1).getNodeID())))
        .thenReturn(new HashMap<>());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--id", nodes.get(1).getNodeID().getUuid());
    cmd.execute(scmClient); // check status of host1

    Pattern p = Pattern.compile("Datanode:\\s(.*)\\sis\\snot\\sin" +
            "\\sDECOMMISSIONING", Pattern.MULTILINE);
    Matcher m = p.matcher(errContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    // no host details are shown
    p = Pattern.compile("Datanode:\\s.*host0\\)", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertFalse(m.find());
    p = Pattern.compile("Datanode:\\s.*host1\\)", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertFalse(m.find());
  }

  @Test
  public void testIpOptionDecommissionStatusSuccess() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.queryNode(any(), any(), any(), any()))
        .thenAnswer(invocation -> nodes); // 2 nodes decommissioning
    when(scmClient.getContainersOnDecomNode(any())).thenReturn(containerOnDecom);

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--ip", nodes.get(1).getNodeID().getIpAddress());
    cmd.execute(scmClient); // check status of host1

    Pattern p = Pattern.compile("Datanode:\\s.*host1\\)", Pattern.MULTILINE);
    Matcher m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
    p = Pattern.compile("host1.*[\r\n].*UnderReplicated.*UnClosed", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    // as IpAddress of only host1 is passed, host0 should NOT be displayed
    p = Pattern.compile("Datanode:\\s.*host0.\\)", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertFalse(m.find());
    p = Pattern.compile("host0.*[\r\n].*UnderReplicated.*UnClosed", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertFalse(m.find());
  }

  @Test
  public void testIpOptionDecommissionStatusFail() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.queryNode(any(), any(), any(), any()))
        .thenAnswer(invocation -> nodes.subList(0, 1)); // host0 decommissioning
    when(scmClient.getContainersOnDecomNode(DatanodeDetails.getFromProtoBuf(nodes.get(0).getNodeID())))
        .thenReturn(containerOnDecom);
    when(scmClient.getContainersOnDecomNode(DatanodeDetails.getFromProtoBuf(nodes.get(1).getNodeID())))
        .thenReturn(new HashMap<>());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--ip", nodes.get(1).getNodeID().getIpAddress());
    cmd.execute(scmClient); // check status of host1

    Pattern p = Pattern.compile("Datanode:\\s(.*)\\sis\\snot\\sin" +
        "\\sDECOMMISSIONING", Pattern.MULTILINE);
    Matcher m = p.matcher(errContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());

    p = Pattern.compile("Datanode:\\s.*host0\\)", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertFalse(m.find());

    p = Pattern.compile("Datanode:\\s.*host1\\)", Pattern.MULTILINE);
    m = p.matcher(outContent.toString(DEFAULT_ENCODING));
    assertFalse(m.find());
  }


  private List<HddsProtos.Node> getNodeDetails(int n) {
    List<HddsProtos.Node> nodesList = new ArrayList<>();

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
      builder.addNodeOperationalStates(
          HddsProtos.NodeOperationalState.DECOMMISSIONING);
      builder.addNodeStates(HddsProtos.NodeState.HEALTHY);
      builder.setNodeID(dnd.build());
      nodesList.add(builder.build());
    }
    return nodesList;
  }

  private Map<String, List<ContainerID>> getContainersOnDecomNodes() {
    Map<String, List<ContainerID>> containerMap = new HashMap<>();
    List<ContainerID> underReplicated = new ArrayList<>();
    underReplicated.add(new ContainerID(1L));
    underReplicated.add(new ContainerID(2L));
    underReplicated.add(new ContainerID(3L));
    containerMap.put("UnderReplicated", underReplicated);
    List<ContainerID> unclosed = new ArrayList<>();
    unclosed.add(new ContainerID(10L));
    unclosed.add(new ContainerID(11L));
    unclosed.add(new ContainerID(12L));
    containerMap.put("UnClosed", unclosed);
    return containerMap;
  }

}
