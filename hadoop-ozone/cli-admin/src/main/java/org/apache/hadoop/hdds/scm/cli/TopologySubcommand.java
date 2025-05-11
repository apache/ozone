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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.cli.AdminSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * Handler of printTopology command.
 */
@CommandLine.Command(
    name = "printTopology",
    description = "Print a tree of the network topology as reported by SCM",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
@MetaInfServices(AdminSubcommand.class)
public class TopologySubcommand extends ScmSubcommand
    implements AdminSubcommand {

  private static final List<HddsProtos.NodeState> STATES = new ArrayList<>();

  static {
    STATES.add(HEALTHY);
    STATES.add(STALE);
    STATES.add(DEAD);
  }

  @CommandLine.Option(names = {"-o", "--order"},
      description = "Print Topology ordered by network location")
  private boolean order;

  @CommandLine.Option(names = {"-f", "--full"},
      description = "Print Topology with full node infos")
  private boolean fullInfo;

  @CommandLine.Option(names = {"--operational-state"},
      description = "Only show datanodes in a specific operational state " +
          "(IN_SERVICE, DECOMMISSIONING, " +
          "DECOMMISSIONED, ENTERING_MAINTENANCE, " +
          "IN_MAINTENANCE)")
  private String nodeOperationalState;

  @CommandLine.Option(names = {"--node-state"},
      description = "Only show datanodes in a specific node state(" +
          " HEALTHY, STALE, DEAD)")
  private String nodeState;

  @CommandLine.Option(names = { "--json" },
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    for (HddsProtos.NodeState state : STATES) {
      List<HddsProtos.Node> nodes = scmClient.queryNode(null, state,
          HddsProtos.QueryScope.CLUSTER, "");
      if (nodes != null && !nodes.isEmpty()) {
        if (nodeOperationalState != null) {
          if (nodeOperationalState.equals("IN_SERVICE") ||
              nodeOperationalState.equals("DECOMMISSIONING") ||
              nodeOperationalState.equals("DECOMMISSIONED") ||
              nodeOperationalState.equals("ENTERING_MAINTENANCE") ||
              nodeOperationalState.equals("IN_MAINTENANCE")) {
            nodes = nodes.stream().filter(
                info -> info.getNodeOperationalStates(0).toString()
                    .equals(nodeOperationalState)).collect(Collectors.toList());
          } else {
            throw new InvalidPropertiesFormatException(
                "the nodeOperationalState isn't " +
                    "IN_SERVICE/DECOMMISSIONING/DECOMMISSIONED/" +
                    "ENTERING_MAINTENANCE/IN_MAINTENANCE " +
                    "the nodeOperationalState is " + nodeState);
          }
        }
        if (nodeState != null) {
          if (nodeState.equals("HEALTHY")
              || nodeState.equals("STALE")
              || nodeState.equals("DEAD")) {
            nodes = nodes.stream().filter(
                info -> info.getNodeStates(0).toString()
                    .equals(nodeState)).collect(Collectors.toList());
          } else {
            throw new InvalidPropertiesFormatException("the nodeState isn't " +
                "HEALTHY/STALE/DEAD the nodeState is " + nodeState);
          }
        }
        if (order) {
          printOrderedByLocation(nodes, state.toString());
        } else {
          printNodesWithLocation(nodes, state.toString());
        }
      }
    }
  }

  // Format
  // Location: rack1
  //  ipAddress(hostName) OperationalState
  private void printOrderedByLocation(List<HddsProtos.Node> nodes,
                                      String state) throws IOException {
    Map<String, TreeSet<DatanodeDetails>> tree = new HashMap<>();
    Map<DatanodeDetails, HddsProtos.NodeOperationalState> operationalState =
        new HashMap<>();
    for (HddsProtos.Node node : nodes) {
      String location = node.getNodeID().getNetworkLocation();
      if (location != null && !tree.containsKey(location)) {
        tree.put(location, new TreeSet<>());
      }
      DatanodeDetails dn = DatanodeDetails.getFromProtoBuf(node.getNodeID());
      tree.get(location).add(dn);
      operationalState.put(dn, node.getNodeOperationalStates(0));
    }
    ArrayList<String> locations = new ArrayList<>(tree.keySet());
    Collections.sort(locations);

    if (json) {
      List<NodeTopologyOrder> nodesJson = new ArrayList<>();
      locations.forEach(location -> {
        tree.get(location).forEach(n -> {
          NodeTopologyOrder nodeJson = new NodeTopologyOrder(n, state,
              operationalState.get(n).toString());
          nodesJson.add(nodeJson);
        });
      });
      System.out.println(
          JsonUtils.toJsonStringWithDefaultPrettyPrinter(nodesJson));
      return;
    }
    // show node state
    System.out.println("State = " + state);
    locations.forEach(location -> {
      System.out.println("Location: " + location);
      tree.get(location).forEach(n -> {
        System.out.println(" " + n.getIpAddress() + "(" + n.getHostName()
            + ") " + operationalState.get(n));
      });
    });
  }

  private String formatPortOutput(List<HddsProtos.Port> ports) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < ports.size(); i++) {
      HddsProtos.Port port = ports.get(i);
      sb.append(port.getName()).append('=').append(port.getValue());
      if (i < ports.size() - 1) {
        sb.append(',');
      }
    }
    return sb.toString();
  }

  private String getAdditionNodeOutput(HddsProtos.Node node) {
    return fullInfo ? node.getNodeID().getUuid() + "/" : "";
  }

  // Format "ipAddress(hostName):PortName1=PortValue1    OperationalState
  //     networkLocation
  private void printNodesWithLocation(Collection<HddsProtos.Node> nodes,
                                      String state) throws IOException {
    if (json) {
      if (fullInfo) {
        List<NodeTopologyFull> nodesJson = new ArrayList<>();
        nodes.forEach(node -> {
          NodeTopologyFull nodeJson =
              new NodeTopologyFull(
                  DatanodeDetails.getFromProtoBuf(node.getNodeID()), state);
          nodesJson.add(nodeJson);
        });
        System.out.println(
            JsonUtils.toJsonStringWithDefaultPrettyPrinter(nodesJson));
        return;
      }
      List<NodeTopologyDefault> nodesJson = new ArrayList<>();
      nodes.forEach(node -> {
        NodeTopologyDefault nodeJson = new NodeTopologyDefault(
            DatanodeDetails.getFromProtoBuf(node.getNodeID()), state);
        nodesJson.add(nodeJson);
      });
      System.out.println(
          JsonUtils.toJsonStringWithDefaultPrettyPrinter(nodesJson));
      return;
    }
    // show node state
    System.out.println("State = " + state);
    nodes.forEach(node -> {
      System.out.print(" " + getAdditionNodeOutput(node) +
          node.getNodeID().getIpAddress() + "(" +
          node.getNodeID().getHostName() + ")" +
          ":" + formatPortOutput(node.getNodeID().getPortsList()));
      System.out.println("    "
          + node.getNodeOperationalStates(0) + "    " +
          (node.getNodeID().getNetworkLocation() != null ?
              node.getNodeID().getNetworkLocation() : "NA"));
    });
  }

  private static class ListJsonSerializer extends
      JsonSerializer<List<DatanodeDetails.Port>> {
    @Override
    public void serialize(List<DatanodeDetails.Port> value, JsonGenerator jgen,
                          SerializerProvider provider)
        throws IOException {
      jgen.writeStartObject();
      for (DatanodeDetails.Port port : value) {
        jgen.writeNumberField(port.getName().toString(), port.getValue());
      }
      jgen.writeEndObject();
    }
  }

  private static class NodeTopologyOrder {
    private String ipAddress;
    private String hostName;
    private String nodeState;
    private String operationalState;
    private String networkLocation;

    NodeTopologyOrder(DatanodeDetails node, String state, String opState) {
      ipAddress = node.getIpAddress();
      hostName = node.getHostName();
      nodeState = state;
      operationalState = opState;
      networkLocation = (node.getNetworkLocation() != null ?
          node.getNetworkLocation() : "NA");
    }

    public String getIpAddress() {
      return ipAddress;
    }

    public String getHostName() {
      return hostName;
    }

    public String getNodeState() {
      return nodeState;
    }

    public String getOperationalState() {
      return operationalState;
    }

    public String getNetworkLocation() {
      return networkLocation;
    }
  }

  private static class NodeTopologyDefault extends NodeTopologyOrder {
    private List<DatanodeDetails.Port> ports;

    NodeTopologyDefault(DatanodeDetails node, String state) {
      super(node, state, node.getPersistedOpState().toString());
      ports = node.getPorts();
    }

    @JsonSerialize(using = ListJsonSerializer.class)
    public List<DatanodeDetails.Port> getPorts() {
      return ports;
    }
  }

  private static class NodeTopologyFull extends NodeTopologyDefault {
    private String uuid;

    NodeTopologyFull(DatanodeDetails node, String state) {
      super(node, state);
      uuid = node.getUuid().toString();
    }

    public String getUuid() {
      return uuid;
    }
  }
}
