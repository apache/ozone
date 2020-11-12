/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;

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
@MetaInfServices(SubcommandWithParent.class)
public class TopologySubcommand extends ScmSubcommand
    implements SubcommandWithParent {

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

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    for (HddsProtos.NodeState state : STATES) {
      List<HddsProtos.Node> nodes = scmClient.queryNode(null, state,
          HddsProtos.QueryScope.CLUSTER, "");
      if (nodes != null && nodes.size() > 0) {
        // show node state
        System.out.println("State = " + state.toString());
        if (order) {
          printOrderedByLocation(nodes);
        } else {
          printNodesWithLocation(nodes);
        }
      }
    }
  }

  public Class<?> getParentType() {
    return OzoneAdmin.class;
  }

  // Format
  // Location: rack1
  //  ipAddress(hostName) OperationalState
  private void printOrderedByLocation(List<HddsProtos.Node> nodes) {
    HashMap<String, TreeSet<DatanodeDetails>> tree =
        new HashMap<>();
    HashMap<DatanodeDetails, HddsProtos.NodeOperationalState> state =
        new HashMap<>();

    for (HddsProtos.Node node : nodes) {
      String location = node.getNodeID().getNetworkLocation();
      if (location != null && !tree.containsKey(location)) {
        tree.put(location, new TreeSet<>());
      }
      DatanodeDetails dn = DatanodeDetails.getFromProtoBuf(node.getNodeID());
      tree.get(location).add(dn);
      state.put(dn, node.getNodeOperationalStates(0));
    }
    ArrayList<String> locations = new ArrayList<>(tree.keySet());
    Collections.sort(locations);

    locations.forEach(location -> {
      System.out.println("Location: " + location);
      tree.get(location).forEach(n -> {
        System.out.println(" " + n.getIpAddress() + "(" + n.getHostName()
            + ") "+state.get(n));
      });
    });
  }

  private String formatPortOutput(List<HddsProtos.Port> ports) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < ports.size(); i++) {
      HddsProtos.Port port = ports.get(i);
      sb.append(port.getName()).append("=").append(port.getValue());
      if (i < ports.size() - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  private String getAdditionNodeOutput(HddsProtos.Node node) {
    return fullInfo ? node.getNodeID().getUuid() + "/" : "";
  }

  // Format "ipAddress(hostName):PortName1=PortValue1    OperationalState
  //     networkLocation
  private void printNodesWithLocation(Collection<HddsProtos.Node> nodes) {
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
}