/**
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
package org.apache.hadoop.hdds.scm.cli.datanode;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;

/**
 * Handler to print decommissioning nodes status.
 */
@CommandLine.Command(
    name = "decommission",
    description = "Show status of datanodes in DECOMMISSIONING",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)

public class DecommissionStatusSubCommand extends ScmSubcommand {

  @CommandLine.Option(names = { "--id" },
      description = "Show info by datanode UUID",
      defaultValue = "")
  private String uuid;

  @CommandLine.Option(names = { "--ip" },
      description = "Show info by datanode ipAddress",
      defaultValue = "")
  private String ipAddress;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<HddsProtos.Node> decommissioningNodes;
    Stream<HddsProtos.Node> allNodes = scmClient.queryNode(DECOMMISSIONING,
        null, HddsProtos.QueryScope.CLUSTER, "").stream();
    if (!Strings.isNullOrEmpty(uuid)) {
      decommissioningNodes = allNodes.filter(p -> p.getNodeID().getUuid()
          .equals(uuid)).collect(Collectors.toList());
      if (decommissioningNodes.isEmpty()) {
        System.err.println("Datanode: " + uuid + " is not in DECOMMISSIONING");
        return;
      }
    } else if (!Strings.isNullOrEmpty(ipAddress)) {
      decommissioningNodes = allNodes.filter(p -> p.getNodeID().getIpAddress()
          .compareToIgnoreCase(ipAddress) == 0).collect(Collectors.toList());
      if (decommissioningNodes.isEmpty()) {
        System.err.println("Datanode: " + ipAddress + " is not in " +
            "DECOMMISSIONING");
        return;
      }
    } else {
      decommissioningNodes = allNodes.collect(Collectors.toList());
      System.out.println("\nDecommission Status: DECOMMISSIONING - " +
          decommissioningNodes.size() + " node(s)");
    }

    for (HddsProtos.Node node : decommissioningNodes) {
      DatanodeDetails datanode = DatanodeDetails.getFromProtoBuf(
          node.getNodeID());
      printDetails(datanode);
    }
  }
  private void printDetails(DatanodeDetails datanode) {
    System.out.println("\nDatanode: " + datanode.getUuid().toString() +
        " (" + datanode.getNetworkLocation() + "/" + datanode.getIpAddress()
        + "/" + datanode.getHostName() + ")");
  }
}
