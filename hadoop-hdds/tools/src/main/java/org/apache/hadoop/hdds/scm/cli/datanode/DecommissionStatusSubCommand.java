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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;

/**
 * Handler to print decommissioning nodes status.
 */
@CommandLine.Command(
    name = "decommission-status",
    description = "Show decommission status for datanodes",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)

public class DecommissionStatusSubCommand extends ScmSubcommand {

  @CommandLine.Option(names = { "--id" },
      description = "Show info by datanode UUID",
      defaultValue = "")
  private String uuid;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<HddsProtos.Node> decommissioningNodes;

    if (!Strings.isNullOrEmpty(uuid)) {
      decommissioningNodes = scmClient.queryNode(DECOMMISSIONING, null,
          HddsProtos.QueryScope.CLUSTER, "").stream().filter(p ->
          p.getNodeID().getUuid().equals(uuid)).collect(Collectors.toList());
      if (decommissioningNodes.isEmpty()) {
        System.err.println("Datanode: " + uuid + " is not in DECOMMISSIONING");
        return;
      }
    } else {
      decommissioningNodes = scmClient.queryNode(DECOMMISSIONING, null,
          HddsProtos.QueryScope.CLUSTER, "");
      System.out.println("\nDecommission Status: DECOMMISSIONING - " +
          decommissioningNodes.size() + " node(s)");
    }

    for (HddsProtos.Node node : decommissioningNodes) {
      DatanodeDetails datanode = DatanodeDetails.getFromProtoBuf(
          node.getNodeID());
      printDetails(datanode);
    }
  }
  private void printDetails(DatanodeDetails datanode)
      throws IOException {
    System.out.println("\nDatanode: " + datanode.getUuid().toString() +
        " (" + datanode.getNetworkLocation() + "/" + datanode.getIpAddress()
        + "/" + datanode.getHostName() + ")");
  }
}
