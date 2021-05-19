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
package org.apache.hadoop.hdds.scm.cli.datanode;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import picocli.CommandLine;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Handler of list datanodes info command.
 */
@CommandLine.Command(
    name = "list",
    description = "List info of datanodes",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ListInfoSubcommand extends ScmSubcommand {

  @CommandLine.Option(names = {"--ip"},
      description = "Show info by ip address.",
      defaultValue = "")
  private String ipaddress;

  @CommandLine.Option(names = {"--id"},
      description = "Show info by datanode UUID.",
      defaultValue = "")
  private String uuid;

  private List<Pipeline> pipelines;


  @Override
  public void execute(ScmClient scmClient) throws IOException {
    pipelines = scmClient.listPipelines();
    if (Strings.isNullOrEmpty(ipaddress) && Strings.isNullOrEmpty(uuid)) {
      getAllNodes(scmClient).forEach(this::printDatanodeInfo);
    } else {
      Stream<DatanodeWithAttributes> allNodes = getAllNodes(scmClient).stream();
      if (!Strings.isNullOrEmpty(ipaddress)) {
        allNodes = allNodes.filter(p -> p.getDatanodeDetails().getIpAddress()
            .compareToIgnoreCase(ipaddress) == 0);
      }
      if (!Strings.isNullOrEmpty(uuid)) {
        allNodes = allNodes.filter(p ->
            p.getDatanodeDetails().getUuidString().equals(uuid));
      }
      allNodes.forEach(this::printDatanodeInfo);
    }
  }

  private List<DatanodeWithAttributes> getAllNodes(ScmClient scmClient)
      throws IOException {
    List<HddsProtos.Node> nodes = scmClient.queryNode(null,
        null, HddsProtos.QueryScope.CLUSTER, "");

    return nodes.stream()
        .map(p -> new DatanodeWithAttributes(
            DatanodeDetails.getFromProtoBuf(p.getNodeID()),
            p.getNodeOperationalStates(0), p.getNodeStates(0)))
        .sorted((o1, o2) -> o1.healthState.compareTo(o2.healthState))
        .collect(Collectors.toList());
  }

  private void printDatanodeInfo(DatanodeWithAttributes dna) {
    StringBuilder pipelineListInfo = new StringBuilder();
    DatanodeDetails datanode = dna.getDatanodeDetails();
    int relatedPipelineNum = 0;
    if (!pipelines.isEmpty()) {
      List<Pipeline> relatedPipelines = pipelines.stream().filter(
          p -> p.getNodes().contains(datanode)).collect(Collectors.toList());
      if (relatedPipelines.isEmpty()) {
        pipelineListInfo.append("No related pipelines" +
            " or the node is not in Healthy state.\n");
      } else {
        relatedPipelineNum = relatedPipelines.size();
        relatedPipelines.forEach(
            p -> pipelineListInfo.append(p.getId().getId().toString())
                .append("/").append(p.getReplicationConfig().toString())
                .append("/").append(p.getType().toString())
                .append("/").append(p.getPipelineState().toString()).append("/")
                .append(datanode.getUuid().equals(p.getLeaderId()) ?
                    "Leader" : "Follower")
                .append(System.getProperty("line.separator")));
      }
    } else {
      pipelineListInfo.append("No pipelines in cluster.");
    }
    System.out.println("Datanode: " + datanode.getUuid().toString() +
        " (" + datanode.getNetworkLocation() + "/" + datanode.getIpAddress()
        + "/" + datanode.getHostName() + "/" + relatedPipelineNum +
        " pipelines)");
    System.out.println("Operational State: " + dna.getOpState());
    System.out.println("Health State: " + dna.getHealthState());
    System.out.println("Related pipelines:\n" + pipelineListInfo);
  }

  private static class DatanodeWithAttributes {
    private DatanodeDetails datanodeDetails;
    private HddsProtos.NodeOperationalState operationalState;
    private HddsProtos.NodeState healthState;

    DatanodeWithAttributes(DatanodeDetails dn,
        HddsProtos.NodeOperationalState opState,
        HddsProtos.NodeState healthState) {
      this.datanodeDetails = dn;
      this.operationalState = opState;
      this.healthState = healthState;
    }

    public DatanodeDetails getDatanodeDetails() {
      return datanodeDetails;
    }

    public HddsProtos.NodeOperationalState getOpState() {
      return operationalState;
    }

    public HddsProtos.NodeState getHealthState() {
      return healthState;
    }
  }
}
