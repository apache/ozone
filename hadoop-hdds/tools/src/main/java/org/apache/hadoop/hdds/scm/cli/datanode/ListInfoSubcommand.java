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
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import picocli.CommandLine;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
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
public class ListInfoSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private DatanodeCommands parent;

  @CommandLine.Option(names = {"--ip"},
      description = "Show info by ip address.",
      defaultValue = "",
      required = false)
  private String ipaddress;

  @CommandLine.Option(names = {"--id"},
      description = "Show info by datanode UUID.",
      defaultValue = "",
      required = false)
  private String uuid;

  private List<Pipeline> pipelines;


  @Override
  public Void call() throws Exception {
    try (ScmClient scmClient = parent.getParent().createScmClient()) {
      pipelines = scmClient.listPipelines();
      if (Strings.isNullOrEmpty(ipaddress) && Strings.isNullOrEmpty(uuid)) {
        getAllNodes(scmClient).stream().forEach(p -> printDatanodeInfo(p));
      } else {
        Stream<DatanodeDetails> allNodes = getAllNodes(scmClient).stream();
        if (!Strings.isNullOrEmpty(ipaddress)) {
          allNodes = allNodes.filter(p -> p.getIpAddress()
              .compareToIgnoreCase(ipaddress) == 0);
        }
        if (!Strings.isNullOrEmpty(uuid)) {
          allNodes = allNodes.filter(p -> p.getUuid().toString().equals(uuid));
        }
        allNodes.forEach(p -> printDatanodeInfo(p));
      }
      return null;
    }
  }

  private List<DatanodeDetails> getAllNodes(ScmClient scmClient)
      throws IOException {
    List<HddsProtos.Node> nodes = scmClient.queryNode(
        HddsProtos.NodeState.HEALTHY, HddsProtos.QueryScope.CLUSTER, "");

    return nodes.stream()
        .map(p -> DatanodeDetails.getFromProtoBuf(p.getNodeID()))
        .collect(Collectors.toList());
  }

  private void printDatanodeInfo(DatanodeDetails datanode) {
    StringBuilder pipelineListInfo = new StringBuilder();
    int relatedPipelineNum = 0;
    if (!pipelines.isEmpty()) {
      List<Pipeline> relatedPipelines = pipelines.stream().filter(
          p -> p.getNodes().contains(datanode)).collect(Collectors.toList());
      if (relatedPipelines.isEmpty()) {
        pipelineListInfo.append("No related pipelines" +
            " or the node is not in Healthy state.");
      } else {
        relatedPipelineNum = relatedPipelines.size();
        relatedPipelines.stream().forEach(
            p -> pipelineListInfo.append(p.getId().getId().toString())
                .append("/").append(p.getFactor().toString()).append("/")
                .append(p.getType().toString()).append("/")
                .append(p.getPipelineState().toString()).append("/")
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
        " pipelines) \n" + "Related pipelines: \n" + pipelineListInfo);
  }
}