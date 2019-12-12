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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

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

  @CommandLine.Option(names = {"-ip", "--byIp"},
      description = "Show info by ip address.",
      defaultValue = "",
      required = false)
  private String ipaddress;

  @CommandLine.Option(names = {"-id", "--byUuid"},
      description = "Show info by datanode UUID.",
      defaultValue = "",
      required = false)
  private String uuid;

  private List<Pipeline> pipelines;


  @Override
  public Void call() throws Exception {
    try (ScmClient scmClient = parent.getParent().createScmClient()) {
      pipelines = scmClient.listPipelines();
      if (isNullOrEmpty(ipaddress) && isNullOrEmpty(uuid)) {
        getAllNodes(scmClient).stream().forEach(p -> printDatanodeInfo(p));
      } else {
        getAllNodes(scmClient).stream().filter(
            p -> ((isNullOrEmpty(ipaddress) ||
            (p.getIpAddress().compareToIgnoreCase(ipaddress) == 0))
            && (isNullOrEmpty(uuid) ||
            (p.getUuid().equals(uuid)))))
            .forEach(p -> printDatanodeInfo(p));
      }
      return null;
    }
  }

  private List<DatanodeDetails> getAllNodes(ScmClient scmClient)
      throws IOException {
    List<HddsProtos.Node> nodes = scmClient.queryNode(
        HddsProtos.NodeState.HEALTHY, HddsProtos.QueryScope.CLUSTER, "");
    List<DatanodeDetails> datanodes = new ArrayList<>(nodes.size());
    nodes.stream().forEach(
        p -> datanodes.add(DatanodeDetails.getFromProtoBuf(p.getNodeID())));
    return datanodes;
  }

  private void printDatanodeInfo(DatanodeDetails datanode) {
    if (pipelines.isEmpty()) {
      System.out.println("Datanode: " + datanode.getUuid().toString() +
          " (" + datanode.getIpAddress() + "/"
          + datanode.getHostName() + "). \n No pipeline created.");
      return;
    }
    List<Pipeline> relatedPipelines = pipelines.stream().filter(
        p -> p.getNodes().contains(datanode)).collect(Collectors.toList());
    StringBuilder pipelineList = new StringBuilder()
        .append("Related pipelines:\n");
    relatedPipelines.stream().forEach(
        p -> pipelineList.append(p.getId().getId().toString())
            .append("/").append(p.getFactor().toString()).append("/")
            .append(p.getType().toString()).append("/")
            .append(p.getPipelineState().toString()).append("/")
            .append(datanode.getUuid().equals(p.getLeaderId()) ?
                "Leader" : "Follower")
            .append(System.getProperty("line.separator")));
    System.out.println("Datanode: " + datanode.getUuid().toString() +
        " (" + datanode.getIpAddress() + "/"
        + datanode.getHostName() + "/" + relatedPipelines.size() +
        " pipelines). \n" + pipelineList);
  }

  protected static boolean isNullOrEmpty(String str) {
    return ((str == null) || str.trim().isEmpty());
  }
}