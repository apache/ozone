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
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;

/**
 * Handler to print decommissioning nodes status.
 */
@CommandLine.Command(
    name = "decommissionStatus",
    description = "Show decommission status for datanodes",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)

public class DecommissionStatusSubCommand extends ScmSubcommand {

  @CommandLine.Option(names = { "--decommissioned" },
      description = "Show info of decommissioned nodes too",
      defaultValue = "false")
  private boolean decommissioned;

  @CommandLine.Option(names = { "--id" },
      description = "Show info by datanode UUID",
      defaultValue = "")
  private String uuid;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<HddsProtos.Node> decommissioningNodes = new ArrayList<>();

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
          decommissioningNodes.size() + " nodes");
    }

    Map<UUID, Map<HddsProtos.LifeCycleState, Long>> containerMap =
        scmClient.getContainerMap();
    Map<UUID, Integer> pipelineMap = scmClient.getPipelineMap();
    List<Pipeline> currentPipelines = scmClient.listPipelines();

    for (HddsProtos.Node node : decommissioningNodes) {
      DatanodeDetails datanode = DatanodeDetails.getFromProtoBuf(
          node.getNodeID());
      printDetails(datanode, scmClient, containerMap, pipelineMap,
          currentPipelines);
    }

    if (decommissioned) {
      List<HddsProtos.Node> decommissionedNodes = scmClient.queryNode(
          DECOMMISSIONED, null, HddsProtos.QueryScope.CLUSTER, "");
      System.out.println("\n\nDecommission Status: DECOMMISSIONED - " +
          decommissionedNodes.size() + " nodes");
      for (HddsProtos.Node node : decommissionedNodes) {
        DatanodeDetails datanode = DatanodeDetails.getFromProtoBuf(
            node.getNodeID());
        System.out.println("\nDatanode: " + datanode.getUuid().toString() +
            " (" + datanode.getNetworkLocation() + "/" + datanode.getIpAddress()
            + "/" + datanode.getHostName() + ")");
        System.out.println("Decommissioning ended at : " +
            scmClient.getLastChangeTime(datanode));
      }
    }
  }
  private void printDetails(DatanodeDetails datanode, ScmClient scmClient,
                            Map<UUID, Map<HddsProtos.LifeCycleState, Long>>
                            containerMap, Map<UUID, Integer> pipelineMap,
                            List<Pipeline> currentPipelines)
      throws IOException {
    List<Pipeline> pipelines = new ArrayList<>();
    for (Pipeline listPipeline : currentPipelines) {
      if (listPipeline.getNodes().contains(datanode)) {
        pipelines.add(listPipeline);
      }
    }
    Set<ContainerID> containers = scmClient.getContainers(datanode);
    Map<HddsProtos.LifeCycleState, Long> containerMapForDn = new HashMap<>();
    for (HddsProtos.LifeCycleState lc : HddsProtos.LifeCycleState.values()) {
      containerMapForDn.put(lc, 0L);
    }
    for (ContainerID id : containers) {
      try {
        ContainerInfo containerInfo = scmClient.getContainer(
            id.getProtobuf().getId());
        containerMapForDn.replace(containerInfo.getState(),
            (containerMapForDn.get(containerInfo.getState()) + 1));
      } catch (ContainerNotFoundException e) {
        // don't do anything, Map values are not incremented
      }
    }

    System.out.println("\nDatanode: " + datanode.getUuid().toString() +
        " (" + datanode.getNetworkLocation() + "/" + datanode.getIpAddress()
        + "/" + datanode.getHostName() + ")");
    System.out.println("Decommissioning started at : " +
        scmClient.getLastChangeTime(datanode));
    System.out.println("No. of pipelines when decommissioning was started: "
        + pipelineMap.get(datanode.getUuid())
        + "\nNo. of pipelines currently: "
        + pipelines.size());
    System.out.println("No. of containers when decommissioning was started: "
        + containerMap.get(datanode.getUuid())
        + "\nNo. of containers currently: "
        + containerMapForDn);
  }
}
