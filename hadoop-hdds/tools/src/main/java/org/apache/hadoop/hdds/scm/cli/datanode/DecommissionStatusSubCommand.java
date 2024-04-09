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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
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

  @CommandLine.Option(names = { "--json" },
      description = "Show output in json format",
      defaultValue = "false")
  private boolean json;

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
      if (!json) {
        System.out.println("\nDecommission Status: DECOMMISSIONING - " +
                decommissioningNodes.size() + " node(s)");
      }
    }

    String metricsJson = scmClient.getMetrics("Hadoop:service=StorageContainerManager,name=NodeDecommissionMetrics");
    int numDecomNodes = -1;
    JsonNode jsonNode = null;
    if (metricsJson != null) {
      ObjectMapper objectMapper = new ObjectMapper();
      JsonFactory factory = objectMapper.getFactory();
      JsonParser parser = factory.createParser(metricsJson);
      jsonNode = (JsonNode) objectMapper.readTree(parser).get("beans").get(0);
      JsonNode totalDecom = jsonNode.get("DecommissioningMaintenanceNodesTotal");
      numDecomNodes = (totalDecom == null ? -1 : Integer.parseInt(totalDecom.toString()));
    }

    if (json) {
      List<Map<String, Object>> decommissioningNodesDetails = new ArrayList<>();

      for (HddsProtos.Node node : decommissioningNodes) {
        DatanodeDetails datanode = DatanodeDetails.getFromProtoBuf(
            node.getNodeID());
        Map<String, Object> datanodeMap = new LinkedHashMap<>();
        datanodeMap.put("datanodeDetails", datanode);
        datanodeMap.put("metrics", getCounts(datanode, jsonNode, numDecomNodes));
        datanodeMap.put("containers", getContainers(scmClient, datanode));
        decommissioningNodesDetails.add(datanodeMap);
      }
      System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(decommissioningNodesDetails));
      return;
    }

    for (HddsProtos.Node node : decommissioningNodes) {
      DatanodeDetails datanode = DatanodeDetails.getFromProtoBuf(
          node.getNodeID());
      printDetails(datanode);
      printCounts(datanode, jsonNode, numDecomNodes);
      Map<String, List<ContainerID>> containers = scmClient.getContainersOnDecomNode(datanode);
      System.out.println(containers);
    }
  }

  private String errorMessage = "Error getting pipeline and container metrics for ";

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  private void printDetails(DatanodeDetails datanode) {
    System.out.println("\nDatanode: " + datanode.getUuid().toString() +
        " (" + datanode.getNetworkLocation() + "/" + datanode.getIpAddress()
        + "/" + datanode.getHostName() + ")");
  }

  private void printCounts(DatanodeDetails datanode, JsonNode counts, int numDecomNodes) {
    Map<String, Object> countsMap = getCounts(datanode, counts, numDecomNodes);
    System.out.println("Decommission Started At : " + countsMap.get("decommissionStartTime"));
    System.out.println("No. of Unclosed Pipelines: " + countsMap.get("numOfUnclosedPipelines"));
    System.out.println("No. of UnderReplicated Containers: " + countsMap.get("numOfUnderReplicatedContainers"));
    System.out.println("No. of Unclosed Containers: " + countsMap.get("numOfUnclosedContainers"));
  }

  private Map<String, Object> getCounts(DatanodeDetails datanode, JsonNode counts, int numDecomNodes) {
    Map<String, Object> countsMap = new LinkedHashMap<>();
    String errMsg = getErrorMessage() + datanode.getHostName();
    try {
      for (int i = 1; i <= numDecomNodes; i++) {
        if (datanode.getHostName().equals(counts.get("tag.datanode." + i).asText())) {
          JsonNode pipelinesDN = counts.get("PipelinesWaitingToCloseDN." + i);
          JsonNode underReplicatedDN = counts.get("UnderReplicatedDN." + i);
          JsonNode unclosedDN = counts.get("UnclosedContainersDN." + i);
          JsonNode startTimeDN = counts.get("StartTimeDN." + i);
          if (pipelinesDN == null || underReplicatedDN == null || unclosedDN == null || startTimeDN == null) {
            throw new IOException(errMsg);
          }

          int pipelines = Integer.parseInt(pipelinesDN.toString());
          double underReplicated = Double.parseDouble(underReplicatedDN.toString());
          double unclosed = Double.parseDouble(unclosedDN.toString());
          long startTime = Long.parseLong(startTimeDN.toString());
          Date date = new Date(startTime);
          DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss z");
          countsMap.put("decommissionStartTime", formatter.format(date));
          countsMap.put("numOfUnclosedPipelines", pipelines);
          countsMap.put("numOfUnderReplicatedContainers", underReplicated);
          countsMap.put("numOfUnclosedContainers", unclosed);
          return countsMap;
        }
      }
      System.err.println(errMsg);
    } catch (IOException e) {
      System.err.println(errMsg);
    }
    return countsMap;
  }

  private Map<String, Object> getContainers(ScmClient scmClient, DatanodeDetails datanode) throws IOException {
    Map<String, List<ContainerID>> containers = scmClient.getContainersOnDecomNode(datanode);
    return containers.entrySet().stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        entry -> entry.getValue().stream().
                 map(ContainerID::toString).
                 collect(Collectors.toList())));
  }
}
