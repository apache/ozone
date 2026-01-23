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

package org.apache.hadoop.hdds.client;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decommission specific stateless utility functions.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public final class DecommissionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DecommissionUtils.class);

  private DecommissionUtils() {
  }

  /**
   * Returns the list of uuid or ipAddress matching decommissioning status nodes.
   *
   * @param allNodes All datanodes which are in decommissioning status.
   * @param uuid node uuid.
   * @param ipAddress node ipAddress
   * @return the list of uuid or ipAddress matching decommissioning status nodes.
   */
  public static List<HddsProtos.Node> getDecommissioningNodesList(Stream<HddsProtos.Node> allNodes,
                                                                  String uuid,
                                                                  String ipAddress) {
    List<HddsProtos.Node> decommissioningNodes;
    if (!Strings.isNullOrEmpty(uuid)) {
      decommissioningNodes = allNodes.filter(p -> p.getNodeID().getUuid()
          .equals(uuid)).collect(Collectors.toList());
    } else if (!Strings.isNullOrEmpty(ipAddress)) {
      decommissioningNodes = allNodes.filter(p -> p.getNodeID().getIpAddress()
          .compareToIgnoreCase(ipAddress) == 0).collect(Collectors.toList());
    } else {
      decommissioningNodes = allNodes.collect(Collectors.toList());
    }
    return decommissioningNodes;
  }

  /**
   * Returns Json node of datanode metrics.
   *
   * @param metricsJson
   * @return Json node of datanode metrics
   * @throws IOException
   */
  public static JsonNode getBeansJsonNode(String metricsJson) throws IOException {
    JsonNode jsonNode;
    ObjectMapper objectMapper = new ObjectMapper();
    JsonFactory factory = objectMapper.getFactory();
    JsonParser parser = factory.createParser(metricsJson);
    jsonNode = (JsonNode) objectMapper.readTree(parser).get("beans").get(0);
    return jsonNode;
  }

  /**
   * Returns the number of decommissioning nodes.
   *
   * @param jsonNode
   */
  public static int getNumDecomNodes(JsonNode jsonNode) {
    int numDecomNodes;
    JsonNode totalDecom = jsonNode.get("DecommissioningMaintenanceNodesTotal");
    numDecomNodes = (totalDecom == null ? -1 : Integer.parseInt(totalDecom.toString()));
    return numDecomNodes;
  }

  /**
   * Returns the counts of following info attributes.
   *  - decommissionStartTime
   *  - numOfUnclosedPipelines
   *  - numOfUnderReplicatedContainers
   *  - numOfUnclosedContainers
   *
   * @param datanode
   * @param counts
   * @param numDecomNodes
   * @param countsMap
   * @param errMsg
   * @throws IOException
   */
  @Nullable
  public static Map<String, Object> getCountsMap(DatanodeDetails datanode, JsonNode counts, int numDecomNodes,
                                                 Map<String, Object> countsMap, String errMsg)
      throws IOException {
    for (int i = 1; i <= numDecomNodes; i++) {
      String datanodeHostName =
          (counts.get("tag.datanode." + i) != null) ? (counts.get("tag.datanode." + i).asText()) : "";
      if (datanode.getHostName().equals(datanodeHostName)) {
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
    return null;
  }
}
