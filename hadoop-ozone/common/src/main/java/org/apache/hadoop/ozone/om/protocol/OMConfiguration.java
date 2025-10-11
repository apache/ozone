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

package org.apache.hadoop.ozone.om.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.NodeDetails;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;

/**
 * Class storing the OM configuration information such as the node details in
 * memory and node details when config is reloaded from disk.
 * Note that this class is used as a structure to transfer the OM configuration
 * information through the {@link OMAdminProtocol} and not for storing the
 * configuration information in OzoneManager itself.
 */
public final class OMConfiguration {

  // OM nodes present in OM's memory (does not include Decommissioned nodes)
  private List<OMNodeDetails> omNodesInMemory = new ArrayList<>();
  // OM nodes reloaded from new config on disk (includes Decommissioned nodes)
  private List<OMNodeDetails> omNodesInNewConf = new ArrayList<>();

  private OMConfiguration(List<OMNodeDetails> inMemoryNodeList,
      List<OMNodeDetails> onDiskNodeList) {
    this.omNodesInMemory.addAll(inMemoryNodeList);
    this.omNodesInNewConf.addAll(onDiskNodeList);
  }

  /**
   * OMConfiguration Builder class.
   */
  public static class Builder {
    private List<OMNodeDetails> omNodesInMemory;
    private List<OMNodeDetails> omNodesInNewConf;

    public Builder() {
      this.omNodesInMemory = new ArrayList<>();
      this.omNodesInNewConf = new ArrayList<>();
    }

    public Builder addToNodesInMemory(OMNodeDetails nodeDetails) {
      this.omNodesInMemory.add(nodeDetails);
      return this;
    }

    public Builder addToNodesInNewConf(OMNodeDetails nodeDetails) {
      this.omNodesInNewConf.add(nodeDetails);
      return this;
    }

    public OMConfiguration build() {
      return new OMConfiguration(omNodesInMemory, omNodesInNewConf);
    }
  }

  /**
   * Return list of all current OM peer's nodeIds (does not reload
   * configuration from disk to find newly configured OMs).
   */
  public List<String> getCurrentPeerList() {
    return omNodesInMemory.stream().map(NodeDetails::getNodeId)
        .collect(Collectors.toList());
  }

  /**
   * Reload configuration from disk and return all active OM nodes (excludes
   * decommissioned nodes) present in the new conf under current serviceId.
   */
  public Map<String, OMNodeDetails> getActiveOmNodesInNewConf() {
    return omNodesInNewConf.stream()
        .filter(omNodeDetails -> !omNodeDetails.isDecommissioned())
        .collect(Collectors.toMap(NodeDetails::getNodeId,
            omNodeDetails -> omNodeDetails,
            (nodeId, omNodeDetails) -> omNodeDetails));
  }

  /**
   * Return all decommissioned nodes in the reloaded config.
   */
  public Map<String, OMNodeDetails> getDecommissionedNodesInNewConf() {
    return omNodesInNewConf.stream()
        .filter(omNodeDetails -> omNodeDetails.isDecommissioned())
        .collect(Collectors.toMap(NodeDetails::getNodeId,
            omNodeDetails -> omNodeDetails,
            (nodeId, omNodeDetails) -> omNodeDetails));
  }
}
