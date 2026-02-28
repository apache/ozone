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

package org.apache.hadoop.hdds.scm.pipeline;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;

/**
 * Utility for filtering pipelines by StorageType. Builds a set of
 * qualifying node UUIDs (nodes that have at least one volume of the
 * requested StorageType), then filters pipelines to those whose every
 * member is in that set.
 */
final class PipelineStorageTypeFilter {

  private PipelineStorageTypeFilter() {
  }

  static Set<UUID> getNodesWithStorageType(NodeManager nodeManager,
      StorageType storageType) {
    Set<UUID> result = new HashSet<>();
    for (DatanodeDetails dn :
        nodeManager.getNodes(NodeStatus.inServiceHealthy())) {
      DatanodeInfo info = nodeManager.getDatanodeInfo(dn);
      if (info == null) {
        continue;
      }
      for (StorageReportProto report : info.getStorageReports()) {
        if (StorageType.valueOf(report.getStorageType()) == storageType) {
          result.add(dn.getUuid());
          break;
        }
      }
    }
    return result;
  }

  static List<Pipeline> filter(List<Pipeline> pipelines,
      NodeManager nodeManager, StorageType storageType) {
    if (storageType == null) {
      return pipelines;
    }
    Set<UUID> qualifiedNodes =
        getNodesWithStorageType(nodeManager, storageType);
    return pipelines.stream()
        .filter(p -> p.getNodes().stream()
            .allMatch(dn -> qualifiedNodes.contains(dn.getUuid())))
        .collect(Collectors.toList());
  }
}
