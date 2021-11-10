/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * The selection criteria for selecting source datanodes , the containers of
 * which will be moved out.
 */
public class SourceDataNodeSelectionCriteria {
  private static final Logger LOG =
      LoggerFactory.getLogger(SourceDataNodeSelectionCriteria.class);
  private Map<DatanodeDetails, Long> sizeLeavingNode;
  private List<DatanodeUsageInfo> overUtilizedNodes;

  public SourceDataNodeSelectionCriteria(
      List<DatanodeUsageInfo> overUtilizedNodes,
      Map<DatanodeDetails, Long> sizeLeavingNode) {
    this.sizeLeavingNode = sizeLeavingNode;
    this.overUtilizedNodes = overUtilizedNodes;
  }

  public DatanodeDetails getNextCandidateSourceDataNode() {
    if (overUtilizedNodes.isEmpty()) {
      LOG.info("no more candidate data node");
      return null;
    }
    //TODO：use a more quick data structure, which will hava a
    // better performance when changing or deleting one element at once
    overUtilizedNodes.sort((a, b) -> {
      double currentUsageOfA = a.calculateUtilization(
          sizeLeavingNode.get(a.getDatanodeDetails()));
      double currentUsageOfB = b.calculateUtilization(
          sizeLeavingNode.get(b.getDatanodeDetails()));
      //in descending order
      return Double.compare(currentUsageOfB, currentUsageOfA);
    });

    return overUtilizedNodes.get(0).getDatanodeDetails();
  }

  public void removeCandidateSourceDataNode(DatanodeDetails dui){
    overUtilizedNodes.removeIf(a -> a.getDatanodeDetails().equals(dui));
  }
}
