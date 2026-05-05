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

package org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The minimum leader count choose policy that chooses leader
 * which has the minimum exist leader count.
 */
public class MinLeaderCountChoosePolicy extends LeaderChoosePolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(MinLeaderCountChoosePolicy.class);

  public MinLeaderCountChoosePolicy(
      NodeManager nodeManager, PipelineStateManager pipelineStateManager) {
    super(nodeManager, pipelineStateManager);
  }

  @Override
  public DatanodeDetails chooseLeader(List<DatanodeDetails> dns) {
    Map<DatanodeDetails, Integer> suggestedLeaderCount =
        getSuggestedLeaderCount(
            dns, getNodeManager(), getPipelineStateManager());
    int minLeaderCount = Integer.MAX_VALUE;
    DatanodeDetails suggestedLeader = null;

    for (Map.Entry<DatanodeDetails, Integer> entry :
        suggestedLeaderCount.entrySet()) {
      if (entry.getValue() < minLeaderCount) {
        minLeaderCount = entry.getValue();
        suggestedLeader = entry.getKey();
      }
    }

    return suggestedLeader;
  }

  private Map<DatanodeDetails, Integer> getSuggestedLeaderCount(
      List<DatanodeDetails> dns, NodeManager nodeManager,
      PipelineStateManager pipelineStateManager) {
    Map<DatanodeDetails, Integer> suggestedLeaderCount = new HashMap<>();
    for (DatanodeDetails dn : dns) {
      suggestedLeaderCount.put(dn, 0);

      Set<PipelineID> pipelineIDSet = nodeManager.getPipelines(dn);
      for (PipelineID pipelineID : pipelineIDSet) {
        try {
          Pipeline pipeline = pipelineStateManager.getPipeline(pipelineID);
          if (!pipeline.isClosed()
              && dn.getID().equals(pipeline.getSuggestedLeaderId())) {
            suggestedLeaderCount.put(dn, suggestedLeaderCount.get(dn) + 1);
          }
        } catch (PipelineNotFoundException e) {
          LOG.debug("Pipeline not found in pipeline state manager : {}",
              pipelineID, e);
        }
      }
    }

    return suggestedLeaderCount;
  }
}
