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

import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManager;

/**
 * A {@link LeaderChoosePolicy} support choosing leader from datanode list.
 */
public abstract class LeaderChoosePolicy {

  private final NodeManager nodeManager;
  private final PipelineStateManager pipelineStateManager;

  public LeaderChoosePolicy(
      NodeManager nodeManager, PipelineStateManager pipelineStateManager) {
    this.nodeManager = nodeManager;
    this.pipelineStateManager = pipelineStateManager;
  }

  /**
   * Given an initial list of datanodes, return one of the datanodes.
   *
   * @param dns list of datanodes.
   * @return one of the datanodes.
   */
  public abstract DatanodeDetails chooseLeader(List<DatanodeDetails> dns);

  protected NodeManager getNodeManager() {
    return nodeManager;
  }

  protected PipelineStateManager getPipelineStateManager() {
    return pipelineStateManager;
  }
}
