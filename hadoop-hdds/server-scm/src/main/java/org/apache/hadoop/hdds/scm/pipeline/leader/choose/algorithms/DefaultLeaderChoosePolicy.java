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
 * The default leader choose policy.
 * Do not choose leader here, so that all nodes have the same priority
 * and ratis elects leader without depending on priority.
 */
public class DefaultLeaderChoosePolicy extends LeaderChoosePolicy {

  public DefaultLeaderChoosePolicy(
      NodeManager nodeManager, PipelineStateManager pipelineStateManager) {
    super(nodeManager, pipelineStateManager);
  }

  @Override
  public DatanodeDetails chooseLeader(List<DatanodeDetails> dns) {
    return null;
  }
}
