/*
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

package org.apache.hadoop.hdds.scm.container.balancer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.LoggerFactory;

import java.util.TreeSet;

/**
 * an implementation of FindTargetGreedy, which will always select the
 * target with the lowest space usage.
 */
public class FindTargetGreedyByUsageInfo extends AbstractFindTargetGreedy {
  public FindTargetGreedyByUsageInfo(
      ContainerManager containerManager,
      PlacementPolicyValidateProxy placementPolicyValidateProxy,
      NodeManager nodeManager) {
    super(containerManager, placementPolicyValidateProxy, nodeManager);
    setLogger(LoggerFactory.getLogger(FindTargetGreedyByUsageInfo.class));
    setPotentialTargets(new TreeSet<>((a, b) -> compareByUsage(a, b)));
  }

  /**
   * do nothing , since TreeSet is ordered itself.
   */
  @VisibleForTesting
  public void sortTargetForSource(DatanodeDetails source) {
    //noop, Treeset is naturally sorted.
    return;
  }
}
