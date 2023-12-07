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

package org.apache.hadoop.hdds.scm.container.balancer.iteration;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.TreeSet;

/**
 * an implementation of FindTargetGreedy, which will always select the
 * target with the lowest space usage.
 */
class FindTargetGreedyByUsageInfo extends AbstractFindTargetGreedy {
  private final TreeSet<DatanodeUsageInfo> potentialTargets;

  FindTargetGreedyByUsageInfo(@Nonnull StorageContainerManager scm) {
    super(scm, FindTargetGreedyByUsageInfo.class);
    potentialTargets = new TreeSet<>(this::compareByUsage);
  }

  @Override
  protected Collection<DatanodeUsageInfo> getPotentialTargets() {
    return potentialTargets;
  }

  @Override
  public void sortTargetForSource(@Nonnull DatanodeDetails source) {
    // noop, Treeset is naturally sorted.
  }
}
