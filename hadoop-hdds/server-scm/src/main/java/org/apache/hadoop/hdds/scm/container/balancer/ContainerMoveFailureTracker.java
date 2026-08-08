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

package org.apache.hadoop.hdds.scm.container.balancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;

/**
 * Tracks per-iteration container move failures by reason and optional details.
 */
public final class ContainerMoveFailureTracker {
  public static final int DEFAULT_MAX_FAILURE_DETAILS = 100;
  public static final int DEFAULT_MAX_FAILURE_DETAILS_PER_REASON = 10;

  private final int maxFailureDetails;
  private final int maxFailureDetailsPerReason;
  private final Map<String, Long> failuresByReason = new HashMap<>();
  private final Map<String, Integer> failureDetailCountByReason = new HashMap<>();
  private final List<ContainerMoveFailureDetail> failureDetails = new ArrayList<>();

  public ContainerMoveFailureTracker() {
    this(DEFAULT_MAX_FAILURE_DETAILS, DEFAULT_MAX_FAILURE_DETAILS_PER_REASON);
  }

  public ContainerMoveFailureTracker(int maxFailureDetails, int maxFailureDetailsPerReason) {
    this.maxFailureDetails = maxFailureDetails;
    this.maxFailureDetailsPerReason = Math.min(maxFailureDetailsPerReason, maxFailureDetails);
  }

  public synchronized void recordFailure(MoveManager.MoveResult result, ContainerID containerId,
      DatanodeDetails source, DatanodeDetails target) {
    recordFailure(result.name(), containerId, source, target);
  }

  public synchronized void recordFailure(String reason, ContainerID containerId,
      DatanodeDetails source, DatanodeDetails target) {
    failuresByReason.merge(reason, 1L, Long::sum);
    if (shouldRecordFailureDetail(reason)) {
      failureDetails.add(new ContainerMoveFailureDetail(
          containerId.getId(),
          source.getUuidString(),
          target.getUuidString(),
          reason));
      failureDetailCountByReason.merge(reason, 1, Integer::sum);
    }
  }

  private boolean shouldRecordFailureDetail(String reason) {
    if (failureDetails.size() >= maxFailureDetails) {
      return false;
    }
    int reasonDetailCount = failureDetailCountByReason.getOrDefault(reason, 0);
    if (reasonDetailCount == 0) {
      return true;
    }
    return reasonDetailCount < maxFailureDetailsPerReason;
  }

  public synchronized void reset() {
    failuresByReason.clear();
    failureDetailCountByReason.clear();
    failureDetails.clear();
  }

  public synchronized Map<String, Long> getFailuresByReason() {
    return Collections.unmodifiableMap(new HashMap<>(failuresByReason));
  }

  public synchronized List<ContainerMoveFailureDetail> getFailureDetails() {
    return Collections.unmodifiableList(new ArrayList<>(failureDetails));
  }
}
