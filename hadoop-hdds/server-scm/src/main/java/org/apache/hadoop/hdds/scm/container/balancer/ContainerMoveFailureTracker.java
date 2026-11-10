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

/**
 * Tracks per-iteration container move failures by reason and per-datanode counts.
 */
final class ContainerMoveFailureTracker {
  private final Map<String, Long> failuresByReason = new HashMap<>();
  private final Map<String, Map<String, Long>> sourceFailureCountsByReason = new HashMap<>();
  private final Map<String, Map<String, Long>> targetFailureCountsByReason = new HashMap<>();
  private final Map<String, String> datanodeHostnames = new HashMap<>();

  synchronized void recordFailure(MoveManager.MoveResult result, DatanodeDetails source,
                                         DatanodeDetails target) {
    recordFailure(result.name(), source, target);
  }

  synchronized void recordFailure(ContainerBalancerTask.ContainerMoveFailureReason result,
                                         DatanodeDetails source, DatanodeDetails target) {
    recordFailure(result.name(), source, target);
  }

  synchronized void recordFailure(String reason, DatanodeDetails source, DatanodeDetails target) {
    failuresByReason.merge(reason, 1L, Long::sum);
    if (source != null) {
      datanodeHostnames.putIfAbsent(source.getUuidString(), source.getHostName());
      sourceFailureCountsByReason.computeIfAbsent(reason, k -> new HashMap<>())
          .merge(source.getUuidString(), 1L, Long::sum);
    }
    if (target != null) {
      datanodeHostnames.putIfAbsent(target.getUuidString(), target.getHostName());
      targetFailureCountsByReason.computeIfAbsent(reason, k -> new HashMap<>())
          .merge(target.getUuidString(), 1L, Long::sum);
    }
  }

  synchronized void reset() {
    failuresByReason.clear();
    sourceFailureCountsByReason.clear();
    targetFailureCountsByReason.clear();
    datanodeHostnames.clear();
  }

  synchronized List<ContainerMoveFailureDetail> getFailures() {
    List<ContainerMoveFailureDetail> result = new ArrayList<>();
    for (Map.Entry<String, Long> entry : failuresByReason.entrySet()) {
      String reason = entry.getKey();
      long count = entry.getValue();
      Map<String, Long> srcCounts = sourceFailureCountsByReason.getOrDefault(reason, Collections.emptyMap());
      Map<String, Long> tgtCounts = targetFailureCountsByReason.getOrDefault(reason, Collections.emptyMap());
      result.add(new ContainerMoveFailureDetail(reason, count, new HashMap<>(srcCounts), new HashMap<>(tgtCounts),
          copyHostnamesForDetail(srcCounts, tgtCounts)));
    }
    return result;
  }

  private Map<String, String> copyHostnamesForDetail(Map<String, Long> srcCounts, Map<String, Long> tgtCounts) {
    Map<String, String> hostnames = new HashMap<>();
    srcCounts.keySet().forEach(uuid -> copyHostnameIfPresent(uuid, hostnames));
    tgtCounts.keySet().forEach(uuid -> copyHostnameIfPresent(uuid, hostnames));
    return hostnames;
  }

  private void copyHostnameIfPresent(String uuid, Map<String, String> hostnames) {
    String hostname = datanodeHostnames.get(uuid);
    if (hostname != null && !hostname.isEmpty()) {
      hostnames.put(uuid, hostname);
    }
  }
}
