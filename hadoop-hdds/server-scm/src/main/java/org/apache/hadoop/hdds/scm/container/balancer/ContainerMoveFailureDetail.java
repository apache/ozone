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

import java.util.Collections;
import java.util.Map;

/**
 * Per-reason container move failure summary with per-datanode failure counts.
 */
public final class ContainerMoveFailureDetail {
  private final String reason;
  private final long count;
  private final Map<String, Long> sourceFailureCounts;
  private final Map<String, Long> targetFailureCounts;
  private final Map<String, String> datanodeHostnames;

  public ContainerMoveFailureDetail(String reason, long count,
      Map<String, Long> sourceFailureCounts, Map<String, Long> targetFailureCounts,
      Map<String, String> datanodeHostnames) {
    this.reason = reason;
    this.count = count;
    this.sourceFailureCounts = Collections.unmodifiableMap(sourceFailureCounts);
    this.targetFailureCounts = Collections.unmodifiableMap(targetFailureCounts);
    this.datanodeHostnames = Collections.unmodifiableMap(datanodeHostnames);
  }

  public String getReason() {
    return reason;
  }

  public long getCount() {
    return count;
  }

  public Map<String, Long> getSourceFailureCounts() {
    return sourceFailureCounts;
  }

  public Map<String, Long> getTargetFailureCounts() {
    return targetFailureCounts;
  }

  public Map<String, String> getDatanodeHostnames() {
    return datanodeHostnames;
  }
}
