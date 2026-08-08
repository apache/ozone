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

/**
 * Details about a single failed container move in a balancer iteration.
 */
public final class ContainerMoveFailureDetail {
  private final long containerId;
  private final String sourceDatanodeUuid;
  private final String targetDatanodeUuid;
  private final String reason;

  public ContainerMoveFailureDetail(long containerId, String sourceDatanodeUuid,
      String targetDatanodeUuid, String reason) {
    this.containerId = containerId;
    this.sourceDatanodeUuid = sourceDatanodeUuid;
    this.targetDatanodeUuid = targetDatanodeUuid;
    this.reason = reason;
  }

  public long getContainerId() {
    return containerId;
  }

  public String getSourceDatanodeUuid() {
    return sourceDatanodeUuid;
  }

  public String getTargetDatanodeUuid() {
    return targetDatanodeUuid;
  }

  public String getReason() {
    return reason;
  }
}
