/**
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
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracking inflight containers per datanode to allow throttling
 * for a single datanode.
 */
public class ReplicationDatanodeThrottling {
  /**
   * This tracks replication destinations.
   */
  private final Map<DatanodeDetails, List<ContainerID>>
      inflightReplicationDests;

  private int maxReplicationCommandsForDatanode;

  public ReplicationDatanodeThrottling(int maxReplicationCommandsForDatanode) {
    this.inflightReplicationDests = new ConcurrentHashMap<>();
    this.maxReplicationCommandsForDatanode = maxReplicationCommandsForDatanode;
  }

  public boolean shouldThrottle(DatanodeDetails datanode, int delta) {
    if (inflightReplicationDests.getOrDefault(datanode,
        Collections.emptyList()).size() + delta >
        maxReplicationCommandsForDatanode) {
      return true;
    }
    // TODO: we could throttle on datanodes as sources either.
    return false;
  }

  public void recordDatanodeForContainer(
      DatanodeDetails datanode, ContainerID containerID) {
    inflightReplicationDests.computeIfAbsent(datanode,
        k -> Collections.synchronizedList(new ArrayList<>()));
    inflightReplicationDests.get(datanode).add(containerID);
  }

  public void forgetDatanodeForContainer(
      DatanodeDetails datanode, ContainerID containerID) {
    inflightReplicationDests.get(datanode).remove(containerID);
    if (inflightReplicationDests.get(datanode).isEmpty()) {
      inflightReplicationDests.remove(datanode);
    }
  }

  public void clear() {
    this.inflightReplicationDests.clear();
  }
}
