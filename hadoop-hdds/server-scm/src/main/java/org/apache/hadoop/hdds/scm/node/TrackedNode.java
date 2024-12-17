/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to track DataNodes that are
 * being decommissioned or are in maintenance mode.
 */
public class TrackedNode {
  private DatanodeDetails datanodeDetails;
  private long startTime;
  private Map<String, List<ContainerID>> containersReplicatedOnNode = new ConcurrentHashMap<>();

  public TrackedNode(DatanodeDetails datanodeDetails, long startTime) {
    this.datanodeDetails = datanodeDetails;
    this.startTime = startTime;
  }

  @Override
  public int hashCode() {
    return datanodeDetails.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof TrackedNode &&
        datanodeDetails.equals(((TrackedNode) obj).getDatanodeDetails());
  }

  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  public long getStartTime() {
    return startTime;
  }

  public Map<String, List<ContainerID>> getContainersReplicatedOnNode() {
    return containersReplicatedOnNode;
  }

  public void setContainersReplicatedOnNode(TrackedNodeContainers containers) {
    List<ContainerID> underReplicated = containers.getUnderReplicatedIDs();
    List<ContainerID> unClosed = containers.getUnClosedIDs();
    this.containersReplicatedOnNode.put("UnderReplicated",
        Collections.unmodifiableList(underReplicated));
    this.containersReplicatedOnNode.put("UnClosed",
        Collections.unmodifiableList(unClosed));
  }
}
