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

import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaCount;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is used to track the status of Containers for DataNodes
 * that are being decommissioning or entering maintenance mode.
 */
public class TrackedNodeContainers {

  // sufficientlyReplicated refers to a replica that has been sufficiently replicated,
  // meaning the required number of copies
  // have been made to ensure data redundancy and reliability.
  private int sufficientlyReplicated = 0;

  // deleting indicates that the replica is in the deletion state,
  // which is allowed during DataNode decommissioning or when entering maintenance mode.
  private int deleting = 0;

  // underReplicated refers to replicas that need to be replicated.
  // Only after all replicas have been successfully replicated can
  // the DataNode be decommissioned or enter maintenance mode.
  private int underReplicated = 0;

  // This list is used to store containers that still need to be replicated.
  private List<ContainerID> underReplicatedIDs = new ArrayList<>();

  // unclosed indicates that the container is not yet closed.
  // The DataNode must wait for the container to close before
  // it can be decommissioned or enter maintenance mode.
  private int unclosed = 0;

  // This list is used to store containers that have not been closed yet.
  private List<ContainerID> unClosedIDs = new ArrayList<>();

  private int containerDetailsLoggingLimit;

  public TrackedNodeContainers(int containerDetailsLoggingLimit) {
    this.containerDetailsLoggingLimit = containerDetailsLoggingLimit;
  }

  public void incrSufficientlyReplicated() {
    sufficientlyReplicated++;
  }

  public void incrDeleting() {
    deleting++;
  }

  public void addUnderReplicated(ContainerID cid) {
    this.underReplicatedIDs.add(cid);
    this.underReplicated++;
  }

  public void addUnClosedIDs(ContainerID cid) {
    this.unClosedIDs.add(cid);
    this.unclosed++;
  }

  public int getSufficientlyReplicated() {
    return sufficientlyReplicated;
  }

  public void setSufficientlyReplicated(int sufficientlyReplicated) {
    this.sufficientlyReplicated = sufficientlyReplicated;
  }

  public int getDeleting() {
    return deleting;
  }

  public void setDeleting(int deleting) {
    this.deleting = deleting;
  }

  public int getUnderReplicated() {
    return underReplicated;
  }

  public void setUnderReplicated(int underReplicated) {
    this.underReplicated = underReplicated;
  }

  public int getUnclosed() {
    return unclosed;
  }

  public void setUnclosed(int unclosed) {
    this.unclosed = unclosed;
  }

  public boolean isLogUnderReplicatedContainers() {
    return this.underReplicated < this.containerDetailsLoggingLimit;
  }

  public boolean isLogUnClosedContainers() {
    return this.unclosed < this.containerDetailsLoggingLimit;
  }

  public String getFormatUnderReplicatedIDs() {
    return this.underReplicatedIDs.stream().map(
        Object::toString).collect(Collectors.joining(", "));
  }

  public String getFormatUnClosedIDs() {
    return this.unClosedIDs.stream().map(
        Object::toString).collect(Collectors.joining(", "));
  }

  public List<ContainerID> getUnderReplicatedIDs() {
    return underReplicatedIDs;
  }

  public List<ContainerID> getUnClosedIDs() {
    return unClosedIDs;
  }

  public String replicaDetails(ContainerReplicaCount replicaSet) {
    Collection<ContainerReplica> replicas = replicaSet.getReplicas();
    StringBuilder sb = new StringBuilder();
    sb.append("Replicas{");
    sb.append(replicas.stream()
        .map(Object::toString)
        .collect(Collectors.joining(",")));
    sb.append("}");
    return sb.toString();
  }
}
