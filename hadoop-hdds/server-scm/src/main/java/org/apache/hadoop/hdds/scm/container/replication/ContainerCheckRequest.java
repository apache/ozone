/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;

import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * Simple class to wrap the parameters needed to check a container's health
 * in ReplicationManager.
 */
public final class ContainerCheckRequest {

  private final ContainerInfo containerInfo;
  private final Set<ContainerReplica> containerReplicas;
  private final List<ContainerReplicaOp> pendingOps;
  private final int maintenanceRedundancy;
  private final ReplicationManagerReport report;
  private final Queue<ContainerHealthResult.UnderReplicatedHealthResult>
      underRepQueue;
  private final Queue<ContainerHealthResult.OverReplicatedHealthResult>
      overRepQueue;


  private ContainerCheckRequest(Builder builder) {
    this.containerInfo = builder.containerInfo;
    this.containerReplicas = builder.containerReplicas;
    this.pendingOps = builder.pendingOps;
    this.maintenanceRedundancy = builder.maintenanceRedundancy;
    this.report = builder.report;
    this.overRepQueue = builder.overRepQueue;
    this.underRepQueue = builder.underRepQueue;
  }

  public List<ContainerReplicaOp> getPendingOps() {
    return pendingOps;
  }

  public int getMaintenanceRedundancy() {
    return maintenanceRedundancy;
  }

  public Set<ContainerReplica> getContainerReplicas() {
    return containerReplicas;
  }

  public ContainerInfo getContainerInfo() {
    return containerInfo;
  }

  public ReplicationManagerReport getReport() {
    return report;
  }

  public Queue<ContainerHealthResult.UnderReplicatedHealthResult>
      getUnderRepQueue() {
    return underRepQueue;
  }

  public Queue<ContainerHealthResult.OverReplicatedHealthResult>
      getOverRepQueue() {
    return overRepQueue;
  }

  /**
   * Builder class for ContainerCheckRequest.
   */
  public static class Builder {

    private ContainerInfo containerInfo;
    private Set<ContainerReplica> containerReplicas;
    private List<ContainerReplicaOp> pendingOps;
    private int maintenanceRedundancy;
    private ReplicationManagerReport report;
    private Queue<ContainerHealthResult.UnderReplicatedHealthResult>
        underRepQueue;
    private Queue<ContainerHealthResult.OverReplicatedHealthResult>
        overRepQueue;

    public Builder setContainerInfo(ContainerInfo containerInfo) {
      this.containerInfo = containerInfo;
      return this;
    }

    public Builder setContainerReplicas(
        Set<ContainerReplica> containerReplicas) {
      this.containerReplicas = containerReplicas;
      return this;
    }

    public Builder setPendingOps(List<ContainerReplicaOp> pendingOps) {
      this.pendingOps = pendingOps;
      return this;
    }

    public Builder setMaintenanceRedundancy(int maintenanceRedundancy) {
      this.maintenanceRedundancy = maintenanceRedundancy;
      return this;
    }

    public Builder setUnderRepQueue(
        Queue<ContainerHealthResult.UnderReplicatedHealthResult> queue) {
      this.underRepQueue = queue;
      return this;
    }

    public Builder setOverRepQueue(
        Queue<ContainerHealthResult.OverReplicatedHealthResult> queue) {
      this.overRepQueue = queue;
      return this;
    }

    public Builder setReport(ReplicationManagerReport report) {
      this.report = report;
      return this;
    }

    public ContainerCheckRequest build() {
      return new ContainerCheckRequest(this);
    }
  }
}