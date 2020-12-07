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

package org.apache.hadoop.ozone.recon.scm;

import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import java.util.UUID;

/**
 * An extension to ContainerReplica that has first and last report time.
 */
public class ContainerReplicaWithTimestamp {

  // Datanode UUID
  private UUID uuid;
  // First reported time of the replica on this datanode,
  //  should only be updated once when the object is created.
  private Long firstSeenTime;
  // Last reported time of the replica
  private Long lastSeenTime;
  // TODO: Remove this if unused.
  private final ContainerReplica containerReplica;

  /**
   * Constructor used when new timestamp entry is being created but has no
   *  ContainerReplica info.
   */
  public ContainerReplicaWithTimestamp(UUID id, Long lastSeenTime) {
    this.uuid = id;
    this.firstSeenTime = lastSeenTime;
    this.lastSeenTime = lastSeenTime;
    this.containerReplica = null;
  }

  /**
   * Constructor used when new timestamp entry is being created.
   */
  public ContainerReplicaWithTimestamp(UUID id, Long lastSeenTime,
      ContainerReplica containerReplica) {
    this.uuid = id;
    this.firstSeenTime = lastSeenTime;
    this.lastSeenTime = lastSeenTime;
    this.containerReplica = containerReplica;
  }

  /**
   * Constructor used when an entry is being read from the DB.
   */
  public ContainerReplicaWithTimestamp(UUID id, Long firstSeenTime,
      Long lastSeenTime, ContainerReplica containerReplica) {
    this.uuid = id;
    this.firstSeenTime = firstSeenTime;
    this.lastSeenTime = lastSeenTime;
    this.containerReplica = containerReplica;
  }

  /**
   * Return value can be null.
   */
  public ContainerReplica getContainerReplica() {
    return containerReplica;
  }

  public UUID getUuid() {
    return uuid;
  }

  public void setUuid(UUID uuid) {
    this.uuid = uuid;
  }

  public Long getFirstSeenTime() {
    return firstSeenTime;
  }

  public void setFirstSeenTime(Long firstSeenTime) {
    this.firstSeenTime = firstSeenTime;
  }

  public Long getLastSeenTime() {
    return lastSeenTime;
  }

  public void setLastSeenTime(Long lastSeenTime) {
    this.lastSeenTime = lastSeenTime;
  }
}
