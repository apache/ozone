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

import java.util.UUID;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerReplicaHistoryProto;

/**
 * A ContainerReplica timestamp class that tracks first and last seen time.
 *
 * Note this only tracks first and last seen time of a container replica.
 * Recon does not guarantee the replica is available during the whole period
 * from first seen time to last seen time.
 * For example, Recon won't track records where a replica could be move out
 * of one DN but later moved back to the same DN.
 */
public class ContainerReplicaHistory {
  // Datanode UUID
  private final UUID uuid;
  // First reported time of the replica on this datanode
  private final Long firstSeenTime;
  // Last reported time of the replica
  private Long lastSeenTime;

  private long bcsId;
  private String state;

  public ContainerReplicaHistory(UUID id, Long firstSeenTime,
      Long lastSeenTime, long bcsId, String state) {
    this.uuid = id;
    this.firstSeenTime = firstSeenTime;
    this.lastSeenTime = lastSeenTime;
    this.bcsId = bcsId;
    this.state = state;
  }

  public long getBcsId() {
    return bcsId;
  }

  public void setBcsId(long bcsId) {
    this.bcsId = bcsId;
  }

  public UUID getUuid() {
    return uuid;
  }

  public Long getFirstSeenTime() {
    return firstSeenTime;
  }

  public Long getLastSeenTime() {
    return lastSeenTime;
  }

  public void setLastSeenTime(Long lastSeenTime) {
    this.lastSeenTime = lastSeenTime;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public static ContainerReplicaHistory fromProto(
      ContainerReplicaHistoryProto proto) {
    return new ContainerReplicaHistory(UUID.fromString(proto.getUuid()),
        proto.getFirstSeenTime(), proto.getLastSeenTime(), proto.getBcsId(),
        proto.getState());
  }

  public ContainerReplicaHistoryProto toProto() {
    return ContainerReplicaHistoryProto.newBuilder().setUuid(uuid.toString())
        .setFirstSeenTime(firstSeenTime).setLastSeenTime(lastSeenTime)
        .setBcsId(bcsId).setState(state).build();
  }
}
