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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

import java.util.HashSet;
import java.util.UUID;

public class ContainerReplicaWithTimestamp {

  // ContainerReplica is final so we are not directly extending it
  private final ContainerReplica containerReplica;
//  Long firstSeenTime;
  private Long lastSeenTime;

  ContainerReplicaWithTimestamp(ContainerReplica containerReplica) {
    this.containerReplica = containerReplica;
    lastSeenTime = System.currentTimeMillis();
  }

  ContainerReplicaWithTimestamp(ContainerReplica containerReplica,
      Long lastSeenTime) {
    this.containerReplica = containerReplica;
    this.lastSeenTime = lastSeenTime;
  }

  public ContainerReplica getContainerReplica() {
    return containerReplica;
  }

  public Long getLastSeenTime() {
    return lastSeenTime;
  }

  public void setLastSeenTime(Long lastSeenTime) {
    this.lastSeenTime = lastSeenTime;
  }

  @Override
  public int hashCode() {
    return containerReplica.getDatanodeDetails().getUuid().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (obj instanceof ContainerReplica) {
      final ContainerReplica replica = (ContainerReplica) obj;
      return containerReplica.getDatanodeDetails().getUuid().equals(
          replica.getDatanodeDetails().getUuid());
    } else if (!(obj instanceof ContainerReplicaWithTimestamp)) {
      return false;
    } else {
      final ContainerReplicaWithTimestamp replicaTS =
          (ContainerReplicaWithTimestamp) obj;
      return containerReplica.getDatanodeDetails().getUuid().equals(
          replicaTS.containerReplica.getDatanodeDetails().getUuid());
    }
  }

  public static void main(String[] args) {
    DatanodeDetails dn1 = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .build();
    ContainerReplica replica1 = ContainerReplica.newBuilder()
        .setContainerID(new ContainerID(2L))
        .setContainerState(StorageContainerDatanodeProtocolProtos.
            ContainerReplicaProto.State.CLOSED)
        .setDatanodeDetails(dn1)
        .build();
    ContainerReplicaWithTimestamp ts1 =
        new ContainerReplicaWithTimestamp(replica1);
    ContainerReplicaWithTimestamp ts2 =
        new ContainerReplicaWithTimestamp(replica1);

//    System.out.println(ts1.equals(ts2));
    HashSet<ContainerReplicaWithTimestamp> hashSet = new HashSet<>();
    hashSet.add(ts1);
    System.out.println(hashSet.contains(ts2));
  }
}
