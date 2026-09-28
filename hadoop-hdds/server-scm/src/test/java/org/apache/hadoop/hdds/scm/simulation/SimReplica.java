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

package org.apache.hadoop.hdds.scm.simulation;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

/** A container replica held by a simulated datanode. */
final class SimReplica {

  private final long containerId;
  private final String originNodeId;
  private final int replicaIndex;
  /** Pipeline the replica was written through; null for imported replicas. */
  private final PipelineID pipelineId;
  private final boolean imported;
  private State state;
  private long bcsid;
  private long used;
  private long keys;

  SimReplica(long containerId, String originNodeId, int replicaIndex, PipelineID pipelineId, boolean imported,
      State state) {
    this.containerId = containerId;
    this.originNodeId = originNodeId;
    this.replicaIndex = replicaIndex;
    this.pipelineId = pipelineId;
    this.imported = imported;
    this.state = state;
  }

  /** Copy placed on a replication target: same state, origin, index and data. */
  SimReplica importedCopy() {
    SimReplica copy = new SimReplica(containerId, originNodeId, replicaIndex, null, true, state);
    copy.bcsid = bcsid;
    copy.used = used;
    copy.keys = keys;
    return copy;
  }

  long getContainerId() {
    return containerId;
  }

  String getOriginNodeId() {
    return originNodeId;
  }

  PipelineID getPipelineId() {
    return pipelineId;
  }

  State getState() {
    return state;
  }

  void setState(State state) {
    this.state = state;
  }

  long getBcsid() {
    return bcsid;
  }

  long getKeys() {
    return keys;
  }

  long getUsed() {
    return used;
  }

  boolean hasData() {
    return keys > 0;
  }

  void setData(long newBcsid, long newUsed, long newKeys) {
    this.bcsid = Math.max(bcsid, newBcsid);
    this.used = Math.max(used, newUsed);
    this.keys = Math.max(keys, newKeys);
  }

  ContainerReplicaProto toProto() {
    return ContainerReplicaProto.newBuilder()
        .setContainerID(containerId)
        .setState(state)
        .setUsed(used)
        .setKeyCount(keys)
        .setBlockCommitSequenceId(bcsid)
        .setOriginNodeId(originNodeId)
        .setReplicaIndex(replicaIndex)
        // Only a replica imported without blocks reports empty; see KeyValueContainerData.
        .setIsEmpty(imported && keys == 0)
        .build();
  }

  @Override
  public String toString() {
    return "#" + containerId + ":" + state + "@" + bcsid + "/" + keys + "k";
  }
}
