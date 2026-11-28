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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
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
  private State state;
  /** Being rebuilt by an EC reconstruction: not reported to SCM, and not readable by other datanodes. */
  private boolean recovering;
  private long bcsid;
  /** Blocks the replica holds: local ID to bytes. */
  private final TreeMap<Long, Long> blocks = new TreeMap<>();
  /**
   * Highest local ID written to the replica. Local IDs grow with the commit order, so a write that catches up skips the
   * blocks up to it, like Ratis replay skips transactions up to the container BCSID; deleted blocks stay deleted.
   */
  private long lastLocalId;
  private long used;
  private long deleteTransactionId;
  /** A block deletion transaction removed blocks. */
  private boolean blocksDeleted;
  /** The datanode found the replica without blocks, see KeyValueContainerData#markAsEmpty; never reset. */
  private boolean markedEmpty;

  SimReplica(long containerId, String originNodeId, int replicaIndex, PipelineID pipelineId, State state) {
    this.containerId = containerId;
    this.originNodeId = originNodeId;
    this.replicaIndex = replicaIndex;
    this.pipelineId = pipelineId;
    this.state = state;
  }

  /** Copy placed on a replication target: same state, origin, index and data, including the deletions applied. */
  SimReplica importedCopy() {
    SimReplica copy = new SimReplica(containerId, originNodeId, replicaIndex, null, state);
    copy.copyBlocksFrom(this);
    copy.deleteTransactionId = deleteTransactionId;
    copy.blocksDeleted = blocksDeleted;
    return copy;
  }

  /**
   * Replica of a missing EC index, being rebuilt on a reconstruction target from the data of the other indexes.
   * It stays RECOVERING until the coordinator closes it.
   */
  static SimReplica recovering(long containerId, String originNodeId, int replicaIndex, SimReplica template) {
    SimReplica replica = new SimReplica(containerId, originNodeId, replicaIndex, null, State.CLOSED);
    replica.recovering = true;
    replica.copyBlocksFrom(template);
    return replica;
  }

  long getContainerId() {
    return containerId;
  }

  String getOriginNodeId() {
    return originNodeId;
  }

  int getReplicaIndex() {
    return replicaIndex;
  }

  boolean isEc() {
    return replicaIndex > 0;
  }

  boolean isRecovering() {
    return recovering;
  }

  /** Ends recovery: the replica is CLOSED if reconstruction finished, or UNHEALTHY if it timed out. */
  void endRecovering(State newState) {
    recovering = false;
    state = newState;
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
    return blocks.size();
  }

  long getUsed() {
    return used;
  }

  boolean hasData() {
    return !blocks.isEmpty();
  }

  /** Blocks the replica holds, by local ID. */
  Set<Long> getBlockIds() {
    return blocks.keySet();
  }

  /** Applies committed writes: the commit sequence, and every block written to the container so far. */
  void applyWrites(long newBcsid, NavigableMap<Long, Long> written) {
    bcsid = Math.max(bcsid, newBcsid);
    for (Map.Entry<Long, Long> block : written.tailMap(lastLocalId, false).entrySet()) {
      blocks.put(block.getKey(), block.getValue());
      used += block.getValue();
      lastLocalId = block.getKey();
    }
  }

  /**
   * Applies a block deletion transaction like DeleteBlocksCommandHandler: a retry of the last transaction is skipped,
   * any other one removes those of its blocks the replica has, even if it comes out of order.
   * @return the blocks it removed
   */
  List<Long> applyDeleteTransaction(long txId, List<Long> localIds) {
    List<Long> removed = new ArrayList<>();
    if (txId == deleteTransactionId) {
      return removed;
    }
    deleteTransactionId = Math.max(deleteTransactionId, txId);
    for (Long id : localIds) {
      Long size = blocks.remove(id);
      if (size != null) {
        used -= size;
        removed.add(id);
        blocksDeleted = true;
      }
    }
    return removed;
  }

  /** Called when the datanode loads the replica, at start or import: a non-open replica without blocks is empty. */
  void markEmptyIfNoBlocks() {
    if (blocks.isEmpty() && state != State.OPEN) {
      markedEmpty = true;
    }
  }

  /**
   * Whether the datanode reports the replica empty. BlockDeletingService removes deleted blocks of CLOSED and
   * QUASI_CLOSED replicas only, and marks a replica empty when it removed the last one.
   */
  boolean isEmpty() {
    if (blocks.isEmpty() && blocksDeleted && (state == State.CLOSED || state == State.QUASI_CLOSED)) {
      markedEmpty = true;
    }
    return markedEmpty;
  }

  private void copyBlocksFrom(SimReplica source) {
    bcsid = source.bcsid;
    blocks.putAll(source.blocks);
    lastLocalId = source.lastLocalId;
    used = source.used;
  }

  ContainerReplicaProto toProto() {
    return ContainerReplicaProto.newBuilder()
        .setContainerID(containerId)
        .setState(state)
        .setUsed(used)
        .setKeyCount(blocks.size())
        .setBlockCommitSequenceId(bcsid)
        .setDeleteTransactionId(deleteTransactionId)
        .setOriginNodeId(originNodeId)
        .setReplicaIndex(replicaIndex)
        .setIsEmpty(isEmpty())
        .build();
  }

  @Override
  public String toString() {
    return "#" + containerId + (replicaIndex > 0 ? "[" + replicaIndex + "]" : "") + ":"
        + (recovering ? "RECOVERING" : state) + "@" + bcsid + "/" + blocks.size() + "k";
  }
}
