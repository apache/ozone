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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

/**
 * Model of the Ratis group behind a pipeline.
 * <p>
 * Members join when they handle CreatePipelineCommand and leave when they handle ClosePipelineCommand. A leader is
 * elected among running members once a majority of the group is up. Client writes and container closes are committed
 * only with a leader and a majority, and are applied to every running member; a member that was down catches up from
 * the committed state when a leader exists again, as log replay would do. Only the entries it has not applied yet:
 * a replica deleted after it applied them does not come back.
 */
final class SimRatisGroup {

  private final ScmSimulation sim;
  private final PipelineID id;
  private final List<DatanodeID> members;
  private final List<Integer> priorities;
  private final Set<DatanodeID> joined = new LinkedHashSet<>();
  private final Map<Long, Committed> committed = new TreeMap<>();
  /** Index of the last log entry each member applied. */
  private final Map<DatanodeID, Long> applied = new HashMap<>();
  private DatanodeID leader;
  private SimScheduler.Timer election;
  private long logIndex;

  SimRatisGroup(ScmSimulation sim, PipelineID id, List<DatanodeID> members, List<Integer> priorities) {
    this.sim = sim;
    this.id = id;
    this.members = new ArrayList<>(members);
    this.priorities = new ArrayList<>(priorities);
  }

  PipelineID getId() {
    return id;
  }

  List<DatanodeID> getMembers() {
    return members;
  }

  DatanodeID getLeader() {
    return leader;
  }

  boolean isMember(DatanodeID dn) {
    return members.contains(dn);
  }

  void join(SimDatanode dn) {
    joined.add(dn.getId());
    maybeElect();
  }

  void leave(SimDatanode dn) {
    joined.remove(dn.getId());
    if (dn.getId().equals(leader)) {
      leader = null;
    }
    maybeElect();
  }

  void memberDown(SimDatanode dn) {
    if (dn.getId().equals(leader)) {
      leader = null;
    }
    maybeElect();
  }

  void memberUp(SimDatanode dn) {
    if (leader != null && hasQuorum()) {
      catchUp(dn);
    }
    maybeElect();
  }

  boolean hasQuorum() {
    return leader != null && runningJoined().size() > members.size() / 2;
  }

  /**
   * Commits a client write of new blocks to the given container.
   * @return the local IDs of the new blocks, or null if the group cannot commit or the container is not open.
   */
  List<Long> write(long containerId, long bytes, int blockCount) {
    if (!hasQuorum()) {
      return null;
    }
    SimReplica leaderReplica = sim.datanode(leader).getReplica(containerId);
    if (leaderReplica != null && leaderReplica.getState() != State.OPEN) {
      return null;
    }
    Committed c = committed.computeIfAbsent(containerId, k -> new Committed());
    c.bcsid = ++logIndex;
    c.lastIndex = logIndex;
    List<Long> written = new ArrayList<>();
    for (int i = 0; i < blockCount; i++) {
      c.blocks.put(++c.lastLocalId, bytes / blockCount);
      written.add(c.lastLocalId);
    }
    for (SimDatanode dn : runningJoined()) {
      dn.applyCommitted(id, containerId, c.bcsid, c.blocks, false);
      applied.put(dn.getId(), logIndex);
    }
    return written;
  }

  /** Commits a close of the container, when the leader handles the close command. */
  boolean closeContainer(SimDatanode requester, long containerId) {
    if (!requester.getId().equals(leader) || !hasQuorum()) {
      return false;
    }
    Committed c = committed.get(containerId);
    if (c == null || c.closed) {
      return c != null;
    }
    c.closed = true;
    c.lastIndex = ++logIndex;
    for (SimDatanode dn : runningJoined()) {
      dn.applyCommitted(id, containerId, c.bcsid, c.blocks, true);
      applied.put(dn.getId(), logIndex);
    }
    return true;
  }

  Set<Long> getContainers() {
    return committed.keySet();
  }

  private void catchUp(SimDatanode dn) {
    long appliedIndex = applied.getOrDefault(dn.getId(), 0L);
    for (Map.Entry<Long, Committed> e : committed.entrySet()) {
      Committed c = e.getValue();
      if (c.lastIndex > appliedIndex) {
        sim.record("ratis.catchUp", dn.getName() + " #" + e.getKey() + " " + sim.describe(id) + " from index "
            + appliedIndex + " to " + c.lastIndex);
        SimReplica before = dn.getReplica(e.getKey());
        long blocksBefore = before == null ? -1 : before.getKeys();
        dn.applyCommitted(id, e.getKey(), c.bcsid, c.blocks, c.closed);
        SimReplica after = dn.getReplica(e.getKey());
        if (after != null && after.getKeys() != blocksBefore) {
          sim.replicaCaughtUp(dn, after);
        }
      }
    }
    applied.put(dn.getId(), logIndex);
  }

  private List<SimDatanode> runningJoined() {
    List<SimDatanode> result = new ArrayList<>();
    for (DatanodeID member : members) {
      SimDatanode dn = sim.datanode(member);
      if (joined.contains(member) && dn != null && dn.isRunning() && dn.hasPipeline(id)) {
        result.add(dn);
      }
    }
    return result;
  }

  private void maybeElect() {
    if (leader != null || election != null) {
      return;
    }
    if (runningJoined().size() <= members.size() / 2) {
      return;
    }
    long delay = sim.config().getElectionDelayMs(sim.random(SimRandom.DATANODE));
    election = sim.scheduler().schedule("ratis.election " + sim.describe(id), delay, this::elect);
  }

  private void elect() {
    election = null;
    List<SimDatanode> candidates = runningJoined();
    if (leader != null || candidates.size() <= members.size() / 2) {
      return;
    }
    SimDatanode best = null;
    int bestPriority = Integer.MIN_VALUE;
    for (SimDatanode dn : candidates) {
      int index = members.indexOf(dn.getId());
      int priority = index < priorities.size() ? priorities.get(index) : 0;
      if (priority > bestPriority) {
        best = dn;
        bestPriority = priority;
      }
    }
    leader = best.getId();
    for (SimDatanode dn : candidates) {
      catchUp(dn);
    }
    best.onBecomeLeader(id);
  }

  /** State committed through the group for one container. */
  private static final class Committed {
    private long bcsid;
    /** Every block written to the container: local ID to bytes. */
    private final NavigableMap<Long, Long> blocks = new TreeMap<>();
    private long lastLocalId;
    private boolean closed;
    /** Index of the last log entry for the container: its last write, or its close. */
    private long lastIndex;
  }
}
