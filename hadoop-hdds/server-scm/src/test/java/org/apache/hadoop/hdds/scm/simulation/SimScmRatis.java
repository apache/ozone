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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.RemoveSCMRequest;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBuffer;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMRatisRequest;
import org.apache.hadoop.hdds.scm.ha.SCMRatisResponse;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SCMSnapshotProvider;
import org.apache.hadoop.hdds.scm.ha.SCMStateMachine;
import org.apache.hadoop.hdds.scm.ha.invoker.ScmInvoker;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.FileUtils;

/**
 * Model of the Ratis group of the SCMs, the replicated log of SCM HA.
 * <p>
 * A replicated call on the leader is committed once a majority of the SCMs is up and connected. The leader applies the
 * entry right away and returns its result; the followers apply it in log order on their state machine lane, so they lag
 * behind. An SCM that was down or cut off gets the entries it missed when it can reach a leader again, and after a
 * restart it first applies again the entries after the last flush of its DB, as Ratis replays its log. When the leader
 * is gone, the others elect a new one after an election timeout, among those with the whole log. The new leader goes
 * through the steps of SCMStateMachine: notifyLeaderChanged, apply every entry, notifyLeaderReady. A leader cut off
 * from the others steps down after an election timeout.
 * <p>
 * Every snapshot threshold entries an SCM takes a snapshot, a flush of its DB, and may purge its log up to it. An SCM
 * that misses entries the leader has purged installs a snapshot instead: it downloads a checkpoint of the leader's DB
 * and reloads its state from it, as SCMStateMachine does when Ratis notifies it.
 * <pre>{@code
 *   scm1 (leader)                                 log              scm2, scm3 (followers)
 *   ContainerManager#updateContainerState
 *     -> @Replicate proxy -> submit -------> entry i ------------> received(i)
 *     <- result <---------- apply(i) on scm1                       state machine lane: apply(i)
 * }</pre>
 * A bootstrapped SCM joins the group at once, without the catch-up phase of a Ratis configuration change. Not modeled:
 * the uncommitted entries of a cut off leader, and the removal of SCMs.
 */
final class SimScmRatis {

  private static final RaftGroupId GROUP_ID = RaftGroupId.valueOf(new UUID(0, 1));
  private static final ClientId CLIENT_ID = ClientId.valueOf(new UUID(0, 2));

  private final ScmSimulation sim;
  private final List<SimScm> members = new ArrayList<>();
  /** Committed entries, encoded as SCMStateMachine receives them; entry i is at position i - 1. */
  private final List<Message> log = new ArrayList<>();
  /** The term in which each entry was appended. */
  private final List<Long> terms = new ArrayList<>();
  private SimScm leader;
  private long term;
  private SimScheduler.Timer election;

  SimScmRatis(ScmSimulation sim) {
    this.sim = sim;
  }

  void add(SimScm scm) {
    members.add(scm);
  }

  List<SimScm> getMembers() {
    return Collections.unmodifiableList(members);
  }

  SimScm getLeader() {
    return leader;
  }

  long getTerm() {
    return term;
  }

  long size() {
    return log.size();
  }

  Message entry(long index) {
    return log.get((int) index - 1);
  }

  long termOf(long index) {
    return terms.get((int) index - 1);
  }

  /** A replicated call of an SCM, like SCMRatisServerImpl#submitRequest. */
  SCMRatisResponse submit(SimScm from, SCMRatisRequest request) throws IOException, TimeoutException {
    if (from != leader || !from.isConnected()) {
      return reply(from, null, notLeader(from));
    }
    if (!hasMajority()) {
      // The leader cannot commit it; the caller gives up after the request timeout.
      throw new TimeoutException(from.getName() + " has no majority to commit " + request.getOperation());
    }
    // Entries of earlier terms come first: the state machine applies entries in order.
    from.applyUpTo(log.size());
    log.add(request.encode());
    terms.add(term);
    long index = log.size();
    from.received(index);
    for (SimScm member : members) {
      if (member != from && member.isConnected()) {
        catchUp(member);
      }
    }
    try {
      return reply(from, from.apply(index), null);
    } catch (Exception e) {
      return reply(from, null, new StateMachineException(memberId(from), e));
    }
  }

  /** An SCM started or got its connection back: it gets the entries it missed, or there may be a leader now. */
  void memberUp(SimScm scm) {
    if (leader != null && leader.isConnected()) {
      catchUp(scm);
      scm.followLeader();
    } else {
      scheduleElection();
    }
  }

  /** An SCM stopped or lost its connection. A leader cut off from the others steps down after an election timeout. */
  void memberDown(SimScm scm) {
    if (scm != leader) {
      return;
    }
    leader = null;
    if (scm.isRunning()) {
      long cutOffTerm = term;
      sim.scheduler().schedule(scm.getName() + ".stepDown", electionTimeoutMs(), () -> {
        if (leader != scm && scm.isLeaderOfTerm(cutOffTerm)) {
          scm.stepDown();
        }
      });
    }
    scheduleElection();
  }

  /** The leader gives up leadership, as on a transfer of leadership. */
  void stepDown() {
    SimScm old = leader;
    if (old == null) {
      return;
    }
    sim.record("scm.stepDown", old.getName() + " term " + term);
    leader = null;
    old.stepDown();
    scheduleElection();
  }

  /** Elects the first leader when the SCMs start, so that datanodes can register right away. */
  void electNow() {
    sim.scheduler().cancel(election);
    election = null;
    elect();
  }

  private void scheduleElection() {
    if (election == null) {
      election = sim.scheduler().schedule("scm.election", electionTimeoutMs(), () -> {
        election = null;
        elect();
      });
    }
  }

  private void elect() {
    if (leader != null || !hasMajority()) {
      // Without a majority, the election is retried when an SCM comes back.
      return;
    }
    List<SimScm> candidates = new ArrayList<>();
    for (SimScm member : members) {
      if (member.isConnected() && member.getReceivedIndex() == log.size()) {
        candidates.add(member);
      }
    }
    if (candidates.isEmpty()) {
      return;
    }
    SimScm winner = candidates.get(sim.random(SimRandom.SCM_HA).nextInt(candidates.size()));
    term++;
    leader = winner;
    sim.record("scm.leader", winner.getName() + " term " + term);
    for (SimScm member : members) {
      if (member != winner && member.isConnected()) {
        catchUp(member);
        if (member.isLeaderOfTerm(-1)) {
          member.stepDown();
        }
      }
    }
    winner.becomeLeader(term);
    for (SimScm member : members) {
      if (member != winner && member.isConnected()) {
        member.followLeader();
      }
    }
    sim.leaderChanged(winner);
  }

  /**
   * The leader sends an SCM the entries it misses. If the leader has purged some of them, it notifies the SCM to
   * install a snapshot; while the SCM installs it, the leader sends it no entries.
   */
  private void catchUp(SimScm member) {
    if (member.isInstalling()) {
      return;
    }
    if (member.getReceivedIndex() + 1 < leader.getLogStart()) {
      member.installSnapshot(leader);
      return;
    }
    member.received(log.size());
    member.scheduleApply(log.size());
  }

  /** The leader notifies an SCM that could not install a snapshot again, with its next heartbeat. */
  void retryCatchUp(SimScm member) {
    sim.scheduler().schedule(member.getName() + ".retryCatchUp", electionTimeoutMs(), () -> {
      if (leader != null && leader.isConnected() && member.isConnected()) {
        catchUp(member);
      }
    });
  }

  /** The lowest index any SCM has: without log purge, Ratis keeps the entries some SCM still needs. */
  long minReceivedIndex() {
    long min = log.size();
    for (SimScm member : members) {
      min = Math.min(min, member.getReceivedIndex());
    }
    return min;
  }

  private boolean hasMajority() {
    int connected = 0;
    for (SimScm member : members) {
      if (member.isConnected()) {
        connected++;
      }
    }
    return connected > members.size() / 2;
  }

  private long electionTimeoutMs() {
    return members.size() == 1 ? 0 : sim.config().getScmElectionTimeoutMs(sim.random(SimRandom.SCM_HA));
  }

  private static RaftGroupMemberId memberId(SimScm scm) {
    return RaftGroupMemberId.valueOf(RaftPeerId.valueOf(scm.getName()), GROUP_ID);
  }

  private static NotLeaderException notLeader(SimScm scm) {
    return new NotLeaderException(memberId(scm), null, new ArrayList<>());
  }

  private static SCMRatisResponse reply(SimScm scm, Message result, RaftException exception) throws IOException {
    RaftClientReply.Builder reply = RaftClientReply.newBuilder()
        .setClientId(CLIENT_ID)
        .setServerId(memberId(scm))
        .setGroupId(GROUP_ID)
        .setCallId(1L)
        .setLogIndex(1L);
    if (exception == null) {
      reply.setSuccess(true).setMessage(result);
    } else {
      reply.setSuccess(false).setMessage(Message.EMPTY).setException(exception);
    }
    return SCMRatisResponse.decode(reply.build());
  }

  /** The Ratis server of one SCM: replicated calls go to the group, state machine handlers to the SCM. */
  static final class Server implements SCMRatisServer {
    private final SimScm scm;
    private final SimScmRatis group;

    Server(SimScm scm, SimScmRatis group) {
      this.scm = scm;
      this.group = group;
    }

    @Override
    public void start() {
    }

    @Override
    public void registerStateMachineHandler(ScmInvoker<?> handler) {
      scm.registerInvoker(handler);
    }

    @Override
    public SCMRatisResponse submitRequest(SCMRatisRequest request) throws IOException, TimeoutException {
      return group.submit(scm, request);
    }

    @Override
    public boolean triggerSnapshot() {
      return false;
    }

    @Override
    public void stop() {
    }

    @Override
    public boolean isStopped() {
      return !scm.isRunning();
    }

    @Override
    public RaftServer.Division getDivision() {
      return null;
    }

    @Override
    public List<String> getRatisRoles() {
      List<String> roles = new ArrayList<>();
      for (SimScm member : group.getMembers()) {
        roles.add(member.getName() + (member == group.getLeader() ? ":LEADER" : ":FOLLOWER"));
      }
      return roles;
    }

    @Override
    public NotLeaderException triggerNotLeaderException() {
      return notLeader(scm);
    }

    @Override
    public boolean addSCM(AddSCMRequest request) {
      return false;
    }

    @Override
    public boolean removeSCM(RemoveSCMRequest request) {
      return false;
    }

    @Override
    public SCMStateMachine getSCMStateMachine() {
      return null;
    }

    @Override
    public GrpcTlsConfig getGrpcTlsConfig() {
      return null;
    }

    @Override
    public RaftPeerId getLeaderId() {
      return group.getLeader() == null ? null : RaftPeerId.valueOf(group.getLeader().getName());
    }
  }

  /** The HA manager of one SCM, with its Ratis server and DB transaction buffer. */
  static final class HAManager implements SCMHAManager {
    private final Server server;
    private final SCMHADBTransactionBuffer buffer;

    HAManager(Server server, SCMHADBTransactionBuffer buffer) {
      this.server = server;
      this.buffer = buffer;
    }

    @Override
    public void start() {
    }

    @Override
    public SCMRatisServer getRatisServer() {
      return server;
    }

    @Override
    public SCMSnapshotProvider getSCMSnapshotProvider() {
      return null;
    }

    @Override
    public DBTransactionBuffer getDBTransactionBuffer() {
      return buffer;
    }

    @Override
    public SCMHADBTransactionBuffer asSCMHADBTransactionBuffer() {
      return buffer;
    }

    @Override
    public void stop() {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean addSCM(AddSCMRequest request) {
      return false;
    }

    @Override
    public boolean removeSCM(RemoveSCMRequest request) {
      return false;
    }

    @Override
    public DBCheckpoint downloadCheckpointFromLeader(String leaderId) {
      throw new UnsupportedOperationException("Snapshots are not simulated");
    }

    @Override
    public List<ManagedSecretKey> getSecretKeysFromLeader(String leaderID) {
      throw new UnsupportedOperationException("Security is not simulated");
    }

    @Override
    public TermIndex verifyCheckpointFromLeader(String leaderId, DBCheckpoint checkpoint) {
      throw new UnsupportedOperationException("Snapshots are not simulated");
    }

    /**
     * Like SCMHAManagerImpl#installCheckpoint: replaces the DB with the checkpoint, and reloads the managers' state
     * from it as SCMHAManagerImpl#startServices does. If that fails, it puts the old DB back and throws; SCM then
     * exits.
     */
    @Override
    public TermIndex installCheckpoint(DBCheckpoint dbCheckpoint) throws Exception {
      StorageContainerManager scm = server.scm.getScm();
      TransactionInfo checkpointInfo = HAUtils.getTrxnInfoFromCheckpoint(scm.getConfiguration(),
          dbCheckpoint.getCheckpointLocation(), SCMDBDefinition.get());
      SCMMetadataStore metadataStore = scm.getScmMetadataStore();
      File dbLocation = metadataStore.getStore().getDbLocation();
      metadataStore.stop();
      File dbBackup = HAUtils.replaceDBWithCheckpoint(checkpointInfo.getTransactionIndex(), dbLocation,
          dbCheckpoint.getCheckpointLocation(), OzoneConsts.SCM_DB_BACKUP_PREFIX);
      try {
        metadataStore.start(scm.getConfiguration());
        scm.getSequenceIdGen().reinitialize(metadataStore.getSequenceIdTable());
        scm.getPipelineManager().reinitialize(metadataStore.getPipelineTable());
        scm.getContainerManager().reinitialize(metadataStore.getContainerTable());
        scm.getScmBlockManager().getDeletedBlockLog().reinitialize(metadataStore.getDeletedBlocksTXTable(),
            metadataStore.getStatefulServiceConfigTable());
        scm.getStatefulServiceStateManager().reinitialize(metadataStore.getStatefulServiceConfigTable());
        scm.getFinalizationManager().reinitialize(metadataStore.getMetaTable());
      } catch (Exception e) {
        metadataStore.stop();
        HAUtils.replaceDBWithCheckpoint(checkpointInfo.getTransactionIndex(), dbLocation, dbBackup.toPath(),
            OzoneConsts.SCM_DB_BACKUP_PREFIX);
        throw e;
      }
      FileUtils.deleteFully(dbBackup);
      return checkpointInfo.getTermIndex();
    }
  }
}
