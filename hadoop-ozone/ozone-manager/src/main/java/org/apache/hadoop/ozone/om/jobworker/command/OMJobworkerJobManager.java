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

package org.apache.hadoop.ozone.om.jobworker.command;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.conf.OMJobworkerConfiguration;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;
import org.apache.hadoop.ozone.jobworker.protocolPB.JobworkerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.jobworker.utils.JobworkerUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.jobworker.JobworkerProtocolServerImpl;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeNotFoundException;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.JobworkerJobRecord;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateJobworkerJobRequest;
import org.apache.hadoop.util.Time;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.RaftConfiguration;

/**
 * Authoritative Jobworker assignments and results. All mutations use OM Ratis; no socket success is a result commit.
 * Assignments survive leadership changes. Uncertain attempts are never automatically reassigned.
 */
public final class OMJobworkerJobManager {
  public static final int PROTOCOL_VERSION = 1;
  private static final int MAX_COMMANDS_PER_HEARTBEAT = 100;
  private final OzoneManager om;
  private final ClientId clientId = ClientId.randomId();
  private final AtomicLong callId = new AtomicLong();
  private String verifiedMembers;
  private final Map<UUID, String> deliveryCursors = new ConcurrentHashMap<>();
  private long observedTerm = -1;
  private long leaderSince;

  private String verifyMembers() throws IOException {
    RaftConfiguration configuration = om.getOmRatisServer().getServerDivision().getRaftConf();
    Set<String> members = new TreeSet<>();
    configuration.getAllPeers().forEach(peer -> members.add(peer.getId().toString()));
    configuration.getAllPeers(RaftPeerRole.LISTENER)
        .forEach(peer -> members.add(peer.getId().toString()));
    String membership = configuration.getLogEntryIndex() + ":" + String.join(",", members);
    if (membership.equals(verifiedMembers)) {
      return membership;
    }
    boolean[] previouslyVerified = {false};
    scan(record -> {
      previouslyVerified[0] = record.getFinalizedMembers().equals(membership);
      return !previouslyVerified[0];
    });
    if (previouslyVerified[0]) {
      verifiedMembers = membership;
      return membership;
    }
    // A local finalized layout is insufficient: an older/offline peer may not understand the new Ratis request.
    // Probe all members before the first new request; retain the proof with the committed job for failover.
    for (String member : members) {
      if (member.equals(om.getOMNodeId())) {
        continue;
      }
      String address = JobworkerUtils.getOmJobWorkerRpcAddress(
          om.getConfiguration(), om.getOMServiceId(), member);
      if (address == null) {
        throw new IOException("No Jobworker endpoint configured for OM member " + member);
      }
      InetSocketAddress endpoint = NetUtils.createSocketAddr(address);
      try (JobworkerProtocolClientSideTranslatorPB client =
          new JobworkerProtocolClientSideTranslatorPB(
              endpoint.getHostString(), endpoint.getPort(), om.getConfiguration())) {
        GetOMVersionResponse version =
            client.getOMVersion(GetOMVersionRequest.getDefaultInstance());
        Map<String, String> keys = new HashMap<>();
        version.getKeysList().forEach(value -> keys.put(value.getKey(), value.getValue()));
        if (version.getDurableJobProtocolVersion() != PROTOCOL_VERSION || !member.equals(version.getOmNodeId())
            || !om.getOMServiceId().equals(keys.get(OzoneConsts.OM_SERVICE_ID))
            || !om.getOmStorage().getClusterID().equals(keys.get(OzoneConsts.CLUSTER_ID))) {
          throw new IOException("OM member " + member + " has not finalized the same durable Jobworker protocol");
        }
      }
    }
    requireLeader();
    return membership;
  }

  public OMJobworkerJobManager(OzoneManager om) {
    this.om = om;
  }

  public boolean isAvailable() {
    return om.isLeaderReady() && om.getVersionManager().isAllowed(OMLayoutFeature.JOBWORKER_HA);
  }

  private void requireLeader() throws IOException {
    if (!isAvailable()) {
      throw new IOException("Jobworker jobs require a ready OM leader and finalized Jobworker HA layout");
    }
  }

  public synchronized long submit(UUID worker, OMJobworkerCommand<?> command) throws IOException {
    requireLeader();
    if (!command.hasExplicitId()) {
      throw new IOException("Durable jobs require an explicit stable workflow ID; "
          + "JVM-local IDs can collide on restart");
    }
    OMJobworkerCommandProto envelope = JobworkerProtocolServerImpl.getCommandResponse(command).toBuilder()
        .setCmdId(command.getId()).build();
    String key = JobworkerJobState.key(command.getType(), command.getId());
    JobworkerJobRecord existing = get(key);
    if (existing != null) {
      OMJobworkerCommandProto previous = OMJobworkerCommandProto.parseFrom(existing.getCommand());
      if (!existing.getWorkerId().equals(worker.toString())
          || !previous.toBuilder().clearAttemptId().build().equals(envelope)) {
        throw new IOException("Jobworker job ID is already assigned; do not reuse IDs or reassign uncertain work");
      }
      return command.getId();
    }
    envelope = envelope.toBuilder().setAttemptId(UUID.randomUUID().toString()).build();
    CommandStatus pending = CommandStatus.newBuilder().setCmdId(command.getId()).setType(command.getType())
        .setOmServiceId(om.getOMServiceId()).setAttemptId(envelope.getAttemptId())
        .setStatus(CommandStatus.Status.PENDING).build();
    commit(JobworkerJobRecord.newBuilder().setJobKey(key).setWorkerId(worker.toString())
        .setCommand(envelope.toByteString()).setStatus(pending.toByteString()).setRevision(0).build(), 0);
    return command.getId();
  }

  public JobworkerJobRecord get(String key) throws IOException {
    return om.getMetadataManager().getJobworkerJobTable().get(key);
  }

  /**
   * Records an outcome proved by the owning workflow after inspecting its authoritative effects.
   * This is an internal reconciliation operation, not permission to retry or reassign uncertain effects.
   */
  public synchronized JobworkerJobRecord resolveUncertain(String key, long expectedRevision, CommandStatus result)
      throws IOException {
    requireLeader();
    JobworkerJobRecord current = get(key);
    if (current == null || current.getRevision() != expectedRevision
        || CommandStatus.parseFrom(current.getStatus()).getStatus() != CommandStatus.Status.UNCERTAIN
        || !JobworkerJobState.terminal(result.getStatus())) {
      throw new IOException("Uncertain assignment changed or the reconciliation result is not terminal");
    }
    return commit(current.toBuilder().setStatus(result.toByteString()).build(), expectedRevision);
  }

  private JobworkerJobRecord commit(JobworkerJobRecord proposed, long expectedRevision) throws IOException {
    requireLeader();
    proposed = proposed.toBuilder().setFinalizedMembers(verifyMembers()).build();
    JobworkerJobState.validate(proposed);
    OMRequest request = OMRequest.newBuilder().setCmdType(Type.UpdateJobworkerJob)
        .setClientId(clientId.toString()).setUpdateJobworkerJobRequest(UpdateJobworkerJobRequest.newBuilder()
            .setRecord(proposed).setExpectedRevision(expectedRevision)).build();
    try {
      OMResponse response = om.getOmRatisServer().submitRequest(request, clientId, callId.incrementAndGet());
      if (!response.getSuccess() || !response.hasJobworkerJobRecord()) {
        throw new IOException("Could not commit Jobworker state: " + response.getStatus()
            + ": " + response.getMessage());
      }
      JobworkerJobRecord committed = response.getJobworkerJobRecord();
      verifiedMembers = committed.getFinalizedMembers();
      return committed;
    } catch (ServiceException e) {
      throw new IOException("Could not commit Jobworker state through Ratis", e);
    }
  }

  public synchronized List<CommandStatus> reconcile(JobworkerDetails details,
      List<CommandStatusReportsProto> reports)
      throws IOException {
    requireLeader();
    UUID worker = details.getUuid();
    List<CommandStatus> acknowledgements = new ArrayList<>();
    for (CommandStatusReportsProto report : reports) {
      for (CommandStatus status : report.getCmdStatusList()) {
        String key = JobworkerJobState.key(status.getType(), status.getCmdId());
        JobworkerJobRecord current = get(key);
        if (current == null || !current.getWorkerId().equals(worker.toString())) {
          continue;
        }
        OMJobworkerCommandProto command = OMJobworkerCommandProto.parseFrom(current.getCommand());
        if (!status.getOmServiceId().equals(om.getOMServiceId())
            || !status.hasAttemptId() || !status.getAttemptId().equals(command.getAttemptId())) {
          continue;
        }
        CommandStatus previous = CommandStatus.parseFrom(current.getStatus());
        if (JobworkerJobState.terminal(previous.getStatus())) {
          if (JobworkerJobState.terminal(status.getStatus()) && !previous.equals(status)) {
            continue;
          }
          // An owning workflow may have resolved the uncertainty while this worker was offline.
          status = previous;
        } else if (!JobworkerJobState.canUpdate(previous, status)) {
          continue;
        } else if (!previous.equals(status)) {
          current = commit(current.toBuilder().setStatus(status.toByteString()).build(), current.getRevision());
          if (status.getStatus() == CommandStatus.Status.EXECUTING) {
            om.getOMJobworkerCommandManager().notifyCommitted(command, status, details);
          }
        }
        if (JobworkerJobState.terminal(status.getStatus())) {
          // Only a committed terminal record may acknowledge the worker's retained result.
          requireLeader();
          om.getOMJobworkerCommandManager().notifyCommitted(command, status, details);
          requireLeader();
          acknowledgements.add(CommandStatus.parseFrom(current.getStatus()));
        }
      }
    }
    return acknowledgements;
  }

  /** Reconstruct delivery from committed records, including assignments created by an earlier leader. */
  public List<OMJobworkerCommandProto> commandsFor(UUID worker) throws IOException {
    requireLeader();
    List<OMJobworkerCommandProto> commands = new ArrayList<>();
    String cursor = deliveryCursors.getOrDefault(worker, "");
    scan(record -> {
      CommandStatus status = CommandStatus.parseFrom(record.getStatus());
      if (record.getWorkerId().equals(worker.toString())
          && record.getJobKey().compareTo(cursor) > 0 && !JobworkerJobState.terminal(status.getStatus())) {
        OMJobworkerCommandProto.Builder command = OMJobworkerCommandProto.parseFrom(record.getCommand()).toBuilder();
        if (status.getStatus() != CommandStatus.Status.PENDING) {
          command.setReconcileOnly(true);
        }
        commands.add(command.build());
        deliveryCursors.put(worker, record.getJobKey());
      }
      return commands.size() < MAX_COMMANDS_PER_HEARTBEAT;
    });
    if (commands.size() < MAX_COMMANDS_PER_HEARTBEAT) {
      deliveryCursors.remove(worker);
    }
    requireLeader();
    return commands;
  }

  /** Reconcile unavailable workers on the leader without changing assignment ownership. */
  public synchronized void reconcileUnavailableWorkers() throws IOException {
    if (!isAvailable()) {
      observedTerm = -1;
      return;
    }
    long term = om.getOmRatisServer().getServerDivision().getInfo().getCurrentTerm();
    long now = Time.monotonicNow();
    if (observedTerm != term) {
      observedTerm = term;
      leaderSince = now;
    }
    List<JobworkerJobRecord> unavailable = new ArrayList<>();
    scan(record -> {
      CommandStatus status = CommandStatus.parseFrom(record.getStatus());
      if (JobworkerJobState.terminal(status.getStatus()) || status.getStatus() == CommandStatus.Status.UNCERTAIN) {
        return true;
      }
      UUID worker = UUID.fromString(record.getWorkerId());
      JobworkerNodeManager nodes = om.getJobworkerNodemanager();
      if (nodes.isJobworkerNodeRegistered(worker)) {
        try {
          if (nodes.getNodeStateManager().getNodeInfo(worker).getNodeStatus().getHealthState()
              == HddsProtos.NodeState.HEALTHY) {
            return true;
          }
        } catch (JobworkerNodeNotFoundException e) {
          // The registry entry was removed concurrently; keep the durable assignment.
        }
      } else if (now - leaderSince < om.getConfiguration().getObject(
          OMJobworkerConfiguration.class).getStaleNodeIntervalMs()) {
        return true;
      }
      unavailable.add(record);
      return unavailable.size() < MAX_COMMANDS_PER_HEARTBEAT;
    });
    // Close the iterator before waiting for Ratis writes.
    for (JobworkerJobRecord record : unavailable) {
      CommandStatus status = CommandStatus.parseFrom(record.getStatus());
      CommandStatus uncertain = status.toBuilder().setStatus(CommandStatus.Status.UNCERTAIN)
          .setMsg("Assigned worker is unavailable; execution outcome requires reconciliation").build();
      commit(record.toBuilder().setStatus(uncertain.toByteString()).build(), record.getRevision());
    }
  }

  public int count(OMJobworkerCommandProto.Type type) throws IOException {
    int[] count = {0};
    scan(record -> {
      CommandStatus status = CommandStatus.parseFrom(record.getStatus());
      if (status.getType() == type && !JobworkerJobState.terminal(status.getStatus())) {
        count[0]++;
      }
      return true;
    });
    return count[0];
  }

  public boolean isInFlight(OMJobworkerCommandProto.Type type, long id) throws IOException {
    JobworkerJobRecord record = get(JobworkerJobState.key(type, id));
    return record != null && !JobworkerJobState.terminal(CommandStatus.parseFrom(record.getStatus()).getStatus());
  }

  @FunctionalInterface
  private interface RecordVisitor {
    boolean visit(JobworkerJobRecord record) throws IOException;
  }

  /** Merge a DB snapshot and committed cache entries without materializing the entire job history. */
  private void scan(RecordVisitor visitor) throws IOException {
    Table<String, JobworkerJobRecord> table = om.getMetadataManager().getJobworkerJobTable();
    Map<String, JobworkerJobRecord> pending = new TreeMap<>();
    Iterator<Map.Entry<CacheKey<String>, CacheValue<JobworkerJobRecord>>> cache = table.cacheIterator();
    while (cache.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<JobworkerJobRecord>> entry = cache.next();
      pending.put(entry.getKey().getCacheKey(), entry.getValue().getCacheValue());
    }
    Iterator<Map.Entry<String, JobworkerJobRecord>> pendingEntries = pending.entrySet().iterator();
    Map.Entry<String, JobworkerJobRecord> cached = pendingEntries.hasNext() ? pendingEntries.next() : null;
    try (TableIterator<String, ? extends Table.KeyValue<String, JobworkerJobRecord>> iterator = table.iterator()) {
      Table.KeyValue<String, JobworkerJobRecord> stored = iterator.hasNext() ? iterator.next() : null;
      while (cached != null || stored != null) {
        JobworkerJobRecord record;
        if (cached != null && (stored == null || cached.getKey().compareTo(stored.getKey()) <= 0)) {
          record = cached.getValue();
          if (stored != null && cached.getKey().equals(stored.getKey())) {
            if (record != null && stored.getValue().getRevision() > record.getRevision()) {
              record = stored.getValue();
            }
            stored = iterator.hasNext() ? iterator.next() : null;
          }
          cached = pendingEntries.hasNext() ? pendingEntries.next() : null;
        } else {
          record = stored.getValue();
          stored = iterator.hasNext() ? iterator.next() : null;
        }
        if (record != null && !visitor.visit(record)) {
          return;
        }
      }
    }
  }
}
