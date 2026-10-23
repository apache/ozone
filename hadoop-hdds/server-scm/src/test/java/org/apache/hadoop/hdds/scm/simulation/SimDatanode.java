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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerActionsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;

/**
 * Simulated datanode. It registers, heartbeats and reports like the datanode state machine, and applies SCM commands to
 * its replicas the way the datanode command handlers do. Its replicas are the ground truth the simulation checks SCM
 * against. For example, this is how a lost replica gets repaired; every step on the SCM side is real SCM code:
 * <pre>{@code
 *  SimDatanode dn3                real SCM                                SimDatanode dn7
 *  (holds #42)                                                            (target)
 *       |                            |                                          |
 *       |--- heartbeat ------------->| SCMDatanodeProtocolServer.sendHeartbeat  |
 *       |                            |                                          |
 *       |                            | RM timer: #42 is under-replicated        |
 *       |                            | under-replication timer: placement picks |
 *       |                            |   dn7 (seeded), queues "replicate #42    |
 *       |                            |   to dn7" for dn3                        |
 *       |<-- heartbeat response -----|                                          |
 *       |    [replicate #42 to dn7]  |                                          |
 *       |                            |                                          |
 *       | decode the proto like      |                                          |
 *       | HeartbeatEndpointTask,     |                                          |
 *       | apply after a delay        |                                          |
 *       |-------------------- push copy (transfer time) ----------------------->|
 *       |                            |                                          |
 *       |                            |<-- heartbeat with incremental report ----|
 *       |                            | ContainerReportHandler (report lane):    |
 *       |                            |   replica on dn7 recorded, pending op    |
 *       |                            |   cleared                                |
 * }</pre>
 */
final class SimDatanode {

  private static final int MAX_ACTIONS_PER_HEARTBEAT = 20;

  private final ScmSimulation sim;
  private final String name;
  private final DatanodeDetails details;
  private final TreeMap<Long, SimReplica> replicas = new TreeMap<>();
  private final Set<PipelineID> pipelines = new LinkedHashSet<>();
  /** One connection per SCM, in SCM order. */
  private final Map<SimScm, Endpoint> endpoints = new LinkedHashMap<>();
  private final Map<SCMCommandProto.Type, Integer> queuedCommands = new EnumMap<>(SCMCommandProto.Type.class);
  private final List<SimScheduler.Timer> timers = new ArrayList<>();
  private final Map<String, SimScheduler.Timer> reportTimers = new HashMap<>();
  private final SimScheduler.WakeUp heartbeatNow;
  private boolean running;
  private boolean partitioned;
  private long generation;
  private long latestTerm;
  private NodeOperationalState persistedOpState = NodeOperationalState.IN_SERVICE;
  private long persistedOpStateExpiry;

  SimDatanode(ScmSimulation sim, String name, DatanodeDetails details) {
    this.sim = sim;
    this.name = name;
    this.details = details;
    this.heartbeatNow = sim.scheduler().newWakeUp(name + ".triggeredHeartbeat", this::heartbeat);
  }

  DatanodeID getId() {
    return details.getID();
  }

  String getName() {
    return name;
  }

  DatanodeDetails getDetails() {
    return details;
  }

  String getRack() {
    return details.getNetworkLocation();
  }

  boolean isRunning() {
    return running;
  }

  boolean isPartitioned() {
    return partitioned;
  }

  boolean hasPipeline(PipelineID id) {
    return pipelines.contains(id);
  }

  SimReplica getReplica(long containerId) {
    return replicas.get(containerId);
  }

  Map<Long, SimReplica> getReplicas() {
    return Collections.unmodifiableMap(replicas);
  }

  NodeOperationalState getPersistedOpState() {
    return persistedOpState;
  }

  // ---- process lifecycle and faults ----

  /** Starts the datanode process; it registers with SCM on its first heartbeat. */
  void start() {
    if (running) {
      return;
    }
    running = true;
    generation++;
    endpoints().forEach(endpoint -> endpoint.registered = false);
    for (SimReplica replica : replicas.values()) {
      if (replica.isRecovering()) {
        // ContainerReader marks a RECOVERING EC replica UNHEALTHY when the datanode starts.
        replica.endRecovering(State.UNHEALTHY);
      }
      replica.markEmptyIfNoBlocks();
    }
    SimConfig config = sim.config();
    Random random = sim.random(SimRandom.DATANODE);
    timers.add(sim.scheduler().scheduleEvery(name + ".heartbeat",
        random.nextInt((int) config.getHeartbeatIntervalMs()), config.getHeartbeatIntervalMs(), this::heartbeat));
    timers.add(sim.scheduler().scheduleEvery(name + ".nodeReport", config.getNodeReportIntervalMs(),
        config.getNodeReportIntervalMs(), () -> endpoints().forEach(endpoint -> endpoint.nodeReportDue = true)));
    scheduleReport("containerReport", config.getContainerReportIntervalMs(),
        () -> endpoints().forEach(endpoint -> endpoint.containerReportDue = true));
    scheduleReport("pipelineReport", config.getPipelineReportIntervalMs(),
        () -> endpoints().forEach(endpoint -> endpoint.pipelineReportDue = true));
    scheduleReport("commandStatusReport", config.getCommandStatusReportIntervalMs(),
        () -> endpoints().forEach(endpoint -> endpoint.commandStatusReportDue = true));
    for (PipelineID id : pipelines) {
      sim.ratisGroup(id).memberUp(this);
    }
  }

  /** Stops the process: queued commands and unsent reports are lost, replicas and Ratis groups stay on disk. */
  void stop() {
    if (!running) {
      return;
    }
    running = false;
    generation++;
    timers.forEach(sim.scheduler()::cancel);
    timers.clear();
    reportTimers.values().forEach(sim.scheduler()::cancel);
    reportTimers.clear();
    heartbeatNow.cancel();
    queuedCommands.clear();
    endpoints().forEach(Endpoint::clear);
    for (PipelineID id : pipelines) {
      sim.ratisGroup(id).memberDown(this);
    }
  }

  /**
   * Restarts the process with different ports, like after a configuration change or an upgrade.
   * @param lostWhileDown replicas on a disk that fails while the process is down
   */
  void restartWithNewPorts(List<Long> lostWhileDown) {
    stop();
    lostWhileDown.forEach(replicas::remove);
    for (Port.Name port : new Port.Name[] {Port.Name.STANDALONE, Port.Name.RATIS}) {
      details.setPort(port, details.getPort(port).getValue() + 1);
    }
    start();
  }

  /** Cuts (or restores) the network between this datanode and the SCMs. */
  void setPartitioned(boolean value) {
    partitioned = value;
  }

  /** A failed volume loses the given replicas; the datanode sends a full report soon. */
  void loseReplicas(List<Long> containerIds) {
    for (Long id : containerIds) {
      replicas.remove(id);
    }
    endpoints().forEach(endpoint -> endpoint.containerReportDue = true);
    triggerHeartbeat();
  }

  /** The replica fails a scan and is marked unhealthy. */
  void markUnhealthy(long containerId) {
    SimReplica replica = replicas.get(containerId);
    if (replica != null && replica.getState() != State.UNHEALTHY) {
      replica.setState(State.UNHEALTHY);
      queueIcr(replica.toProto());
    }
  }

  // ---- data path ----

  /** Applies state committed through the Ratis group of a pipeline, creating the replica if needed. */
  void applyCommitted(PipelineID pipelineId, long containerId, long bcsid, NavigableMap<Long, Long> blocks,
      boolean closed) {
    SimReplica replica = replicas.get(containerId);
    if (replica == null) {
      replica = new SimReplica(containerId, getId().toString(), 0, pipelineId, State.OPEN);
      replicas.put(containerId, replica);
      queueIcr(replica.toProto());
    }
    State state = replica.getState();
    if (state != State.OPEN && state != State.CLOSING) {
      return;
    }
    replica.applyWrites(bcsid, blocks);
    if (closed) {
      replica.setState(State.CLOSED);
      queueIcr(replica.toProto());
    } else if (state == State.OPEN && replica.getUsed() >= sim.config().getContainerCloseThresholdBytes()) {
      addContainerAction(containerId, ContainerAction.Reason.CONTAINER_FULL);
    }
  }

  /** Whether an EC client could write to this datanode's replica of the container now. */
  boolean canTakeEcWrite(long containerId, int replicaIndex) {
    SimReplica replica = replicas.get(containerId);
    return running && (replica == null
        || replica.getState() == State.OPEN && replica.getReplicaIndex() == replicaIndex && !replica.isRecovering());
  }

  /** Applies an EC client write to this datanode's index of the block group, creating the replica if needed. */
  void applyEcWrite(PipelineID pipelineId, long containerId, int replicaIndex, long bcsid,
      NavigableMap<Long, Long> blocks) {
    SimReplica replica = replicas.get(containerId);
    if (replica == null) {
      replica = new SimReplica(containerId, getId().toString(), replicaIndex, pipelineId, State.OPEN);
      replicas.put(containerId, replica);
      queueIcr(replica.toProto());
    }
    replica.applyWrites(bcsid, blocks);
    if (replica.getUsed() >= sim.config().getContainerCloseThresholdBytes()) {
      addContainerAction(containerId, ContainerAction.Reason.CONTAINER_FULL);
    }
  }

  void onBecomeLeader(PipelineID id) {
    endpoints().forEach(endpoint -> endpoint.pipelineReportDue = true);
    triggerHeartbeat();
  }

  private void addContainerAction(long containerId, ContainerAction.Reason reason) {
    ContainerAction action = ContainerAction.newBuilder()
        .setContainerID(containerId)
        .setAction(ContainerAction.Action.CLOSE)
        .setReason(reason)
        .build();
    boolean added = false;
    for (Endpoint endpoint : endpoints()) {
      added |= endpoint.containerActions.putIfAbsent(containerId, action) == null;
    }
    if (added) {
      triggerHeartbeat();
    }
  }

  // ---- heartbeat and reports ----

  /** Marks a full report due after the interval plus up to one more interval of jitter, over and over. */
  private void scheduleReport(String report, long interval, Runnable markDue) {
    long delay = interval + (long) (sim.random(SimRandom.DATANODE).nextDouble() * interval);
    reportTimers.put(report, sim.scheduler().schedule(name + "." + report, delay, () -> {
      markDue.run();
      scheduleReport(report, interval, markDue);
    }));
  }

  private void queueIcr(ContainerReplicaProto report) {
    if (!running) {
      return;
    }
    endpoints().forEach(endpoint -> endpoint.icrs.add(report));
    triggerHeartbeat();
  }

  /** Datanodes send a heartbeat right away when they have urgent reports. */
  private void triggerHeartbeat() {
    if (running) {
      heartbeatNow.request(sim.random(SimRandom.DATANODE).nextInt(200));
    }
  }

  /** Heartbeats to every SCM, like the endpoint tasks of the datanode state machine. */
  private void heartbeat() {
    for (Endpoint endpoint : endpoints()) {
      if (!running) {
        return;
      }
      if (endpoint.registered) {
        heartbeat(endpoint);
      } else {
        register(endpoint);
      }
    }
  }

  private void heartbeat(Endpoint endpoint) {
    SCMHeartbeatRequestProto.Builder hb = SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(currentDetails().getProtoBufMessage())
        .setDatanodeVersion(UpgradeUtils.defaultVersionProto())
        .setCommandQueueReport(commandQueueReport());
    if (endpoint.nodeReportDue) {
      hb.setNodeReport(nodeReport());
      endpoint.nodeReportDue = false;
    }
    List<ContainerReplicaProto> discardedIcrs = new ArrayList<>();
    if (endpoint.containerReportDue) {
      // Building a full report drops the pending incremental reports.
      hb.setContainerReport(containerReport());
      endpoint.containerReportDue = false;
      discardedIcrs.addAll(endpoint.icrs);
      endpoint.icrs.clear();
    }
    List<ContainerReplicaProto> icrs = new ArrayList<>(endpoint.icrs);
    endpoint.icrs.clear();
    for (ContainerReplicaProto icr : icrs) {
      hb.addIncrementalContainerReport(IncrementalContainerReportProto.newBuilder().addReport(icr));
    }
    if (!endpoint.containerActions.isEmpty()) {
      ContainerActionsProto.Builder actions = ContainerActionsProto.newBuilder();
      List<Long> sent = new ArrayList<>();
      for (Map.Entry<Long, ContainerAction> e : endpoint.containerActions.entrySet()) {
        if (sent.size() == MAX_ACTIONS_PER_HEARTBEAT) {
          break;
        }
        actions.addContainerActions(e.getValue());
        sent.add(e.getKey());
      }
      sent.forEach(endpoint.containerActions::remove);
      hb.setContainerActions(actions);
    }
    if (endpoint.pipelineReportDue) {
      hb.setPipelineReports(pipelineReports());
      endpoint.pipelineReportDue = false;
    }
    List<CommandStatus> statuses = new ArrayList<>();
    if (endpoint.commandStatusReportDue) {
      statuses.addAll(endpoint.commandStatuses);
      endpoint.commandStatuses.clear();
      endpoint.commandStatusReportDue = false;
      if (!statuses.isEmpty()) {
        hb.addCommandStatusReports(CommandStatusReportsProto.newBuilder().addAllCmdStatus(statuses));
      }
    }
    if (partitioned || !endpoint.scm.isReachable()) {
      // The RPC fails: incremental reports and command statuses are queued again, everything else is lost.
      endpoint.icrs.addAll(0, icrs);
      endpoint.commandStatuses.addAll(0, statuses);
      if (!discardedIcrs.isEmpty()) {
        sim.reportsLost(this, discardedIcrs);
      }
      return;
    }
    SCMHeartbeatResponseProto response = sim.heartbeat(this, endpoint.scm, hb.build());
    if (response.hasTerm()) {
      latestTerm = Math.max(latestTerm, response.getTerm());
    }
    for (SCMCommandProto command : response.getCommandsList()) {
      receive(endpoint, command);
    }
  }

  private void register(Endpoint endpoint) {
    if (partitioned || !endpoint.scm.isReachable()) {
      return;
    }
    endpoint.registered = sim.register(this, endpoint.scm, currentDetails().getExtendedProtoBufMessage(), nodeReport(),
        containerReport(), pipelineReports(), UpgradeUtils.defaultVersionProto());
    if (endpoint.registered) {
      endpoint.nodeReportDue = false;
      endpoint.containerReportDue = false;
      endpoint.pipelineReportDue = false;
    }
  }

  /** The connections to the SCMs, one per SCM. */
  private Collection<Endpoint> endpoints() {
    for (SimScm scm : sim.scms()) {
      endpoints.computeIfAbsent(scm, Endpoint::new);
    }
    return endpoints.values();
  }

  /** The details a datanode sends over RPC, including its persisted operational state. */
  private DatanodeDetails currentDetails() {
    DatanodeDetails copy = DatanodeDetails.getFromProtoBuf(details.getExtendedProtoBufMessage());
    copy.setPersistedOpState(persistedOpState);
    copy.setPersistedOpStateExpiryEpochSec(persistedOpStateExpiry);
    return copy;
  }

  private NodeReportProto nodeReport() {
    long capacity = sim.config().getDatanodeCapacityBytes();
    long used = 0;
    for (SimReplica r : replicas.values()) {
      used += r.getUsed();
    }
    long metaCapacity = sim.config().getDatanodeCapacityBytes() / 10;
    return HddsTestUtils.createNodeReport(
        Collections.singletonList(HddsTestUtils.createStorageReport(getId(), "/data/" + name,
            capacity, used, capacity - used, StorageTypeProto.DISK)),
        Collections.singletonList(HddsTestUtils.createMetadataStorageReport("/ratis/" + name,
            metaCapacity, 0, metaCapacity, StorageTypeProto.DISK)));
  }

  private ContainerReportsProto containerReport() {
    ContainerReportsProto.Builder report = ContainerReportsProto.newBuilder();
    for (SimReplica replica : replicas.values()) {
      // Like ContainerSet#getContainerReport, which skips RECOVERING containers.
      if (!replica.isRecovering()) {
        report.addReports(replica.toProto());
      }
    }
    return report.build();
  }

  private PipelineReportsProto pipelineReports() {
    PipelineReportsProto.Builder report = PipelineReportsProto.newBuilder();
    for (PipelineID id : pipelines) {
      report.addPipelineReport(PipelineReport.newBuilder()
          .setPipelineID(id.getProtobuf())
          .setIsLeader(getId().equals(sim.ratisGroup(id).getLeader())));
    }
    return report.build();
  }

  private CommandQueueReportProto commandQueueReport() {
    CommandQueueReportProto.Builder report = CommandQueueReportProto.newBuilder();
    for (Map.Entry<SCMCommandProto.Type, Integer> e : queuedCommands.entrySet()) {
      report.addCommand(e.getKey());
      report.addCount(e.getValue());
    }
    return report.build();
  }

  // ---- commands ----

  private void receive(Endpoint from, SCMCommandProto proto) {
    SCMCommandProto.Type type = proto.getCommandType();
    if (type == SCMCommandProto.Type.reregisterCommand) {
      from.registered = false;
      triggerHeartbeat();
      return;
    }
    SCMCommand<?> command = decode(proto);
    if (command == null) {
      sim.count("datanode.ignored." + type);
      return;
    }
    queuedCommands.merge(type, 1, Integer::sum);
    long expectedGeneration = generation;
    long delay = sim.config().getCommandDelayMs(type, sim.random(SimRandom.DATANODE));
    sim.scheduler().schedule(name + ".execute " + type, delay, () -> {
      if (generation != expectedGeneration) {
        return;
      }
      queuedCommands.merge(type, -1, Integer::sum);
      queuedCommands.remove(type, 0);
      if (command.getTerm() < latestTerm) {
        sim.count("datanode.staleTermCommandDropped");
        return;
      }
      execute(command);
    });
  }

  /** Reads a command from a heartbeat response like HeartbeatEndpointTask; null for commands not simulated. */
  private static SCMCommand<?> decode(SCMCommandProto proto) {
    final SCMCommand<?> command;
    switch (proto.getCommandType()) {
    case createPipelineCommand:
      command = CreatePipelineCommand.getFromProtobuf(proto.getCreatePipelineCommandProto());
      break;
    case closePipelineCommand:
      command = ClosePipelineCommand.getFromProtobuf(proto.getClosePipelineCommandProto());
      break;
    case closeContainerCommand:
      command = CloseContainerCommand.getFromProtobuf(proto.getCloseContainerCommandProto());
      break;
    case replicateContainerCommand:
      command = ReplicateContainerCommand.getFromProtobuf(proto.getReplicateContainerCommandProto());
      break;
    case deleteContainerCommand:
      command = DeleteContainerCommand.getFromProtobuf(proto.getDeleteContainerCommandProto());
      break;
    case setNodeOperationalStateCommand:
      command = SetNodeOperationalStateCommand.getFromProtobuf(proto.getSetNodeOperationalStateCommandProto());
      break;
    case reconstructECContainersCommand:
      command = ReconstructECContainersCommand.getFromProtobuf(proto.getReconstructECContainersCommandProto());
      break;
    case deleteBlocksCommand:
      command = DeleteBlocksCommand.getFromProtobuf(proto.getDeleteBlocksCommandProto());
      break;
    default:
      return null;
    }
    if (proto.hasTerm()) {
      command.setTerm(proto.getTerm());
    }
    if (proto.hasDeadlineMsSinceEpoch()) {
      command.setDeadline(proto.getDeadlineMsSinceEpoch());
    }
    return command;
  }

  private void execute(SCMCommand<?> command) {
    switch (command.getType()) {
    case createPipelineCommand:
      createPipeline((CreatePipelineCommand) command);
      break;
    case closePipelineCommand:
      closePipeline(((ClosePipelineCommand) command).getPipelineID());
      break;
    case closeContainerCommand:
      closeContainer((CloseContainerCommand) command);
      break;
    case replicateContainerCommand:
      replicate((ReplicateContainerCommand) command);
      break;
    case deleteContainerCommand:
      delete((DeleteContainerCommand) command);
      break;
    case reconstructECContainersCommand:
      reconstruct((ReconstructECContainersCommand) command);
      break;
    case deleteBlocksCommand:
      deleteBlocks((DeleteBlocksCommand) command);
      break;
    case setNodeOperationalStateCommand:
      SetNodeOperationalStateCommand op = (SetNodeOperationalStateCommand) command;
      persistedOpState = op.getOpState();
      persistedOpStateExpiry = op.getStateExpiryEpochSeconds();
      break;
    default:
      throw new IllegalStateException("Not simulated: " + command.getType());
    }
  }

  private void createPipeline(CreatePipelineCommand command) {
    PipelineID id = command.getPipelineID();
    List<DatanodeID> members = new ArrayList<>();
    for (DatanodeDetails dn : command.getNodeList()) {
      members.add(dn.getID());
    }
    SimRatisGroup group = sim.ratisGroup(id, members, command.getPriorityList());
    if (pipelines.add(id)) {
      // The group starts before a leader is known; report it right away.
      endpoints().forEach(endpoint -> endpoint.pipelineReportDue = true);
      triggerHeartbeat();
      group.join(this);
    }
  }

  private void closePipeline(PipelineID id) {
    if (!pipelines.remove(id)) {
      return;
    }
    SimRatisGroup group = sim.ratisGroup(id);
    group.leave(this);
    // Removing the group quasi-closes the open containers it wrote to.
    for (Long containerId : group.getContainers()) {
      SimReplica replica = replicas.get(containerId);
      if (replica != null && (replica.getState() == State.OPEN || replica.getState() == State.CLOSING)) {
        if (replica.getState() == State.OPEN) {
          replica.setState(State.CLOSING);
          queueIcr(replica.toProto());
        }
        replica.setState(State.QUASI_CLOSED);
        queueIcr(replica.toProto());
      }
    }
  }

  private void closeContainer(CloseContainerCommand command) {
    SimReplica replica = replicas.get(command.getContainerID());
    if (replica == null) {
      return;
    }
    if (replica.getState() == State.OPEN) {
      replica.setState(State.CLOSING);
      queueIcr(replica.toProto());
    }
    if (replica.getState() == State.CLOSING) {
      PipelineID pipelineId = command.getPipelineID();
      if (pipelineId != null && pipelines.contains(pipelineId)) {
        // Submitted through Ratis; followers get NotLeader, the leader's close applies to all replicas.
        sim.ratisGroup(pipelineId).closeContainer(this, command.getContainerID());
      } else {
        replica.setState(command.isForce() ? State.CLOSED : State.QUASI_CLOSED);
        queueIcr(replica.toProto());
      }
    } else if (replica.getState() == State.QUASI_CLOSED && command.isForce()) {
      replica.setState(State.CLOSED);
      queueIcr(replica.toProto());
    }
  }

  private void replicate(ReplicateContainerCommand command) {
    if (command.hasExpired(sim.now())) {
      sim.count("datanode.replicate.expired");
      return;
    }
    SimReplica source = replicas.get(command.getContainerID());
    if (source == null || !(source.getState() == State.CLOSED || source.getState() == State.QUASI_CLOSED
        || source.getState() == State.UNHEALTHY)) {
      sim.count("datanode.replicate.noSource");
      return;
    }
    SimDatanode target = sim.datanode(command.getTargetDatanode().getID());
    SimReplica copy = source.importedCopy();
    long transferMs = sim.config().getReplicationTransferMs(copy.getUsed(), sim.random(SimRandom.DATANODE));
    sim.scheduler().schedule(name + ".push #" + command.getContainerID() + " to " + target.getName(), transferMs,
        () -> target.importReplica(copy, this));
  }

  private void importReplica(SimReplica copy, SimDatanode source) {
    if (!running) {
      sim.count("datanode.import.targetDown");
      return;
    }
    if (replicas.containsKey(copy.getContainerId())) {
      // SendContainerRequestHandler rejects it with CONTAINER_EXISTS.
      sim.count("datanode.import.containerExists");
      return;
    }
    sim.checkImport(this, copy, source);
    copy.markEmptyIfNoBlocks();
    replicas.put(copy.getContainerId(), copy);
    queueIcr(copy.toProto());
  }

  /**
   * Coordinates an EC reconstruction like ECReconstructionCoordinator: it creates RECOVERING replicas of the missing
   * indexes on the targets, rebuilds them from the source indexes, and closes them. If a source or a target fails, it
   * deletes the replicas it created. If the coordinator itself stops, they stay RECOVERING until they time out.
   */
  private void reconstruct(ReconstructECContainersCommand command) {
    if (command.hasExpired(sim.now())) {
      sim.count("datanode.reconstruct.expired");
      return;
    }
    long containerId = command.getContainerID();
    if (command.getSources().size() < command.getEcReplicationConfig().getData()) {
      sim.violation("SCM asked " + name + " to reconstruct #" + containerId + " from " + command.getSources().size()
          + " sources, fewer than the " + command.getEcReplicationConfig().getData() + " data indexes");
      return;
    }
    SimReplica template = readSources(command);
    if (template == null) {
      sim.count("datanode.reconstruct.sourceUnavailable");
      return;
    }
    byte[] missing = command.getMissingContainerIndexes().toByteArray();
    List<DatanodeDetails> targetNodes = command.getTargetDatanodes();
    Map<SimDatanode, SimReplica> created = new LinkedHashMap<>();
    for (int i = 0; i < targetNodes.size(); i++) {
      SimDatanode target = sim.datanode(targetNodes.get(i).getID());
      if (target == null || !target.running || target.replicas.containsKey(containerId)) {
        // createRecoveringContainer fails, for example with CONTAINER_EXISTS.
        sim.count("datanode.reconstruct.targetUnavailable");
        created.forEach((t, replica) -> t.dropReconstructed(replica));
        return;
      }
      SimReplica replica = SimReplica.recovering(containerId, target.getId().toString(), missing[i], template);
      target.addRecovering(replica);
      created.put(target, replica);
    }
    long expectedGeneration = generation;
    long ms = sim.config().getReconstructionMs(template.getUsed(), sim.random(SimRandom.DATANODE));
    sim.scheduler().schedule(name + ".reconstruct #" + containerId, ms, () -> {
      if (generation != expectedGeneration) {
        sim.count("datanode.reconstruct.coordinatorStopped");
        return;
      }
      boolean targetsReady = created.entrySet().stream().allMatch(e -> e.getKey().isRecovering(e.getValue()));
      if (readSources(command) == null || !targetsReady) {
        sim.count("datanode.reconstruct.failed");
        created.forEach((t, replica) -> t.dropReconstructed(replica));
        return;
      }
      created.forEach((t, replica) -> t.finishRecovering(replica));
      sim.recordReconstruction(this, containerId, created.keySet());
    });
  }

  /**
   * The replica with the most data among the reconstruction sources, or null if a source is down or does not hold its
   * index as a closed replica.
   */
  private SimReplica readSources(ReconstructECContainersCommand command) {
    SimReplica best = null;
    for (DatanodeDetailsAndReplicaIndex source : command.getSources()) {
      SimDatanode dn = sim.datanode(source.getDnDetails().getID());
      SimReplica replica = dn == null || !dn.running ? null : dn.replicas.get(command.getContainerID());
      if (replica == null || replica.isRecovering() || replica.getReplicaIndex() != source.getReplicaIndex()
          || replica.getState() != State.CLOSED) {
        return null;
      }
      if (best == null || replica.getKeys() > best.getKeys()) {
        best = replica;
      }
    }
    return best;
  }

  /** Holds a replica being rebuilt here, until the coordinator closes it or it times out and becomes UNHEALTHY. */
  private void addRecovering(SimReplica replica) {
    replicas.put(replica.getContainerId(), replica);
    sim.scheduler().schedule(name + ".recoveringTimeout #" + replica.getContainerId(),
        sim.config().getRecoveringTimeoutMs(), () -> {
          if (replicas.get(replica.getContainerId()) == replica && replica.isRecovering()) {
            replica.endRecovering(State.UNHEALTHY);
            queueIcr(replica.toProto());
          }
        });
  }

  private boolean isRecovering(SimReplica replica) {
    return running && replicas.get(replica.getContainerId()) == replica && replica.isRecovering();
  }

  private void finishRecovering(SimReplica replica) {
    replica.endRecovering(State.CLOSED);
    queueIcr(replica.toProto());
  }

  /** Deletes a replica a failed reconstruction created here, like deleteContainerInState(RECOVERING, UNHEALTHY). */
  private void dropReconstructed(SimReplica replica) {
    if (!running || replicas.get(replica.getContainerId()) != replica
        || !replica.isRecovering() && replica.getState() != State.UNHEALTHY) {
      return;
    }
    replicas.remove(replica.getContainerId());
    if (!replica.isRecovering()) {
      queueIcr(replica.toProto().toBuilder().setState(State.DELETED).build());
    }
  }

  /**
   * Applies block deletion transactions like DeleteBlocksCommandHandler, and acknowledges them in the next command
   * status report. A transaction for a container this datanode does not have fails.
   */
  private void deleteBlocks(DeleteBlocksCommand command) {
    ContainerBlocksDeletionACKProto.Builder ack = ContainerBlocksDeletionACKProto.newBuilder()
        .setDnId(getId().toString());
    for (DeletedBlocksTransaction tx : command.blocksTobeDeleted()) {
      SimReplica replica = replicas.get(tx.getContainerID());
      if (replica != null) {
        List<Long> removed = replica.applyDeleteTransaction(tx.getTxID(), tx.getLocalIDList());
        sim.record("deleteBlocks", name + " tx" + tx.getTxID() + " " + replica + " removed " + removed);
        sim.checkBlockDelete(this, replica, removed);
        sim.recordDeleteAck(this, tx.getTxID(), replica);
      } else {
        sim.record("deleteBlocks", name + " tx" + tx.getTxID() + " #" + tx.getContainerID() + " not found");
      }
      ack.addResults(DeleteBlockTransactionResult.newBuilder()
          .setTxID(tx.getTxID())
          .setContainerID(tx.getContainerID())
          .setSuccess(replica != null));
    }
    CommandStatus status = CommandStatus.newBuilder()
        .setCmdId(command.getId())
        .setStatus(CommandStatus.Status.EXECUTED)
        .setType(SCMCommandProto.Type.deleteBlocksCommand)
        .setBlockDeletionAck(ack)
        .build();
    endpoints().forEach(endpoint -> endpoint.commandStatuses.add(status));
  }

  private void delete(DeleteContainerCommand command) {
    if (command.hasExpired(sim.now())) {
      sim.count("datanode.delete.expired");
      return;
    }
    SimReplica replica = replicas.get(command.getContainerID());
    if (replica == null) {
      return;
    }
    if (!command.isForce() && (replica.getState() == State.OPEN || replica.hasData())) {
      sim.count("datanode.delete.rejected");
      return;
    }
    sim.checkDelete(this, replica);
    replicas.remove(command.getContainerID());
    ContainerReplicaProto deleted = replica.toProto().toBuilder().setState(State.DELETED).build();
    queueIcr(deleted);
  }

  /**
   * A connection to one SCM, with what is queued for it, like the per endpoint queues of StateContext: registration,
   * incremental reports, container actions and command statuses, and which full reports are due.
   */
  private static final class Endpoint {
    private final SimScm scm;
    private final List<ContainerReplicaProto> icrs = new ArrayList<>();
    private final List<CommandStatus> commandStatuses = new ArrayList<>();
    private final Map<Long, ContainerAction> containerActions = new LinkedHashMap<>();
    private boolean registered;
    private boolean containerReportDue;
    private boolean pipelineReportDue;
    private boolean nodeReportDue;
    private boolean commandStatusReportDue;

    Endpoint(SimScm scm) {
      this.scm = scm;
    }

    /** The process stopped: what was queued is lost. */
    void clear() {
      icrs.clear();
      commandStatuses.clear();
      containerActions.clear();
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
