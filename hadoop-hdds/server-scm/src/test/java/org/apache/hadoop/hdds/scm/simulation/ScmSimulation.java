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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ExtendedDatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DatanodeVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DeadNodeHandler;
import org.apache.hadoop.hdds.scm.node.InvalidNodeStateException;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.ozone.test.MockClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deterministic simulation of SCM.
 * <p>
 * The real {@link StorageContainerManager} runs in-process with simulated datanodes ({@link SimDatanode}), which
 * register and heartbeat through its datanode protocol server. SCM is built with a simulated clock, seeded random
 * sources and an event queue whose handlers run on the simulation's scheduler, and the simulation runs the services SCM
 * would run on their own threads. A single thread runs everything through {@link SimScheduler}, so a run is fully
 * determined by its {@link SimConfig}: rerunning a seed reproduces the exact same trace.
 * <pre>{@code
 * +-------------------------------------------------------------------------+
 * |                     real StorageContainerManager                        |
 * |                                                                         |
 * |   SCMDatanodeProtocolServer   SCMNodeManager      ReplicationManager    |
 * |   ContainerManager            PipelineManager     DatanodeAdminMonitor  |
 * |   event handlers: DeadNodeHandler, ContainerReportHandler, ...          |
 * |                                                                         |
 * |   injected through SCMConfigurator:                                     |
 * |     SimEventQueue ... each handler call becomes a scheduler task        |
 * |     MockClock ....... simulated time                                    |
 * |     seeded Random ... topology, placement, pipeline IDs, RM shuffle     |
 * +-------------------^-----------------------------------+-----------------+
 *                     |                                   |
 *       register and heartbeat,                 commands in the heartbeat
 *       carrying node, container                response (replicate, delete,
 *       and pipeline reports                    close, create pipeline, ...)
 *                     |                                   v
 * +-------------------+-----------------------------------+-----------------+
 * |   SimDatanode x 8..12 on 3 racks                                        |
 * |     SimReplica ...... ground truth: which replicas really exist         |
 * |     SimRatisGroup ... one per pipeline: leader election, commits        |
 * +-------------------^-----------------------------------^-----------------+
 *                     |                                   |
 *           +---------+---------+             +-----------+------------+
 *           | SimWorkload       |             | SimChaos               |
 *           | client writes     |             | crash, partition, lose |
 *           | every ~2 s        |             | or corrupt replicas,   |
 *           +-------------------+             | decommission (node or  |
 *                                             | rack), maintenance,    |
 *                                             | add node, restart SCM  |
 *                                             +------------------------+
 * }</pre>
 * A run has two phases. During the chaos phase a client writes to containers while {@link SimChaos} crashes,
 * partitions, decommissions and adds datanodes and damages replicas. Then faults stop, every datanode comes back, and
 * the settle phase gives SCM time to repair the cluster. Safety invariants are checked throughout. At the end SCM must
 * have repaired everything, and then stay quiet: no more replicate or delete commands. With the default
 * {@link SimConfig}:
 * <pre>{@code
 * simulated time
 * 0                     2h                           3h            3h15m
 * +---------------------+----------------------------+-------------+
 * |        chaos        |           settle           |    quiet    |
 * +---------------------+----------------------------+-------------+
 * | client writes;      | writes and faults stop;    | SCM must    |
 * | a fault every 1-3   | every datanode comes back; | send no     |
 * | min, at most 2      | SCM repairs the cluster    | replicate / |
 * | datanodes down      |                            | delete cmds |
 * +---------------------+----------------------------+-------------+
 * |<-------------- safety checks every 30 s, whole run ----------->|
 *                                                    ^
 *                                                    | convergence check:
 *                                                    |  - every container correctly replicated
 *                                                    |  - no CLOSING container, no ALLOCATED pipeline
 *                                                    |  - every datanode registered, IN_SERVICE, HEALTHY
 *                                                    |  - SCM's replicas == datanodes' replicas
 *
 * pass: next seed        fail: violations + seed + full trace in target/scm-simulation
 * }</pre>
 * With {@link SimConfig#setScmCount} above one, several SCMs run in a Ratis group ({@link SimScmRatis}); the checks
 * look at the leader. Not simulated: the container balancer, reconciliation and security.
 */
final class ScmSimulation implements AutoCloseable {

  /** Simulated time starts far in the future, so stray wall clock timestamps always look old. */
  static final Instant START = Instant.parse("2100-01-01T00:00:00Z");

  private static final Logger LOG = LoggerFactory.getLogger(ScmSimulation.class);
  /** The simulation running on this thread, for the plugins SCM creates from its configuration. */
  private static final ThreadLocal<ScmSimulation> RUNNING = new ThreadLocal<>();
  private static final ReplicationConfig RATIS_THREE = RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
  private static final String OWNER = "sim";
  private static final int MAX_VIOLATIONS = 20;
  private static final long SAFETY_CHECK_INTERVAL_MS = 30_000;
  /** How long SCM is watched after settling, when it must not move replicas any more. */
  private static final long QUIET_WINDOW_MS = 15 * 60_000;
  /** Container health states SCM must have repaired after settling. */
  private static final List<ContainerHealthState> UNREPAIRED_STATES = Arrays.asList(
      ContainerHealthState.UNDER_REPLICATED, ContainerHealthState.OVER_REPLICATED,
      ContainerHealthState.MIS_REPLICATED, ContainerHealthState.MISSING, ContainerHealthState.UNHEALTHY,
      ContainerHealthState.OPEN_UNHEALTHY, ContainerHealthState.OPEN_WITHOUT_PIPELINE,
      ContainerHealthState.UNHEALTHY_UNDER_REPLICATED, ContainerHealthState.UNHEALTHY_OVER_REPLICATED,
      ContainerHealthState.MISSING_UNDER_REPLICATED, ContainerHealthState.QUASI_CLOSED_STUCK_UNDER_REPLICATED,
      ContainerHealthState.QUASI_CLOSED_STUCK_OVER_REPLICATED, ContainerHealthState.QUASI_CLOSED_STUCK_MISSING);
  /** Container health states with no healthy replica left. */
  private static final Set<ContainerHealthState> WITHOUT_HEALTHY_COPY = EnumSet.of(
      ContainerHealthState.MISSING, ContainerHealthState.UNHEALTHY, ContainerHealthState.UNHEALTHY_UNDER_REPLICATED,
      ContainerHealthState.UNHEALTHY_OVER_REPLICATED, ContainerHealthState.MISSING_UNDER_REPLICATED,
      ContainerHealthState.QUASI_CLOSED_STUCK_MISSING);
  /** Handlers that repair node, topology and pipeline state after a node state change. */
  private static final List<String> NODE_LIFECYCLE_HANDLERS = Arrays.asList(
      "StaleNodeHandler", "DeadNodeHandler", "HealthyReadOnlyNodeHandler", "NewNodeHandler",
      "NodeAddressUpdateHandler", "StartDatanodeAdminHandler", "PipelineReportHandler");

  private final SimConfig config;
  private final Path dir;
  private final MockClock clock = new MockClock(START, ZoneOffset.UTC);
  private final Map<SimRandom, Random> randoms = new EnumMap<>(SimRandom.class);
  private final SimTrace trace;
  private final SimScheduler scheduler;
  private final Map<DatanodeID, SimDatanode> datanodes = new LinkedHashMap<>();
  private final Map<PipelineID, SimRatisGroup> ratisGroups = new LinkedHashMap<>();
  private final Map<PipelineID, String> pipelineNames = new HashMap<>();
  private final Map<String, Long> counters = new TreeMap<>();
  private final List<String> violations = new ArrayList<>();
  private final SimScmRatis ratis = new SimScmRatis(this);
  private final List<SimScm> scms = new ArrayList<>();
  /** Blocks the client wrote and has not deleted, by container: local ID to bytes per replica. */
  private final Map<Long, NavigableMap<Long, Long>> liveBlocks = new TreeMap<>();
  /** Blocks the client deleted, by container. */
  private final Map<Long, Set<Long>> deletedBlocks = new HashMap<>();
  /**
   * Replicas SCM did not know while it purged delete transactions, by {@link #replicaKey} and cause. Such a replica
   * keeps the deleted blocks for good, and so do copies made from it. Known causes: the datanode was down, dead to SCM
   * or not registered (HDDS-13928, left to container reconciliation; SCM may even bring back a deleted container for
   * it, HDDS-12421), a Ratis follower created the replica from log entries it missed, a copy finished after its pending
   * ADD expired, or an EC reconstruction was still running (both HDDS-17152), or the leader installed a snapshot and
   * has not heard from the datanode since (HDDS-16925).
   */
  private final Map<String, String> missedDeletions = new HashMap<>();
  /**
   * Replicas each SCM dropped when it installed a snapshot, until a report from their datanode brings them back. If the
   * SCM is the leader meanwhile, it may purge deletions they missed (HDDS-16925).
   */
  private final Map<SimScm, Set<String>> forgottenReplicas = new HashMap<>();
  /** When each SCM last heard from each datanode, by "scm@datanode". */
  private final Map<String, Long> lastContact = new HashMap<>();
  /** When each SCM first heard from each datanode again after a gap of at least the dead interval. */
  private final Map<String, Long> lastReturn = new HashMap<>();
  /** Whether the datanode was in maintenance, if the last contact was a registration rather than a heartbeat. */
  private final Map<String, Boolean> lastRegistration = new HashMap<>();
  /** Delete transactions seen in SCM's log, by ID; the ones that disappear from it were purged. */
  private final Map<Long, DeletedBlocksTransaction> deleteTransactions = new TreeMap<>();
  /** The replica that applied each delete transaction on each datanode, by "txID@datanode". */
  private final Map<String, SimReplica> deleteAcks = new HashMap<>();
  /** Last traced SCM view of each watched container. */
  private final Map<Long, String> watchedViews = new HashMap<>();
  private final String clusterId;

  /** The SCM leading, or the last one that led; the checks look at its state. */
  private SimScm leader;
  private StorageContainerManager scm;
  private SCMNodeManager nodeManager;
  private int timerFailuresSeen;
  /** Replicate and delete commands SCM sends while it should be quiet; null outside that window. */
  private List<String> replicaMoves;

  ScmSimulation(SimConfig config, Path dir) {
    this.config = config;
    this.dir = dir;
    for (SimRandom stream : SimRandom.values()) {
      randoms.put(stream, new Random(mix(config.getSeed() + 0x9E3779B97F4A7C15L * (stream.ordinal() + 1))));
    }
    this.trace = new SimTrace(2000, config.isKeepFullTrace());
    this.scheduler = new SimScheduler(clock, random(SimRandom.SCHEDULER), trace,
        config.getSlowHandlerProbability(), config.getMaxSlowHandlerMs());
    Random ids = random(SimRandom.IDS);
    this.clusterId = new UUID(ids.nextLong(), ids.nextLong()).toString();
    boolean ha = config.getScmCount() > 1;
    for (int i = 1; i <= config.getScmCount(); i++) {
      String name = ha ? "scm" + i : "scm";
      SimScm member = new SimScm(this, ratis, name, ha, dir.resolve(name), clusterId,
          new UUID(ids.nextLong(), ids.nextLong()).toString());
      scms.add(member);
      ratis.add(member);
    }
    // Several SCM instances register the same metrics sources in one JVM.
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  /** SplitMix64 finalizer, so that nearby seeds give unrelated streams. */
  private static long mix(long z) {
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }

  // ---- running ----

  SimResult run() throws IOException {
    LOG.info("Starting SCM simulation {}", config);
    RUNNING.set(this);
    for (SimScm member : scms) {
      member.start();
    }
    ratis.electNow();
    for (int i = 0; i < config.getInitialDatanodes(); i++) {
      addDatanode().start();
    }
    SimWorkload workload = new SimWorkload(this);
    workload.start();
    SimChaos chaos = config.isFaults() ? new SimChaos(this) : null;
    if (chaos != null) {
      chaos.start();
    }
    scheduler.scheduleEvery("sim.checkSafety", SAFETY_CHECK_INTERVAL_MS, SAFETY_CHECK_INTERVAL_MS, this::checkSafety);

    long chaosEnd = clock.millis() + config.getChaosDurationMs();
    runUntil(chaosEnd);
    trace.record(scheduler.elapsed(), "phase", "settle");
    workload.stop();
    if (chaos != null) {
      chaos.stopAndHeal();
    }
    runUntil(chaosEnd + config.getSettleDurationMs());
    if (violations.size() < MAX_VIOLATIONS) {
      checkConvergence();
      checkQuiet();
    }
    return result();
  }

  private void runUntil(long time) {
    while (clock.millis() < time && violations.size() < MAX_VIOLATIONS) {
      if (!scheduler.step()) {
        break;
      }
      collectHandlerFailures();
      checkSafeModeExits();
    }
  }

  private SimResult result() throws IOException {
    Path traceFile = null;
    if (!violations.isEmpty() || config.isKeepFullTrace()) {
      traceFile = config.getTraceDir()
          .resolve("trace-" + config.getSeed() + (scms.size() > 1 ? "-ha" : "") + ".txt").toAbsolutePath();
      trace.writeTo(traceFile);
    }
    return new SimResult(config, new ArrayList<>(violations), new TreeMap<>(counters), trace.hash(),
        scheduler.steps(), scheduler.elapsed(), trace.recentLines(), traceFile);
  }

  @Override
  public void close() throws IOException {
    try {
      for (SimScm member : scms) {
        member.stop(false);
      }
    } finally {
      RUNNING.remove();
    }
  }

  static ScmSimulation running() {
    return RUNNING.get();
  }

  // ---- SCM ----

  /**
   * Restarts an SCM. What it flushed survives in its DB, and it applies the later log entries again; events being
   * handled, pending commands and all other in-memory state are lost, and datanodes register again.
   */
  void restartScm(SimScm target) throws IOException {
    record("scm.restart", target.getName() + " in term " + ratis.getTerm());
    target.stop(true);
    target.start();
  }

  List<SimScm> scms() {
    return scms;
  }

  /**
   * Adds an SCM to the group, as `ozone scm --bootstrap` and SCMHAManagerImpl#start do: it starts with an empty DB, and
   * the leader brings it up to date with its log, or with a snapshot if it purged the log. Datanodes heartbeat to it as
   * to the other SCMs.
   */
  void bootstrapScm() throws IOException {
    Random ids = random(SimRandom.IDS);
    SimScm member = new SimScm(this, ratis, "scm" + (scms.size() + 1), true, dir.resolve("scm" + (scms.size() + 1)),
        clusterId, new UUID(ids.nextLong(), ids.nextLong()).toString());
    record("scm.bootstrap", member.getName() + " in term " + ratis.getTerm());
    scms.add(member);
    ratis.add(member);
    member.start();
  }

  SimScmRatis ratis() {
    return ratis;
  }

  /** Called when an SCM becomes the leader: from now on the checks look at its state. */
  void leaderChanged(SimScm newLeader) {
    leader = newLeader;
    scm = newLeader.getScm();
    nodeManager = newLeader.getNodeManager();
    // The new leader's DB may not have every transaction the old one had yet; start over from what it has.
    deleteTransactions.clear();
  }

  /** Whether a leader that is up and ready serves SCM clients now. */
  boolean hasReadyLeader() {
    SimScm current = ratis.getLeader();
    return current != null && current.isConnected() && current.getScm().getScmContext().isLeaderReady();
  }

  MockClock clock() {
    return clock;
  }

  /**
   * DeadNodeHandler drops the replicas of a datanode that is down or cut off. SCM learns about them again only from the
   * next full report of the datanode, so it may purge deletions they missed meanwhile (HDDS-13928). An SCM cut off from
   * the datanodes does so for all of them; with SCM HA it may become the leader before their next reports. SCM also
   * drops the replicas of a datanode it heard from within the dead interval, if that was a registration, which does not
   * refresh the heartbeat time: the next health check finds the datanode DEAD (HDDS-17067), or maintenance ends while
   * SCM still has it DEAD from before it registered (HDDS-17313).
   */
  void beforeHandler(SimScm owner, EventHandler<?> handler, Object payload) {
    if (!(handler instanceof DeadNodeHandler) || !(payload instanceof DatanodeDetails)) {
      return;
    }
    SimDatanode dn = datanodes.get(((DatanodeDetails) payload).getID());
    NodeStatus status = dn == null ? null : nodeStatus(owner.getNodeManager(), dn);
    if (dn == null || status == null || status.isInMaintenance()) {
      return;
    }
    String contact = owner.getName() + "@" + dn.getName();
    long deadIntervalMs = config.getDeadNodeIntervalMs();
    boolean heardRecently = lastContact.containsKey(contact) && now() - lastContact.get(contact) < deadIntervalMs;
    // A datanode that is back from a real outage only by heartbeats: SCM handles the DEAD_NODE of the outage late.
    boolean lateForOutage = !lastRegistration.containsKey(contact) && lastReturn.containsKey(contact)
        && now() - lastReturn.get(contact) < deadIntervalMs;
    String cause;
    if (!owner.isConnected()) {
      // This SCM was cut off from the datanodes, which are fine; it keeps no replica of theirs until their next full
      // report, also if it becomes the leader meanwhile.
      count("scm.deadNodeHandler.scmCutOff");
      cause = "droppedByCutOffScm";
    } else if (dn.isRunning() && !dn.isPartitioned() && heardRecently && !lateForOutage) {
      Boolean registeredInMaintenance = lastRegistration.get(contact);
      cause = registeredInMaintenance == null ? "droppedWhileLive"
          : registeredInMaintenance ? "droppedAfterMaintenance" : "droppedAfterRegister";
      count("scm.deadNodeHandler." + cause);
      record("deadNodeHandler", dn.getName() + " is running, " + status
          + (scms.size() > 1 ? " in " + owner.getName() : "") + ": " + cause);
    } else {
      cause = "droppedAsDead";
    }
    dn.getReplicas().keySet().forEach(id -> missedDeletions.putIfAbsent(replicaKey(id, dn), cause));
  }

  void afterHandler(SimScm owner, EventHandler<?> handler, Object payload) {
    Set<String> forgotten = forgottenReplicas.get(owner);
    SimDatanode reporter = payload instanceof ReportFromDatanode
        ? datanodes.get(((ReportFromDatanode<?>) payload).getDatanodeDetails().getID()) : null;
    if (forgotten != null && reporter != null) {
      if (payload instanceof ContainerReportFromDatanode) {
        forgotten.removeIf(key -> key.endsWith("@" + reporter.getName()));
      } else if (payload instanceof IncrementalContainerReportFromDatanode) {
        ((IncrementalContainerReportFromDatanode) payload).getReport().getReportList()
            .forEach(replica -> forgotten.remove(replicaKey(replica.getContainerID(), reporter)));
      }
    }
    if (owner != leader) {
      return;
    }
    if (handler instanceof DeletedBlockLogImpl) {
      checkPurgedTransactions();
    }
    traceWatchedContainers(handler);
  }

  /** Called after the block deleting service ran: it purges transactions that all replicas already applied. */
  void blockDeletingServiceRan(SimScm owner) {
    if (owner == leader) {
      checkPurgedTransactions();
    }
  }

  /** Traces SCM's replicas of the watched containers whenever a handler changed them. */
  private void traceWatchedContainers(EventHandler<?> handler) {
    for (long id : config.getWatchedContainers()) {
      String view;
      try {
        view = scm.getContainerManager().getContainer(ContainerID.valueOf(id)).getState() + " "
            + scm.getContainerManager().getContainerReplicas(ContainerID.valueOf(id)).stream()
            .map(r -> name(r.getDatanodeDetails()) + "=" + r.getState()).sorted().collect(Collectors.joining(","));
      } catch (ContainerNotFoundException e) {
        continue;
      }
      if (!view.equals(watchedViews.put(id, view))) {
        record("watch", "#" + id + " " + view + " after " + handler.getClass().getSimpleName());
      }
    }
  }

  /**
   * SCM purges a delete transaction once every replica it knows has applied it, so right after a purge no replica it
   * knows may hold the blocks. A replica it does not know keeps them for good, see {@link #missedDeletions}; that is
   * expected if the datanode is down, not registered, dead to SCM, or still rebuilding the replica, but not otherwise.
   */
  private void checkPurgedTransactions() {
    if (!scm.getScmContext().isLeaderReady()) {
      // Until then the leader is still applying, and flushing, the transactions of earlier terms.
      return;
    }
    Set<Long> current = new HashSet<>();
    try (Table.KeyValueIterator<Long, DeletedBlocksTransaction> iter = ((DeletedBlockLogImpl) scm.getScmBlockManager()
        .getDeletedBlockLog()).getDeletedBlockLogStateManager().getReadOnlyIterator()) {
      while (iter.hasNext()) {
        Table.KeyValue<Long, DeletedBlocksTransaction> tx = iter.next();
        current.add(tx.getKey());
        deleteTransactions.putIfAbsent(tx.getKey(), tx.getValue());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    for (Iterator<DeletedBlocksTransaction> it = deleteTransactions.values().iterator(); it.hasNext();) {
      DeletedBlocksTransaction tx = it.next();
      if (!current.contains(tx.getTxID())) {
        it.remove();
        checkPurged(tx);
        datanodes.values().forEach(dn -> deleteAcks.remove(tx.getTxID() + "@" + dn.getName()));
      }
    }
  }

  private void checkPurged(DeletedBlocksTransaction tx) {
    long id = tx.getContainerID();
    Set<DatanodeID> known = new HashSet<>();
    try {
      ContainerInfo container = scm.getContainerManager().getContainer(ContainerID.valueOf(id));
      if (container.getState() == LifeCycleState.DELETING || container.getState() == LifeCycleState.DELETED) {
        // SCM deletes the replicas that are left, also ones that come back later (HDDS-7126).
        return;
      }
      scm.getContainerManager().getContainerReplicas(ContainerID.valueOf(id))
          .forEach(r -> known.add(r.getDatanodeDetails().getID()));
    } catch (ContainerNotFoundException e) {
      return;
    }
    for (SimDatanode dn : datanodes.values()) {
      SimReplica replica = dn.getReplica(id);
      if (replica == null || tx.getLocalIDList().stream().noneMatch(replica.getBlockIds()::contains)) {
        continue;
      }
      SimReplica acked = deleteAcks.get(tx.getTxID() + "@" + dn.getName());
      if (known.contains(dn.getId()) && !replica.isRecovering()) {
        if (acked == null) {
          violation("SCM purged delete transaction " + tx.getTxID() + " of #" + id + " while its blocks are still on "
              + dn.getName() + ", a replica it knows, which never applied it");
        } else {
          // The datanode applied it to an earlier replica, and now holds a copy made before the deletion. SCM keeps
          // the datanodes that applied a transaction, not the replicas, so it counts the copy as done.
          count("purge.ackFromEarlierReplica");
          record("purge", "tx" + tx.getTxID() + " of #" + id + ": " + dn.getName() + " applied it to an earlier "
              + "replica, " + replica + " still holds its blocks");
          missedDeletions.putIfAbsent(replicaKey(id, dn), "ackFromEarlierReplica");
        }
        continue;
      }
      NodeStatus status = nodeStatus(dn);
      String cause = replica.isRecovering() ? "ecReconstruction"
          : !dn.isRunning() || dn.isPartitioned() ? "datanodeDown"
          : status == null ? "notRegistered"
          : status.isDead() ? "deadInScm"
          : forgottenReplicas.getOrDefault(leader, Collections.emptySet()).contains(replicaKey(id, dn))
          ? "droppedByInstall" : null;
      if (cause != null) {
        missedDeletions.putIfAbsent(replicaKey(id, dn), cause);
      } else {
        record("purge", "tx" + tx.getTxID() + " of #" + id + " leaves its blocks on " + dn.getName() + ", "
            + status + ", which SCM does not list as a replica");
      }
    }
  }

  /**
   * Called when a heartbeat with a full report fails, after building the report dropped incremental reports. The full
   * report is not sent again until the next one is due (HDDS-17064), so that SCM does not learn about these changes;
   * with SCM HA, also if it becomes the leader meanwhile.
   */
  void reportsLost(SimDatanode dn, List<ContainerReplicaProto> icrs) {
    icrs.forEach(icr -> missedDeletions.putIfAbsent(replicaKey(icr.getContainerID(), dn), "fullReportLost"));
  }

  /** Called when a datanode applies a delete transaction to its replica and acknowledges it. */
  void recordDeleteAck(SimDatanode dn, long txId, SimReplica replica) {
    deleteAcks.put(txId + "@" + dn.getName(), replica);
  }

  /**
   * Called when a Ratis follower creates or updates a replica from log entries it missed. If the client deleted some of
   * the blocks meanwhile, SCM may have purged those deletions before it knew about this replica.
   */
  void replicaCaughtUp(SimDatanode dn, SimReplica replica) {
    Set<Long> deleted = deletedBlocks.getOrDefault(replica.getContainerId(), Collections.emptySet());
    if (replica.getBlockIds().stream().anyMatch(deleted::contains)) {
      missedDeletions.putIfAbsent(replicaKey(replica.getContainerId(), dn), "ratisCatchUp");
    }
  }

  private static String replicaKey(long containerId, SimDatanode dn) {
    return containerId + "@" + dn.getName();
  }

  /** Called when an SCM installed a snapshot: it knows no replica until their datanodes report them again. */
  void snapshotInstalled(SimScm owner) {
    Set<String> forgotten = forgottenReplicas.computeIfAbsent(owner, k -> new HashSet<>());
    for (SimDatanode dn : datanodes.values()) {
      dn.getReplicas().keySet().forEach(id -> forgotten.add(replicaKey(id, dn)));
    }
  }

  // ---- datanode boundary, through the datanode protocol server of each SCM ----

  boolean register(SimDatanode dn, SimScm target, ExtendedDatanodeDetailsProto details, NodeReportProto nodeReport,
      ContainerReportsProto containerReport, PipelineReportsProto pipelineReports, DatanodeVersionProto version) {
    NodeStatus status = nodeStatus(target.getNodeManager(), dn);
    SCMRegisteredResponseProto response;
    try {
      response = target.register(details, nodeReport, containerReport, pipelineReports, version);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (response.getErrorCode() != SCMRegisteredResponseProto.ErrorCode.success) {
      count("datanode.register.failed");
      return false;
    }
    record("register", dn.getName() + (scms.size() > 1 ? " with " + target.getName() : "") + " was " + status
        + " with " + dn.getReplicas().size() + " replicas");
    contacted(target, dn);
    lastRegistration.put(target.getName() + "@" + dn.getName(), status != null && status.isMaintenance());
    return true;
  }

  private void contacted(SimScm target, SimDatanode dn) {
    String contact = target.getName() + "@" + dn.getName();
    Long previous = lastContact.put(contact, now());
    if (previous == null || now() - previous >= config.getDeadNodeIntervalMs()) {
      lastReturn.put(contact, now());
    }
  }

  SCMHeartbeatResponseProto heartbeat(SimDatanode dn, SimScm target, SCMHeartbeatRequestProto request) {
    SCMHeartbeatResponseProto response;
    try {
      response = target.sendHeartbeat(request);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (TimeoutException e) {
      throw new IllegalStateException(e);
    }
    contacted(target, dn);
    lastRegistration.remove(target.getName() + "@" + dn.getName());
    for (SCMCommandProto command : response.getCommandsList()) {
      record("command", dn.getName() + " " + command.getCommandType()
          + (scms.size() > 1 ? " from " + target.getName() : ""));
      if (replicaMoves != null) {
        recordReplicaMove(dn, command);
      }
    }
    return response;
  }

  private void recordReplicaMove(SimDatanode dn, SCMCommandProto command) {
    if (command.getCommandType() == SCMCommandProto.Type.replicateContainerCommand) {
      replicaMoves.add("replicate #" + command.getReplicateContainerCommandProto().getContainerID() + " from "
          + dn.getName());
    } else if (command.getCommandType() == SCMCommandProto.Type.deleteContainerCommand) {
      replicaMoves.add("delete #" + command.getDeleteContainerCommandProto().getContainerID() + " on " + dn.getName());
    } else if (command.getCommandType() == SCMCommandProto.Type.reconstructECContainersCommand) {
      replicaMoves.add("reconstruct #" + command.getReconstructECContainersCommandProto().getContainerID() + " on "
          + dn.getName());
    }
  }

  // ---- datanodes and Ratis groups ----

  SimDatanode addDatanode() {
    int index = datanodes.size() + 1;
    Random ids = random(SimRandom.IDS);
    DatanodeID id = DatanodeID.of(new UUID(ids.nextLong(), ids.nextLong()));
    int rack = (index - 1) % config.getRacks();
    String name = "dn" + index;
    DatanodeDetails details = MockDatanodeDetails.createDatanodeDetails(id, name, "10.0." + rack + "." + index,
        "/rack-" + rack);
    SimDatanode dn = new SimDatanode(this, name, details);
    datanodes.put(id, dn);
    record("datanode.add", name + " /rack-" + rack);
    return dn;
  }

  SimDatanode datanode(DatanodeID id) {
    return datanodes.get(id);
  }

  Collection<SimDatanode> datanodes() {
    return datanodes.values();
  }

  SimRatisGroup ratisGroup(PipelineID id, List<DatanodeID> members, List<Integer> priorities) {
    return ratisGroups.computeIfAbsent(id, k -> new SimRatisGroup(this, id, members, priorities));
  }

  SimRatisGroup ratisGroup(PipelineID id) {
    return ratisGroups.get(id);
  }

  // ---- client and admin operations ----

  /** Whether SCM serves clients: a leader is ready, and out of safe mode. */
  boolean isOutOfSafeMode() {
    return hasReadyLeader() && !scm.getScmContext().isInSafeMode();
  }

  /**
   * Picks the container for a client write of the given size the way block allocation does: on a random open
   * RATIS/THREE pipeline, a container of this client with enough room, allocated if there is none.
   */
  ContainerInfo containerForWrite(Random random, long size) {
    List<Pipeline> open = scm.getPipelineManager().getPipelines(RATIS_THREE, Pipeline.PipelineState.OPEN);
    if (open.isEmpty()) {
      count("client.allocate.noPipeline");
      return null;
    }
    Pipeline pipeline = open.get(random.nextInt(open.size()));
    ContainerInfo container = scm.getContainerManager().getMatchingContainer(size, OWNER, pipeline);
    if (container == null) {
      count("client.allocate.failed");
    }
    return container;
  }

  /** Allocates an EC block through SCM's block manager, the way OM does for a client; null if SCM cannot. */
  AllocatedBlock allocateEcBlock(long size, ExcludeList excludeList) {
    try {
      AllocatedBlock block = scm.getScmBlockManager().allocateBlock(size, config.getEcReplicationConfig(), OWNER,
          excludeList);
      if (block == null) {
        count("client.allocateEc.failed");
      }
      return block;
    } catch (IOException | TimeoutException e) {
      count("client.allocateEc.failed");
      return null;
    }
  }

  /** Records blocks the client wrote: they must stay readable until the client deletes them. */
  void recordWrite(long containerId, List<Long> localIds, long bytesPerReplica) {
    NavigableMap<Long, Long> blocks = liveBlocks.computeIfAbsent(containerId, k -> new TreeMap<>());
    localIds.forEach(id -> blocks.put(id, bytesPerReplica));
  }

  /** Containers with blocks the client has not deleted, in ID order. */
  List<Long> containersWithLiveBlocks() {
    List<Long> result = new ArrayList<>();
    liveBlocks.forEach((id, blocks) -> {
      if (!blocks.isEmpty()) {
        result.add(id);
      }
    });
    return result;
  }

  List<Long> liveBlocks(long containerId) {
    return new ArrayList<>(liveBlocks.getOrDefault(containerId, Collections.emptyNavigableMap()).keySet());
  }

  /** Deletes blocks of a deleted key through SCM's block manager, the way OM's key deleting service does. */
  void deleteBlocks(long containerId, List<Long> localIds) {
    NavigableMap<Long, Long> live = liveBlocks.get(containerId);
    ReplicationConfig replication;
    try {
      replication = scm.getContainerManager().getContainer(ContainerID.valueOf(containerId)).getReplicationConfig();
    } catch (ContainerNotFoundException e) {
      throw new IllegalStateException("The client wrote to #" + containerId + ", which SCM does not know", e);
    }
    int dataNodes = replication instanceof ECReplicationConfig ? ((ECReplicationConfig) replication).getData() : 1;
    List<DeletedBlock> blocks = new ArrayList<>();
    for (Long id : localIds) {
      long perReplica = live.get(id);
      blocks.add(new DeletedBlock(new BlockID(containerId, id), perReplica * dataNodes,
          perReplica * replication.getRequiredNodes(), perReplica));
    }
    try {
      scm.getScmBlockManager().deleteBlocks(Collections.singletonList(BlockGroup.newBuilder()
          .setKeyName("key-" + containerId + "-" + localIds.get(0))
          .addAllDeletedBlocks(blocks)
          .build()));
    } catch (IOException e) {
      count("client.delete.failed");
      return;
    }
    localIds.forEach(live::remove);
    deletedBlocks.computeIfAbsent(containerId, k -> new HashSet<>()).addAll(localIds);
    count("client.delete.ok");
    record("client.delete", "#" + containerId + " " + localIds);
  }

  /** The datanode's status in the leader SCM. */
  NodeStatus nodeStatus(SimDatanode dn) {
    return nodeStatus(nodeManager, dn);
  }

  private static NodeStatus nodeStatus(SCMNodeManager manager, SimDatanode dn) {
    try {
      return manager.getNodeStatus(dn.getDetails());
    } catch (NodeNotFoundException e) {
      return null;
    }
  }

  boolean startDecommission(SimDatanode dn) {
    return admin("decommission", dn, node -> scm.getScmDecommissionManager().startDecommission(node));
  }

  boolean startMaintenance(SimDatanode dn) {
    return admin("maintenance", dn, node -> scm.getScmDecommissionManager().startMaintenance(node, 0));
  }

  /** @return false if SCM rejected it, for example because the node has not registered since SCM restarted. */
  boolean recommission(SimDatanode dn) {
    return admin("recommission", dn, node -> scm.getScmDecommissionManager().recommission(node));
  }

  private boolean admin(String operation, SimDatanode dn, AdminCommand command) {
    if (!hasReadyLeader()) {
      // The admin client fails over between the SCMs, and gives up while none leads.
      count("admin." + operation + ".noLeader");
      return false;
    }
    DatanodeDetails node = nodeManager.getNode(dn.getId());
    if (node == null) {
      // SCM does not know the node until it registers, for example after SCM restarted.
      count("admin." + operation + ".unregistered");
      return false;
    }
    try {
      command.run(node);
      record("admin." + operation, dn.getName());
      return true;
    } catch (NodeNotFoundException | InvalidNodeStateException e) {
      count("admin." + operation + ".rejected");
      return false;
    }
  }

  private boolean isEc(ContainerID id) {
    try {
      return scm.getContainerManager().getContainer(id).getReplicationConfig() instanceof ECReplicationConfig;
    } catch (ContainerNotFoundException e) {
      return false;
    }
  }

  /**
   * Placement can fix the mis-replication of a container only if as many racks as its placement policy requires have a
   * healthy in-service datanode. The policy counts every rack in the network topology, also racks whose datanodes are
   * all out of service but still running.
   */
  private boolean misReplicationFixable(ContainerID id) {
    ReplicationConfig replication;
    try {
      replication = scm.getContainerManager().getContainer(id).getReplicationConfig();
    } catch (ContainerNotFoundException e) {
      return false;
    }
    NetworkTopology topology = nodeManager.getClusterNetworkTopologyMap();
    int racks = topology.getNumOfNodes(topology.getMaxLevel() - 1);
    if (!(replication instanceof ECReplicationConfig)) {
      // Rack aware placement wants two racks.
      return usableRacks().size() >= Math.min(racks, 2);
    }
    // Rack scatter wants the replicas spread over as many racks as there are, at most ceil(n / racks) on each.
    int n = replication.getRequiredNodes();
    Map<String, Integer> perRack = inServicePerRack(Collections.emptyList());
    int required = Math.min(racks, n);
    return perRack.size() >= required && placeable(perRack, n, required) >= n;
  }

  /**
   * Whether the in-service datanodes, except the given ones, can hold an EC block group, also those down for now: rack
   * scatter placement puts at most ceil(n / racks) replicas of n on a rack, counting only racks with an in-service
   * datanode. SCM accepts a decommission if enough datanodes are left, whatever their racks, and puts a decommissioning
   * datanode back in service when it goes dead, so a decommission can be left without a place for the replicas.
   */
  boolean ecPlacementPossibleWithout(Collection<SimDatanode> leaving) {
    if (config.getEcWriteFraction() <= 0) {
      return true;
    }
    Map<String, Integer> perRack = inServicePerRack(leaving);
    int required = config.getEcReplicationConfig().getRequiredNodes();
    return placeable(perRack, required, Math.min(perRack.size(), required)) >= required;
  }

  /** In-service datanodes, also those down for now, per rack. */
  private Map<String, Integer> inServicePerRack(Collection<SimDatanode> leaving) {
    Map<String, Integer> perRack = new TreeMap<>();
    for (SimDatanode dn : datanodes.values()) {
      NodeStatus status = nodeStatus(dn);
      if (!leaving.contains(dn) && status != null && status.getOperationalState() == NodeOperationalState.IN_SERVICE) {
        perRack.merge(dn.getRack(), 1, Integer::sum);
      }
    }
    return perRack;
  }

  /** How many of n replicas fit on the racks if each may hold ceil(n / requiredRacks). */
  private static int placeable(Map<String, Integer> perRack, int n, int requiredRacks) {
    if (requiredRacks == 0) {
      return 0;
    }
    int maxPerRack = (n + requiredRacks - 1) / requiredRacks;
    int result = 0;
    for (int count : perRack.values()) {
      result += Math.min(count, maxPerRack);
    }
    return result;
  }

  /** Racks with a healthy in-service datanode, where placement can put new replicas. */
  Set<String> usableRacks() {
    Set<String> racks = new HashSet<>();
    for (SimDatanode dn : datanodes.values()) {
      NodeStatus status = nodeStatus(dn);
      if (status != null && status.isHealthy() && status.isInService()) {
        racks.add(dn.getRack());
      }
    }
    return racks;
  }

  // ---- invariants ----

  /**
   * Safety invariants over SCM state. Checked only when no node lifecycle handler work is pending, since those handlers
   * repair the state after each node state transition.
   */
  private void checkSafety() {
    if (scheduler.hasPendingHandlerWork(ScmSimulation::isNodeLifecycleLane)) {
      count("check.skippedBusy");
      return;
    }
    if (!hasReadyLeader()) {
      count("check.skippedNoLeader");
      return;
    }
    count("check.safety");
    checkTopology("");
    for (Pipeline pipeline : scm.getPipelineManager().getPipelines()) {
      if (pipeline.getPipelineState() != Pipeline.PipelineState.OPEN) {
        continue;
      }
      for (DatanodeDetails node : pipeline.getNodes()) {
        // After an SCM restart, pipelines are loaded before their datanodes register again.
        NodeStatus status = nodeStatus(datanodes.get(node.getID()));
        if (status != null && (status.getHealth() == NodeState.STALE || status.getHealth() == NodeState.DEAD)) {
          violation("OPEN pipeline " + describe(pipeline.getId()) + " has " + name(node) + " in "
              + status.getHealth() + " state");
        }
      }
    }
    for (ContainerInfo container : scm.getContainerManager().getContainers(LifeCycleState.OPEN)) {
      try {
        if (!scm.getPipelineManager().getPipeline(container.getPipelineID()).isOpen()) {
          violation("OPEN container #" + container.getContainerID() + " is on non-open pipeline "
              + describe(container.getPipelineID()));
        }
      } catch (PipelineNotFoundException e) {
        violation("OPEN container #" + container.getContainerID() + " has no pipeline");
      }
    }
  }

  /** Every registered node that is not dead must be in the network topology, or placement cannot use it. */
  private void checkTopology(String when) {
    NetworkTopology topology = nodeManager.getClusterNetworkTopologyMap();
    for (DatanodeDetails node : nodeManager.getAllNodes()) {
      NodeStatus status = nodeStatus(datanodes.get(node.getID()));
      if (status != null && status.getHealth() != NodeState.DEAD && !topology.contains(node)) {
        violation(when + name(node) + " is " + status.getHealth() + " but missing from the network topology");
      }
    }
  }

  /** Called by a datanode that is about to delete a replica on SCM's command. */
  void checkDelete(SimDatanode dn, SimReplica replica) {
    count("datanode.delete");
    if (!hasLiveBlocks(replica)) {
      return;
    }
    if (replica.isEc()) {
      checkEcDelete(dn, replica);
      return;
    }
    boolean otherCopy = false;
    boolean otherHealthy = false;
    long bestOtherBcsid = -1;
    for (SimDatanode other : datanodes.values()) {
      SimReplica r = other == dn ? null : other.getReplica(replica.getContainerId());
      if (r == null || !hasLiveBlocks(r)) {
        continue;
      }
      otherCopy = true;
      if (r.getState() != State.UNHEALTHY) {
        otherHealthy = true;
        bestOtherBcsid = Math.max(bestOtherBcsid, r.getBcsid());
      }
    }
    String what = replica + " on " + dn.getName();
    if (!otherCopy) {
      violation("SCM deleted the last replica " + what);
    } else if (replica.getState() != State.UNHEALTHY && !otherHealthy) {
      violation("SCM deleted the last healthy replica " + what);
    } else if (replica.getState() != State.UNHEALTHY && replica.getBcsid() > bestOtherBcsid) {
      violation("SCM deleted " + what + ", newer than any other healthy replica (BCSID " + bestOtherBcsid + ")");
    }
  }

  /**
   * An EC replica may go if another healthy copy of its index exists, or if it is unhealthy and the container keeps at
   * least as many healthy indexes as it has data indexes.
   */
  private void checkEcDelete(SimDatanode dn, SimReplica replica) {
    Set<Integer> healthyIndexes = new HashSet<>();
    boolean sameIndexElsewhere = false;
    for (SimDatanode other : datanodes.values()) {
      SimReplica r = other == dn ? null : other.getReplica(replica.getContainerId());
      if (r == null || r.isRecovering() || r.getState() != State.CLOSED) {
        continue;
      }
      healthyIndexes.add(r.getReplicaIndex());
      sameIndexElsewhere |= r.getReplicaIndex() == replica.getReplicaIndex();
    }
    if (sameIndexElsewhere) {
      return;
    }
    String what = replica + " on " + dn.getName();
    if (replica.getState() == State.CLOSED) {
      violation("SCM deleted the last healthy copy of index " + replica.getReplicaIndex() + ": " + what);
    } else if (healthyIndexes.size() < config.getEcReplicationConfig().getData()) {
      violation("SCM deleted " + what + ", leaving only indexes " + healthyIndexes + " healthy");
    }
  }

  /** Called when a datanode applies a block deletion transaction: it may remove only blocks the client deleted. */
  void checkBlockDelete(SimDatanode dn, SimReplica replica, List<Long> removed) {
    Set<Long> deleted = deletedBlocks.getOrDefault(replica.getContainerId(), Collections.emptySet());
    for (Long id : removed) {
      count("datanode.deleteBlock");
      if (!deleted.contains(id)) {
        violation(dn.getName() + " deleted block " + id + " of " + replica + ", which the client did not delete");
        return;
      }
    }
  }

  /** Whether the copy holds every block of the replica that the client has not deleted. */
  boolean holdsLiveBlocksOf(SimReplica copy, SimReplica replica) {
    NavigableMap<Long, Long> live = liveBlocks.getOrDefault(replica.getContainerId(), Collections.emptyNavigableMap());
    return replica.getBlockIds().stream().filter(live::containsKey).allMatch(copy.getBlockIds()::contains);
  }

  /** Whether the client wrote blocks to the container that it has not deleted. */
  private boolean holdsLiveBlocks(ContainerID id) {
    NavigableMap<Long, Long> live = liveBlocks.get(id.getId());
    return live != null && !live.isEmpty();
  }

  /** Whether the replica holds a block the client wrote and has not deleted. */
  private boolean hasLiveBlocks(SimReplica replica) {
    NavigableMap<Long, Long> live = liveBlocks.get(replica.getContainerId());
    return live != null && replica.getBlockIds().stream().anyMatch(live::containsKey);
  }

  /**
   * Called when placement of container replicas fails. Replication Manager tries again later, so a failure while enough
   * datanodes were eligible delays repairs rather than preventing them; it is counted, not a violation.
   */
  void placementFailed(List<DatanodeDetails> eligible, int required, IOException e) {
    if (eligible.size() < required) {
      count("placement.failed.noEligibleNodes");
      return;
    }
    count("placement.failed.withEligibleNodes");
    StringBuilder nodes = new StringBuilder();
    eligible.forEach(node -> nodes.append(' ').append(name(node)));
    record("placement.failed", "needed " + required + " of" + nodes + ": " + e.getMessage());
  }

  /** Called by the coordinator of an EC reconstruction once the rebuilt replicas are closed on the targets. */
  void recordReconstruction(SimDatanode coordinator, long containerId, Collection<SimDatanode> targets) {
    count("datanode.reconstruct");
    record("reconstructed", "#" + containerId + " by " + coordinator.getName() + " -> " + targets);
    // The rebuilt replicas get the blocks of the sources, which may have missed deletions.
    String cause = null;
    for (SimDatanode dn : datanodes.values()) {
      cause = cause != null ? cause : missedDeletions.get(replicaKey(containerId, dn));
    }
    if (cause != null) {
      for (SimDatanode target : targets) {
        missedDeletions.putIfAbsent(replicaKey(containerId, target), cause);
      }
    }
  }

  /** Called by a datanode that imports a replica pushed by another datanode. */
  void checkImport(SimDatanode target, SimReplica copy, SimDatanode source) {
    count("datanode.import");
    record("replicated", copy + " " + source.getName() + " -> " + target.getName());
    ContainerID id = ContainerID.valueOf(copy.getContainerId());
    String targetKey = replicaKey(copy.getContainerId(), target);
    String sourceCause = missedDeletions.get(replicaKey(copy.getContainerId(), source));
    if (sourceCause != null) {
      missedDeletions.put(targetKey, sourceCause);
    }
    boolean addPending = scm.getReplicationManager().getContainerReplicaPendingOps().getPendingOps(id).stream()
        .anyMatch(op -> op.getOpType() == ContainerReplicaOp.PendingOpType.ADD
            && op.getTarget().getID().equals(target.getId()));
    if (!addPending) {
      // SCM may have purged deletions the copy missed (HDDS-17152).
      missedDeletions.putIfAbsent(targetKey, "copyAfterAddExpired");
    }
  }

  /** Liveness: after faults stop and the cluster settles, SCM must have repaired everything. */
  private void checkConvergence() {
    int violationsBefore = violations.size();
    try {
      checkConvergenceOnce();
    } finally {
      if (violations.size() > violationsBefore) {
        describeCluster().forEach(line -> record("diagnose", line));
      }
    }
  }

  private void checkConvergenceOnce() {
    scheduler.drainHandlers(1_000_000);
    collectHandlerFailures();
    checkTopology("after settling, ");
    ReplicationManager rm = scm.getReplicationManager();
    rm.processAll();
    ReplicationManagerReport report = rm.getContainerReport();
    boolean ecPlaceable = ecPlacementPossibleWithout(Collections.emptyList());
    if (!ecPlaceable) {
      count("final.ecPlacementImpossible");
    }
    boolean unhealthy = false;
    for (ContainerHealthState state : UNREPAIRED_STATES) {
      List<ContainerID> sample = report.getSample(state);
      long count = report.getStat(state);
      if (state == ContainerHealthState.MIS_REPLICATED) {
        sample = sample.stream().filter(this::misReplicationFixable).collect(Collectors.toList());
        count = sample.size();
      } else if (state == ContainerHealthState.UNDER_REPLICATED && !ecPlaceable) {
        sample = sample.stream().filter(id -> !isEc(id)).collect(Collectors.toList());
        count = sample.size();
      } else if (WITHOUT_HEALTHY_COPY.contains(state)) {
        // No healthy replica is left to repair from. Fine for a container whose blocks the client all deleted: a
        // replica that missed the deletions can bring it back after SCM deleted it (HDDS-12421).
        sample = sample.stream().filter(this::holdsLiveBlocks).collect(Collectors.toList());
        count = sample.size();
      }
      if (count > 0) {
        violation("after settling, " + count + " container(s) are " + state + ": " + sample);
        unhealthy = true;
        for (ContainerID id : sample.subList(0, Math.min(3, sample.size()))) {
          record("diagnose", describeContainer(id));
        }
      }
    }
    for (ContainerHealthState state : ContainerHealthState.values()) {
      if (report.getStat(state) > 0) {
        counters.put("final.health." + state, report.getStat(state));
      }
    }
    for (ContainerInfo container : scm.getContainerManager().getContainers()) {
      counters.merge("final.container." + container.getState(), 1L, Long::sum);
      if (container.getState() == LifeCycleState.CLOSING) {
        violation("after settling, container #" + container.getContainerID() + " is still CLOSING");
      }
      checkReplicasMatch(container);
    }
    checkBlocks();
    checkScmsAgree();
    for (Pipeline pipeline : scm.getPipelineManager().getPipelines()) {
      if (pipeline.getPipelineState() == Pipeline.PipelineState.ALLOCATED) {
        violation("after settling, pipeline " + describe(pipeline.getId()) + " is still ALLOCATED");
      }
    }
    for (SimDatanode dn : datanodes.values()) {
      NodeStatus status = nodeStatus(dn);
      if (status == null) {
        violation("after settling, " + dn.getName() + " is not registered");
      } else if (status.getOperationalState() == NodeOperationalState.DECOMMISSIONING && !ecPlaceable) {
        count("final.decommissionWithoutPlace");
      } else if (status.getOperationalState() == NodeOperationalState.DECOMMISSIONING
          || status.getOperationalState() == NodeOperationalState.ENTERING_MAINTENANCE) {
        // After a snapshot install, NodeManager may still list pipelines deleted meanwhile, which never close
        // (HDDS-16905).
        long unknownPipelines = nodeManager.getPipelines(dn.getDetails()).stream()
            .filter(id -> !scm.getPipelineManager().containsPipeline(id)).count();
        if (unknownPipelines > 0 && config.hasWorkaround("HDDS-16905")) {
          count("final.adminWaitsForDeletedPipelines");
        } else {
          violation("after settling, " + dn.getName() + " is still " + status.getOperationalState()
              + (unknownPipelines == 0 ? "" : ", and NodeManager lists " + unknownPipelines
              + " pipeline(s) for it that PipelineManager does not have"));
        }
      } else if (status.getHealth() != NodeState.HEALTHY) {
        violation("after settling, " + dn.getName() + " is " + status.getHealth());
      }
    }
  }

  /**
   * After settling, every block the client did not delete is still readable, and every block it deleted is gone from
   * the containers SCM sends deletions for. SCM holds deletions back for open and unhealthy containers, and for
   * containers with a replica on a datanode that is not in service and healthy; those are only counted. So are
   * containers with a replica SCM did not know while it purged the deletions, see {@link #missedDeletions}.
   */
  private void checkBlocks() {
    List<String> lost = new ArrayList<>();
    List<String> notDeleted = new ArrayList<>();
    for (ContainerInfo container : scm.getContainerManager().getContainers()) {
      long id = container.getContainerID();
      Set<Long> deleted = deletedBlocks.getOrDefault(id, Collections.emptySet());
      // Indexes of the readable replicas holding each block; 0 for RATIS replicas.
      Map<Long, Set<Integer>> holders = new HashMap<>();
      // Replicas holding deleted blocks, and why SCM may not know they do.
      Map<SimDatanode, String> deletedLeft = new LinkedHashMap<>();
      for (SimDatanode dn : datanodes.values()) {
        SimReplica replica = dn.getReplica(id);
        if (replica == null || replica.isRecovering()) {
          continue;
        }
        for (Long block : replica.getBlockIds()) {
          if (replica.getState() != State.UNHEALTHY) {
            holders.computeIfAbsent(block, k -> new HashSet<>()).add(replica.getReplicaIndex());
          }
          if (deleted.contains(block)) {
            deletedLeft.put(dn, missedDeletions.get(replicaKey(id, dn)));
          }
        }
      }
      ReplicationConfig replication = container.getReplicationConfig();
      int needed = replication instanceof ECReplicationConfig ? ((ECReplicationConfig) replication).getData() : 1;
      for (Long block : liveBlocks.getOrDefault(id, Collections.emptyNavigableMap()).keySet()) {
        if (holders.getOrDefault(block, Collections.emptySet()).size() < needed) {
          lost.add("#" + id + "/" + block);
        }
      }
      if (!deletedLeft.isEmpty() && container.getState() != LifeCycleState.DELETING
          && container.getState() != LifeCycleState.DELETED) {
        String heldBack = deletionHeldBack(container);
        for (Map.Entry<SimDatanode, String> e : deletedLeft.entrySet()) {
          String cause = heldBack != null ? heldBack : e.getValue();
          if (cause == null) {
            notDeleted.add("#" + id + " on " + e.getKey().getName());
          } else {
            counters.merge("final.deletedBlocksLeft." + cause, 1L, Long::sum);
          }
        }
      }
    }
    if (!lost.isEmpty()) {
      violation("after settling, " + lost.size() + " block(s) the client did not delete are lost: "
          + lost.subList(0, Math.min(10, lost.size())));
    }
    if (!notDeleted.isEmpty()) {
      violation("after settling, " + notDeleted.size() + " replica(s) still hold blocks the client deleted: "
          + notDeleted.subList(0, Math.min(10, notDeleted.size())));
      notDeleted.subList(0, Math.min(3, notDeleted.size())).forEach(c -> record("diagnose",
          describeContainer(ContainerID.valueOf(Long.parseLong(c.substring(1).split(" ")[0])))));
    }
    try {
      counters.put("final.deleteTransactions", (long) scm.getScmBlockManager().getDeletedBlockLog()
          .getNumOfValidTransactions());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * With SCM HA, every SCM that is up and connected must have received the whole log by now, and every SCM that applied
   * the whole log must have the same replicated state as the leader: the containers and their states, the pipelines and
   * their states, and the delete transactions.
   */
  private void checkScmsAgree() {
    if (scms.size() == 1) {
      return;
    }
    Map<String, String> leaderState = replicatedState(leader);
    for (SimScm member : scms) {
      boolean behind = member.getReceivedIndex() < ratis.size()
          || member.getAppliedIndex() < ratis.size() && member.isApplyIdle();
      if (member != leader && member.isConnected() && (member.isInstalling() || behind)) {
        violation("after settling, " + member.getName() + " has received " + member.getReceivedIndex() + " and applied "
            + member.getAppliedIndex() + " of the " + ratis.size() + " log entries"
            + (member.isInstalling() ? ", and is installing a snapshot" : ""));
      }
      if (member == leader || !member.isRunning() || member.getAppliedIndex() < ratis.size()) {
        continue;
      }
      Map<String, String> state = replicatedState(member);
      List<String> differences = new ArrayList<>();
      Set<String> keys = new TreeSet<>(leaderState.keySet());
      keys.addAll(state.keySet());
      for (String key : keys) {
        String expected = leaderState.get(key);
        String actual = state.get(key);
        if (expected == null || !expected.equals(actual)) {
          differences.add(key + " " + actual + " (leader: " + expected + ")");
        }
      }
      if (!differences.isEmpty()) {
        violation("after settling, " + member.getName() + " has applied the whole log but differs from the leader "
            + leader.getName() + " in " + differences.size() + " place(s): "
            + differences.subList(0, Math.min(5, differences.size())));
      }
    }
  }

  private static Map<String, String> replicatedState(SimScm member) {
    Map<String, String> state = new TreeMap<>();
    StorageContainerManager target = member.getScm();
    for (ContainerInfo container : target.getContainerManager().getContainers()) {
      state.put("#" + container.getContainerID(), container.getState().toString());
    }
    for (Pipeline pipeline : target.getPipelineManager().getPipelines()) {
      state.put("pipeline " + pipeline.getId(), pipeline.getPipelineState().toString());
    }
    try {
      member.flush();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    state.put("deleteTransactions", String.valueOf(numOfDeleteTransactions(target)));
    return state;
  }

  /** Why SCM holds back block deletions for the container, like DeletedBlockLogImpl#getTransactions; null if not. */
  private String deletionHeldBack(ContainerInfo container) {
    if (container.isOpen()) {
      return "open";
    }
    Set<ContainerReplica> replicas;
    try {
      replicas = scm.getContainerManager().getContainerReplicas(container.containerID());
    } catch (ContainerNotFoundException e) {
      return "notFound";
    }
    for (ContainerReplica replica : replicas) {
      NodeStatus status;
      try {
        status = nodeManager.getNodeStatus(replica.getDatanodeDetails());
      } catch (NodeNotFoundException e) {
        return "replicaOnUnknownNode";
      }
      if (!status.isInService() || !status.isHealthy()) {
        return "replicaNotInService";
      }
    }
    ContainerHealthResult health = scm.getReplicationManager().getContainerReplicationHealth(container, replicas);
    return health.getHealthState() == ContainerHealthResult.HealthState.HEALTHY ? null : "unhealthy";
  }

  /**
   * After settling SCM has nothing left to repair, so it must stop moving replicas. A replicate or delete command in a
   * further quiet window means it keeps copying and deleting replicas, for example to fix mis-replication it cannot
   * fix.
   */
  private void checkQuiet() {
    replicaMoves = new ArrayList<>();
    runUntil(clock.millis() + QUIET_WINDOW_MS);
    if (!replicaMoves.isEmpty()) {
      violation("after settling, SCM still sent " + replicaMoves.size() + " replicate or delete commands in "
          + QUIET_WINDOW_MS / 60_000 + " minutes: " + replicaMoves.subList(0, Math.min(6, replicaMoves.size())));
    }
    replicaMoves = null;
  }

  /** One line per datanode: rack, SCM's view of it, and what the simulation knows. */
  List<String> describeCluster() {
    List<String> lines = new ArrayList<>();
    if (scms.size() > 1) {
      for (SimScm member : scms) {
        lines.add(member.getName() + " running=" + member.isRunning() + " connected=" + member.isConnected()
            + " leader=" + (member == ratis.getLeader()) + " term=" + ratis.getTerm()
            + (member.isRunning() ? " leaderReady=" + member.getScm().getScmContext().isLeaderReady()
                + " safeMode=" + member.getScm().getScmContext().isInSafeMode()
                + " rmRuns=" + member.getScm().getReplicationManager().shouldRun()
                + " blockDeletingRuns=" + member.getScm().getScmBlockManager().getSCMBlockDeletingService().shouldRun()
                + " deleteTransactions=" + numOfDeleteTransactions(member.getScm())
                + (member.getScm().getScmContext().isInSafeMode() ? " rules=" + member.getScm().getScmSafeModeManager()
                .getRuleStatus().stream().filter(r -> !r.getValidate())
                .map(r -> r.getRuleName() + "(" + r.getStatusText().replace('\n', ' ') + ")")
                .collect(Collectors.toList()) : "") : "")
            + " applied=" + member.getAppliedIndex() + "/" + ratis.size());
      }
    }
    NetworkTopology topology = nodeManager.getClusterNetworkTopologyMap();
    for (SimDatanode dn : datanodes.values()) {
      NodeStatus status = nodeStatus(dn);
      // NetworkTopology#contains follows parent links, so ask about SCM's own node object.
      DatanodeDetails scmNode = nodeManager.getNode(dn.getId());
      lines.add(dn.getName() + " " + dn.getRack() + " scm=" + status
          + " inTopology=" + (scmNode != null && topology.contains(scmNode)) + " running=" + dn.isRunning()
          + " partitioned=" + dn.isPartitioned() + " replicas=" + dn.getReplicas().size());
    }
    return lines;
  }

  private static int numOfDeleteTransactions(StorageContainerManager target) {
    try {
      return target.getScmBlockManager().getDeletedBlockLog().getNumOfValidTransactions();
    } catch (IOException e) {
      return -1;
    }
  }

  /** SCM's replicas and pending operations of a container, next to what the datanodes hold. */
  String describeContainer(ContainerID id) {
    StringBuilder sb = new StringBuilder("#").append(id.getId());
    ContainerManager containerManager = scm.getContainerManager();
    try {
      ContainerInfo info = containerManager.getContainer(id);
      sb.append(' ').append(info.getState()).append(" since t=")
          .append((info.getStateEnterTime().toEpochMilli() - START.toEpochMilli()) / 1000).append("s scm:");
      for (ContainerReplica replica : containerManager.getContainerReplicas(id)) {
        sb.append(' ').append(name(replica.getDatanodeDetails()))
            .append(replica.getReplicaIndex() > 0 ? "[" + replica.getReplicaIndex() + "]" : "")
            .append('=').append(replica.getState()).append('@').append(replica.getSequenceId());
      }
    } catch (ContainerNotFoundException e) {
      sb.append(" not found in SCM");
    }
    sb.append(" pending:").append(scm.getReplicationManager().getContainerReplicaPendingOps().getPendingOps(id).size())
        .append(" actual:");
    for (SimDatanode dn : datanodes.values()) {
      SimReplica replica = dn.getReplica(id.getId());
      if (replica != null) {
        Set<Long> deleted = deletedBlocks.getOrDefault(id.getId(), Collections.emptySet());
        sb.append(' ').append(dn.getName()).append(replica.isEc() ? "[" + replica.getReplicaIndex() + "]" : "")
            .append('=').append(replica.isRecovering() ? "RECOVERING" : replica.getState()).append('@')
            .append(replica.getBcsid()).append('/').append(replica.getKeys()).append('b')
            .append(replica.getBlockIds().stream().filter(deleted::contains).count()).append('d');
      }
    }
    return sb.toString();
  }

  /** SCM's replicas of a container must match what the datanodes hold. */
  private void checkReplicasMatch(ContainerInfo container) {
    ContainerID id = container.containerID();
    Set<DatanodeID> scmView = new HashSet<>();
    try {
      for (ContainerReplica replica : scm.getContainerManager().getContainerReplicas(id)) {
        scmView.add(replica.getDatanodeDetails().getID());
        SimReplica actual = datanodes.get(replica.getDatanodeDetails().getID()).getReplica(id.getId());
        if (actual == null) {
          violation("after settling, SCM lists a replica of #" + id.getId() + " on "
              + name(replica.getDatanodeDetails()) + " which the datanode does not have");
        } else if (actual.getReplicaIndex() != replica.getReplicaIndex()) {
          violation("after settling, SCM has replica #" + id.getId() + " on " + name(replica.getDatanodeDetails())
              + " as index " + replica.getReplicaIndex() + " but it is index " + actual.getReplicaIndex());
        } else if (actual.getState() != replica.getState()) {
          violation("after settling, SCM has replica #" + id.getId() + " on " + name(replica.getDatanodeDetails())
              + " as " + replica.getState() + " but it is " + actual.getState());
        }
      }
    } catch (ContainerNotFoundException e) {
      violation("container #" + id.getId() + " disappeared");
      return;
    }
    for (SimDatanode dn : datanodes.values()) {
      if (dn.getReplica(id.getId()) != null && !scmView.contains(dn.getId())) {
        violation("after settling, " + dn.getName() + " holds " + dn.getReplica(id.getId())
            + " which SCM does not know about");
      }
    }
  }

  private void collectHandlerFailures() {
    for (SimScm member : scms) {
      if (member.isRunning()) {
        for (String failure : member.getHandlerFailures()) {
          violation("handler failed" + (scms.size() > 1 ? " on " + member.getName() : "") + ": " + failure);
        }
      }
    }
    List<String> timerFailures = scheduler.getTimerFailures();
    while (timerFailuresSeen < timerFailures.size()) {
      violation("task failed: " + timerFailures.get(timerFailuresSeen++));
    }
  }

  /**
   * SCM leaves safe mode once 99% of the closed containers its rules know have enough reported replicas. The rules take
   * their containers from the DB when SCM starts, before it replays its log. A new SCM, which starts with an empty DB,
   * must still not leave safe mode before datanodes report most of the containers it has by then (HDDS-16896). A
   * restarted SCM whose DB is older than its log may also leave safe mode early; that is only counted. Checked over the
   * closed containers with a replica that holds blocks.
   */
  private void checkSafeModeExits() {
    for (SimScm member : scms) {
      if (!member.isRunning() || !member.leftSafeMode()) {
        continue;
      }
      Set<Long> withBlocks = new HashSet<>();
      for (SimDatanode dn : datanodes.values()) {
        dn.getReplicas().forEach((id, replica) -> {
          if (!replica.getBlockIds().isEmpty()) {
            withBlocks.add(id);
          }
        });
      }
      ContainerManager containers = member.getScm().getContainerManager();
      int closed = 0;
      int reported = 0;
      for (ContainerInfo container : containers.getContainers()) {
        LifeCycleState state = container.getState();
        if ((state == LifeCycleState.CLOSED || state == LifeCycleState.QUASI_CLOSED)
            && withBlocks.contains(container.getContainerID())) {
          closed++;
          try {
            if (containers.getContainerReplicas(container.containerID()).size()
                >= container.getReplicationConfig().getMinimumNodes()) {
              reported++;
            }
          } catch (ContainerNotFoundException e) {
            throw new IllegalStateException(e);
          }
        }
      }
      record("scm.leftSafeMode", member.getName() + " with " + reported + " of " + closed
          + " closed containers reported");
      if (reported < Math.ceil(closed * HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT)) {
        if (!member.startedEmpty()) {
          count("scm.leftSafeModeEarly.oldDb");
        } else if (config.hasWorkaround("HDDS-16896")) {
          count("scm.leftSafeModeEarly");
        } else {
          violation((scms.size() > 1 ? member.getName() : "SCM") + " left safe mode with " + reported + " of its "
              + closed + " closed containers with blocks reported");
        }
      }
    }
  }

  void violation(String message) {
    String line = "t=" + scheduler.elapsed() / 1000 + "s " + message;
    violations.add(line);
    trace.record(scheduler.elapsed(), "VIOLATION", message);
    LOG.error("Seed {}: {}", config.getSeed(), line);
  }

  // ---- utilities ----

  SimConfig config() {
    return config;
  }

  SimScheduler scheduler() {
    return scheduler;
  }

  Random random(SimRandom stream) {
    return randoms.get(stream);
  }

  long now() {
    return clock.millis();
  }

  void count(String name) {
    counters.merge(name, 1L, Long::sum);
  }

  void record(String kind, String detail) {
    trace.record(scheduler.elapsed(), kind, detail);
  }

  String describe(PipelineID id) {
    return pipelineNames.computeIfAbsent(id, k -> "p" + (pipelineNames.size() + 1));
  }

  private static boolean isNodeLifecycleLane(String lane) {
    for (String handler : NODE_LIFECYCLE_HANDLERS) {
      if (lane.endsWith("For" + handler)) {
        return true;
      }
    }
    return false;
  }

  private String name(DatanodeDetails node) {
    SimDatanode dn = datanodes.get(node.getID());
    return dn != null ? dn.getName() : node.getID().toString();
  }

  /** Short, deterministic description of an event payload for the trace. */
  String describe(Object payload) {
    if (payload instanceof DatanodeDetails) {
      return name((DatanodeDetails) payload);
    } else if (payload instanceof ContainerID) {
      return "#" + ((ContainerID) payload).getId();
    } else if (payload instanceof ReportFromDatanode) {
      return name(((ReportFromDatanode<?>) payload).getDatanodeDetails());
    } else if (payload instanceof CommandForDatanode) {
      CommandForDatanode<?> command = (CommandForDatanode<?>) payload;
      SimDatanode dn = datanodes.get(command.getDatanodeId());
      return command.getCommand().getType() + " " + (dn != null ? dn.getName() : command.getDatanodeId());
    }
    return payload.getClass().getSimpleName();
  }

  /** An admin operation of NodeDecommissionManager on a node. */
  @FunctionalInterface
  private interface AdminCommand {
    void run(DatanodeDetails node) throws NodeNotFoundException, InvalidNodeStateException;
  }
}
