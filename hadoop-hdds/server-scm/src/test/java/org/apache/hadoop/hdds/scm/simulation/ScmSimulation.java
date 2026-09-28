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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ExtendedDatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationSimSupport;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.BackgroundSCMService;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeDecommissionManager;
import org.apache.hadoop.hdds.scm.node.NodeSimSupport;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.BackgroundPipelineCreator;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineSimSupport;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
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
 * Not simulated: SCM HA (Ratis replication of SCM state), block deletion, EC containers, the container balancer and
 * security.
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
  private static final int REPORT_LANES = 10;
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
  private final List<SimScheduler.Timer> scmTimers = new ArrayList<>();
  private final List<SimScheduler.WakeUp> scmWakeUps = new ArrayList<>();
  private final String clusterId;
  private final String scmId;

  private StorageContainerManager scm;
  private SimEventQueue eventQueue;
  private SCMNodeManager nodeManager;
  private SimScheduler.WakeUp replicationWakeUp;
  private SimScheduler.WakeUp pipelineCreatorWakeUp;
  private long term = 1;
  private int handlerFailuresSeen;
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
    this.scmId = new UUID(ids.nextLong(), ids.nextLong()).toString();
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
    startScm();
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
    }
  }

  private SimResult result() throws IOException {
    Path traceFile = null;
    if (!violations.isEmpty() || config.isKeepFullTrace()) {
      traceFile = config.getTraceDir().resolve("trace-" + config.getSeed() + ".txt").toAbsolutePath();
      trace.writeTo(traceFile);
    }
    return new SimResult(config, new ArrayList<>(violations), new TreeMap<>(counters), trace.hash(),
        scheduler.steps(), scheduler.elapsed(), trace.recentLines(), traceFile);
  }

  @Override
  public void close() throws IOException {
    try {
      stopScm();
    } finally {
      RUNNING.remove();
    }
  }

  static ScmSimulation running() {
    return RUNNING.get();
  }

  // ---- SCM ----

  /**
   * Restarts SCM. Committed state survives in its DB (in production the Ratis log guarantees that); events being
   * handled, pending commands and all other in-memory state are lost, and datanodes register again.
   */
  void restartScm() throws IOException {
    record("scm.restart", "term " + (term + 1));
    scm.getScmHAManager().asSCMHADBTransactionBuffer().flush();
    stopScm();
    term++;
    handlerFailuresSeen = 0;
    startScm();
  }

  private void stopScm() throws IOException {
    scmTimers.forEach(scheduler::cancel);
    scmTimers.clear();
    scmWakeUps.forEach(SimScheduler.WakeUp::cancel);
    scmWakeUps.clear();
    scheduler.clearLanes();
    if (scm != null) {
      scm.stop();
      nodeManager.close();
      scm = null;
    }
  }

  private void startScm() throws IOException {
    OzoneConfiguration conf = config.toOzoneConfiguration(dir.resolve("scm"), random(SimRandom.SCM).nextLong());
    SCMStorageConfig storage = new SCMStorageConfig(conf);
    if (storage.getState() != Storage.StorageState.INITIALIZED) {
      storage.setClusterId(clusterId);
      storage.setScmId(scmId);
      storage.setSCMHAFlag(true);
      storage.initialize();
    }
    SCMMetadataStore metadataStore = new SCMMetadataStoreImpl(conf);
    SCMContext scmContext = new SCMContext.Builder()
        .setLeader(true)
        .setTerm(term)
        .setSafeModeStatus(SCMSafeModeManager.SafeModeStatus.INITIAL)
        .setSCM(scmProxy())
        .build();
    scmContext.setLeaderReady();
    eventQueue = new SimEventQueue(scheduler, this::describe, REPORT_LANES);
    NetworkTopology topology = new NetworkTopologyImpl(conf, random(SimRandom.SCM));
    // Built here to track heartbeats on the simulated clock, and to take rack locations from the datanodes.
    nodeManager = new SCMNodeManager(conf, storage, eventQueue, topology, scmContext,
        new HDDSLayoutVersionManager(storage.getLayoutVersion()), host -> null, clock);
    NodeSimSupport.stopHealthCheckThread(nodeManager);

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setMetadataStore(metadataStore);
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true, metadataStore.getStore()));
    configurator.setScmContext(scmContext);
    configurator.setNetworkTopology(topology);
    configurator.setScmNodeManager(nodeManager);
    configurator.setLeaseManager(new SimLeaseManager(scheduler, config.getCloseContainerWaitMs()));
    configurator.setEventQueue(eventQueue);
    configurator.setSystemClock(clock);
    Random ids = random(SimRandom.IDS);
    configurator.setPipelineIdGenerator(() -> PipelineID.valueOf(new UUID(ids.nextLong(), ids.nextLong())));
    configurator.setRandom(random(SimRandom.SCM));
    try {
      scm = StorageContainerManager.createSCM(conf, configurator);
    } catch (AuthenticationException e) {
      throw new IOException(e);
    }
    runScmServices();
  }

  /**
   * SCM runs its periodic services on their own threads. Stop those threads, and run each service's task on the
   * scheduler instead, at the same interval and behind the same checks, woken up by the same requests.
   */
  private void runScmServices() {
    scm.getSCMServiceManager().stop();
    NodeDecommissionManager decommissionManager = scm.getScmDecommissionManager();
    decommissionManager.stop();
    ReplicationManager rm = scm.getReplicationManager();
    PipelineManagerImpl pipelineManager = (PipelineManagerImpl) scm.getPipelineManager();
    BackgroundPipelineCreator creator = pipelineManager.getBackgroundPipelineCreator();
    // Like the pipeline scrubber, the expired replica op scrubber is a BackgroundSCMService, which waits as long.
    BackgroundSCMService scrubber = pipelineManager.getBackgroundPipelineScrubber();

    replicationWakeUp = newWakeUp("scm.replicationMonitor.wakeUp", rm::processAll);
    pipelineCreatorWakeUp = newWakeUp("scm.pipelineCreator.wakeUp",
        () -> PipelineSimSupport.runPipelineCreator(creator));
    scm.getSCMServiceManager().register(new PipelineCreatorTrigger());
    eventQueue.addHandler(SCMEvents.REPLICATION_MANAGER_NOTIFY, new ReplicationMonitorTrigger());

    every("scm.nodeHealthCheck", config.getHeartbeatProcessIntervalMs(),
        () -> NodeSimSupport.checkNodesHealth(nodeManager));
    every("scm.replicationMonitor", config.getReplicationIntervalMs(), rm::processAll);
    every("scm.underReplicatedProcessor", config.getUnderReplicatedIntervalMs(),
        () -> ReplicationSimSupport.processUnderReplicated(rm));
    every("scm.overReplicatedProcessor", config.getOverReplicatedIntervalMs(),
        () -> ReplicationSimSupport.processOverReplicated(rm));
    every("scm.pipelineCreator", config.getPipelineCreationIntervalMs(),
        () -> PipelineSimSupport.runPipelineCreator(creator));
    every("scm.pipelineScrubber", config.getPipelineScrubIntervalMs(), () -> {
      if (scrubber.shouldRun()) {
        pipelineManager.scrubAndClosePipelinesMissingDataStreamPort();
      }
    });
    every("scm.replicaOpScrubber", config.getReplicaOpScrubIntervalMs(), () -> {
      if (scrubber.shouldRun()) {
        rm.getContainerReplicaPendingOps().removeExpiredEntries();
      }
    });
    every("scm.datanodeAdminMonitor", config.getAdminMonitorIntervalMs(), decommissionManager.getMonitor()::run);
  }

  private void every(String name, long intervalMs, Runnable task) {
    scmTimers.add(scheduler.scheduleEvery(name, intervalMs, intervalMs, task));
  }

  private SimScheduler.WakeUp newWakeUp(String name, Runnable task) {
    SimScheduler.WakeUp wakeUp = scheduler.newWakeUp(name, task);
    scmWakeUps.add(wakeUp);
    return wakeUp;
  }

  /**
   * The SCM that managers reach through {@link SCMContext#getScm()}. The context has to exist before SCM, which is
   * looked up only when called.
   */
  private OzoneStorageContainerManager scmProxy() {
    return (OzoneStorageContainerManager) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {OzoneStorageContainerManager.class}, (proxy, method, args) -> {
          try {
            return method.invoke(scm, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  // ---- datanode boundary, through SCM's datanode protocol server ----

  boolean register(SimDatanode dn, ExtendedDatanodeDetailsProto details, NodeReportProto nodeReport,
      ContainerReportsProto containerReport, PipelineReportsProto pipelineReports, LayoutVersionProto layout) {
    SCMRegisteredResponseProto response;
    try {
      response = scm.getDatanodeProtocolServer().register(details, nodeReport, containerReport, pipelineReports,
          layout);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (response.getErrorCode() != SCMRegisteredResponseProto.ErrorCode.success) {
      count("datanode.register.failed");
      return false;
    }
    record("register", dn.getName());
    return true;
  }

  SCMHeartbeatResponseProto heartbeat(SimDatanode dn, SCMHeartbeatRequestProto request) {
    SCMHeartbeatResponseProto response;
    try {
      response = scm.getDatanodeProtocolServer().sendHeartbeat(request);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (TimeoutException e) {
      throw new IllegalStateException(e);
    }
    for (SCMCommandProto command : response.getCommandsList()) {
      record("command", dn.getName() + " " + command.getCommandType());
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

  boolean isOutOfSafeMode() {
    return !scm.getScmContext().isInSafeMode();
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

  NodeStatus nodeStatus(SimDatanode dn) {
    try {
      return nodeManager.getNodeStatus(dn.getDetails());
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
    try {
      command.run(nodeManager.getNode(dn.getId()));
      record("admin." + operation, dn.getName());
      return true;
    } catch (Exception e) {
      count("admin." + operation + ".rejected");
      return false;
    }
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
    if (!replica.hasData()) {
      return;
    }
    boolean otherCopy = false;
    boolean otherHealthy = false;
    long bestOtherBcsid = -1;
    for (SimDatanode other : datanodes.values()) {
      SimReplica r = other == dn ? null : other.getReplica(replica.getContainerId());
      if (r == null || !r.hasData()) {
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

  /** Called by a datanode that imports a replica pushed by another datanode. */
  void checkImport(SimDatanode target, SimReplica copy, SimDatanode source) {
    count("datanode.import");
    record("replicated", copy + " " + source.getName() + " -> " + target.getName());
  }

  /** Liveness: after faults stop and the cluster settles, SCM must have repaired everything. */
  private void checkConvergence() {
    scheduler.drainHandlers(1_000_000);
    checkTopology("after settling, ");
    ReplicationManager rm = scm.getReplicationManager();
    rm.processAll();
    ReplicationManagerReport report = rm.getContainerReport();
    // Rack aware placement fixes mis-replication only when there is another rack to place replicas on.
    boolean misReplicationFixable = usableRacks().size() > 1;
    boolean unhealthy = false;
    for (ContainerHealthState state : new ContainerHealthState[] {ContainerHealthState.UNDER_REPLICATED,
        ContainerHealthState.OVER_REPLICATED, ContainerHealthState.MIS_REPLICATED, ContainerHealthState.MISSING}) {
      long count = report.getStat(state);
      if (count > 0 && (state != ContainerHealthState.MIS_REPLICATED || misReplicationFixable)) {
        violation("after settling, " + count + " container(s) are " + state + ": " + report.getSample(state));
        unhealthy = true;
        for (ContainerID id : report.getSample(state).subList(0, Math.min(3, report.getSample(state).size()))) {
          record("diagnose", describeContainer(id));
        }
      }
    }
    if (unhealthy) {
      describeCluster().forEach(line -> record("diagnose", line));
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
    for (Pipeline pipeline : scm.getPipelineManager().getPipelines()) {
      if (pipeline.getPipelineState() == Pipeline.PipelineState.ALLOCATED) {
        violation("after settling, pipeline " + describe(pipeline.getId()) + " is still ALLOCATED");
      }
    }
    for (SimDatanode dn : datanodes.values()) {
      NodeStatus status = nodeStatus(dn);
      if (status == null) {
        violation("after settling, " + dn.getName() + " is not registered");
      } else if (status.getOperationalState() == NodeOperationalState.DECOMMISSIONING
          || status.getOperationalState() == NodeOperationalState.ENTERING_MAINTENANCE) {
        violation("after settling, " + dn.getName() + " is still " + status.getOperationalState());
      } else if (status.getHealth() != NodeState.HEALTHY) {
        violation("after settling, " + dn.getName() + " is " + status.getHealth());
      }
    }
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

  /** SCM's replicas and pending operations of a container, next to what the datanodes hold. */
  String describeContainer(ContainerID id) {
    StringBuilder sb = new StringBuilder("#").append(id.getId());
    ContainerManager containerManager = scm.getContainerManager();
    try {
      sb.append(' ').append(containerManager.getContainer(id).getState()).append(" scm:");
      for (ContainerReplica replica : containerManager.getContainerReplicas(id)) {
        sb.append(' ').append(name(replica.getDatanodeDetails())).append('=').append(replica.getState())
            .append('@').append(replica.getSequenceId());
      }
    } catch (ContainerNotFoundException e) {
      sb.append(" not found in SCM");
    }
    sb.append(" pending:").append(scm.getReplicationManager().getContainerReplicaPendingOps().getPendingOps(id).size())
        .append(" actual:");
    for (SimDatanode dn : datanodes.values()) {
      SimReplica replica = dn.getReplica(id.getId());
      if (replica != null) {
        sb.append(' ').append(dn.getName()).append('=').append(replica.getState()).append('@')
            .append(replica.getBcsid());
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
    List<String> failures = eventQueue.getHandlerFailures();
    while (handlerFailuresSeen < failures.size()) {
      violation("handler failed: " + failures.get(handlerFailuresSeen++));
    }
    List<String> timerFailures = scheduler.getTimerFailures();
    while (timerFailuresSeen < timerFailures.size()) {
      violation("task failed: " + timerFailures.get(timerFailuresSeen++));
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
  private String describe(Object payload) {
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
    void run(DatanodeDetails node) throws Exception;
  }

  /**
   * Wakes up the replication monitor as ReplicationManager#notifyNodeStateChange wakes up its waiting thread: only when
   * no replication work is queued.
   */
  private final class ReplicationMonitorTrigger implements EventHandler<DatanodeDetails> {
    @Override
    public void onMessage(DatanodeDetails node, EventPublisher publisher) {
      SCMContext context = scm.getScmContext();
      if (context.isLeaderReady() && !context.isInSafeMode()
          && ReplicationSimSupport.hasNoQueuedWork(scm.getReplicationManager())) {
        replicationWakeUp.request();
      }
    }
  }

  /** Runs the pipeline creator when an SCM event asks for a one-shot run, as it wakes up the creator thread. */
  private final class PipelineCreatorTrigger implements SCMService {
    @Override
    public void notifyStatusChanged() {
    }

    @Override
    public void notifyEventTriggered(Event event) {
      pipelineCreatorWakeUp.request();
    }

    @Override
    public boolean shouldRun() {
      return false;
    }

    @Override
    public String getServiceName() {
      return "SimulatedPipelineCreatorTrigger";
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
  }
}
