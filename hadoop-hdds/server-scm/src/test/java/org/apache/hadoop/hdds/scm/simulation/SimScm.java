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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ExtendedDatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DatanodeVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.block.SCMBlockDeletingService;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationSimSupport;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.BackgroundSCMService;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBuffer;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBufferImpl;
import org.apache.hadoop.hdds.scm.ha.SCMHATransactionBufferMonitorTask;
import org.apache.hadoop.hdds.scm.ha.SCMRatisRequest;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.invoker.ScmInvoker;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeDecommissionManager;
import org.apache.hadoop.hdds.scm.node.NodeSimSupport;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.pipeline.BackgroundPipelineCreator;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineSimSupport;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.ScmVersionManager;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.server.protocol.TermIndex;

/**
 * One SCM process: the real {@link StorageContainerManager} with its own DB, network topology, node manager and event
 * queue, whose periodic services run on scheduler timers. The SCMs form a Ratis group, {@link SimScmRatis}: a
 * replicated call goes through the group's log, and every SCM applies the entry with its own state machine handlers.
 * With one SCM the group has one member, which is always the leader.
 */
final class SimScm {

  private static final int REPORT_LANES = 10;

  private final ScmSimulation sim;
  private final SimScmRatis ratis;
  private final String name;
  /** Prefix of timer and lane names: "scm." with one SCM, as before SCM HA was simulated. */
  private final String prefix;
  private final Path dir;
  private final String clusterId;
  private final String scmId;
  private final Map<RequestType, ScmInvoker<?>> invokers = new EnumMap<>(RequestType.class);
  private final List<SimScheduler.Timer> timers = new ArrayList<>();
  private final List<SimScheduler.WakeUp> wakeUps = new ArrayList<>();
  private StorageContainerManager scm;
  private SCMContext context;
  private SCMNodeManager nodeManager;
  private SimEventQueue eventQueue;
  private SimScheduler.Lane stateMachine;
  private SimScheduler.WakeUp replicationWakeUp;
  private SimScheduler.WakeUp pipelineCreatorWakeUp;
  private boolean running;
  private boolean partitioned;
  /** Whether the datanode protocol server runs, see SCMStateMachine#tryStartDNServerAndRefreshSafeMode. */
  private boolean datanodeServerStarted;
  private boolean wasInSafeMode;
  /** Whether SCM started with an empty DB, as a new SCM does. */
  private boolean startedEmpty;
  /** The leader's log size when this SCM learned about the leader; once applied, it may serve datanodes. */
  private long catchUpTarget = Long.MAX_VALUE;
  /** Index of the last log entry this SCM has, and applied. */
  private long receivedIndex;
  private long appliedIndex;
  /** Index of the last snapshot: the last entry in the DB when this SCM last flushed it for Ratis. */
  private long snapshotIndex;
  private boolean snapshotPending;
  /** The first entry of this SCM's Raft log; Ratis purged the ones before. */
  private long logStart = 1;
  /** Whether this SCM is downloading the leader's checkpoint to install it. */
  private boolean installing;
  /** Whether Ratis acknowledged the checkpoint, and the state machine still has to reload its state from it. */
  private boolean reloadPending;
  /** Whether the next entry to apply is not in this SCM's log any more, so it can never apply it. */
  private boolean stuck;
  private int checkpoints;
  /** The DB transaction buffer, as in production; SCMStateMachine shares it with the managers. */
  private SCMHADBTransactionBuffer buffer;
  private int handlerFailuresSeen;

  SimScm(ScmSimulation sim, SimScmRatis ratis, String name, boolean ha, Path dir, String clusterId, String scmId) {
    this.sim = sim;
    this.ratis = ratis;
    this.name = name;
    this.prefix = ha ? name + "." : "scm.";
    this.dir = dir;
    this.clusterId = clusterId;
    this.scmId = scmId;
  }

  String getName() {
    return name;
  }

  StorageContainerManager getScm() {
    return scm;
  }

  SCMNodeManager getNodeManager() {
    return nodeManager;
  }

  SimEventQueue getEventQueue() {
    return eventQueue;
  }

  boolean isRunning() {
    return running;
  }

  /** Up and able to reach the other SCMs and the datanodes. */
  boolean isConnected() {
    return running && !partitioned;
  }

  /** Whether datanodes can register and send heartbeats to this SCM. */
  boolean isReachable() {
    return isConnected() && datanodeServerStarted;
  }

  long getReceivedIndex() {
    return receivedIndex;
  }

  long getAppliedIndex() {
    return appliedIndex;
  }

  long getLogStart() {
    return logStart;
  }

  boolean isInstalling() {
    return installing;
  }

  boolean startedEmpty() {
    return startedEmpty;
  }

  /** Whether SCM left safe mode since the last call. */
  boolean leftSafeMode() {
    boolean inSafeMode = scm.getScmSafeModeManager().getInSafeMode();
    boolean left = wasInSafeMode && !inSafeMode;
    wasInSafeMode = inSafeMode;
    return left;
  }

  /** Whether the state machine has nothing left to apply now: it applied what it has, or it cannot go on. */
  boolean isApplyIdle() {
    return stateMachine.isIdle();
  }

  /** Whether this SCM acts as the leader of the given term, or of any term if it is negative. */
  boolean isLeaderOfTerm(long term) {
    try {
      return running && context.isLeader() && (term < 0 || context.getTermOfLeader() == term);
    } catch (IOException e) {
      return false;
    }
  }

  // ---- process lifecycle ----

  /**
   * Starts the SCM process. Its managers load what its DB holds, then it applies again the log entries after the last
   * flush, as Ratis replays its log, and joins the group.
   */
  void start() throws IOException {
    OzoneConfiguration conf = sim.config().toOzoneConfiguration(dir, sim.random(SimRandom.SCM).nextLong());
    SCMStorageConfig storage = new SCMStorageConfig(conf);
    if (storage.getState() != Storage.StorageState.INITIALIZED) {
      storage.setClusterId(clusterId);
      storage.setScmId(scmId);
      storage.setSCMHAFlag(true);
      storage.initialize();
    }
    SCMMetadataStore metadataStore = new SCMMetadataStoreImpl(conf);
    context = new SCMContext.Builder()
        .setLeader(false)
        .setTerm(0)
        .setSafeModeStatus(SCMSafeModeManager.SafeModeStatus.INITIAL)
        .setSCM(scmProxy())
        .build();
    eventQueue = new SimEventQueue(sim.scheduler(), prefix.equals("scm.") ? "" : prefix, sim::describe, REPORT_LANES,
        (handler, payload) -> sim.beforeHandler(this, handler, payload),
        (handler, payload) -> sim.afterHandler(this, handler, payload));
    NetworkTopology topology = new NetworkTopologyImpl(conf, sim.random(SimRandom.SCM));
    // Built here to track heartbeats on the simulated clock, and to take rack locations from the datanodes.
    nodeManager = new SCMNodeManager(conf, storage, eventQueue, topology, context,
        new ScmVersionManager(storage, scmProxy()), host -> null, sim.clock());
    NodeSimSupport.stopHealthCheckThread(nodeManager);

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setMetadataStore(metadataStore);
    SimScmRatis.Server server = new SimScmRatis.Server(this, ratis);
    configurator.setSCMHAManager(new SimScmRatis.HAManager(server, lazy(SCMHADBTransactionBuffer.class, () -> buffer)));
    configurator.setScmContext(context);
    configurator.setNetworkTopology(topology);
    configurator.setScmNodeManager(nodeManager);
    configurator.setLeaseManager(new SimLeaseManager(sim.scheduler(), sim.config().getCloseContainerWaitMs()));
    configurator.setEventQueue(eventQueue);
    configurator.setSystemClock(sim.clock());
    Random ids = sim.random(SimRandom.IDS);
    configurator.setPipelineIdGenerator(() -> PipelineID.valueOf(new UUID(ids.nextLong(), ids.nextLong())));
    configurator.setRandom(sim.random(SimRandom.SCM));
    try {
      scm = StorageContainerManager.createSCM(conf, configurator);
    } catch (AuthenticationException e) {
      throw new IOException(e);
    }
    buffer = new SCMHADBTransactionBufferImpl(scm);
    running = true;
    stateMachine = sim.scheduler().newLane(prefix + "stateMachine");
    snapshotPending = false;
    wasInSafeMode = true;
    // Like SCMStateMachine: the DB has the entries up to the transaction info flushed with them; Ratis replays the
    // rest.
    startedEmpty = buffer.getLatestTrxInfo().isDefault();
    appliedIndex = Math.max(0, buffer.getLatestTrxInfo().getTransactionIndex());
    snapshotIndex = appliedIndex;
    applyUpTo(receivedIndex);
    runServices();
    ratis.memberUp(this);
  }

  /**
   * Stops the process. Events being handled, pending commands and all other in-memory state are lost; the DB keeps what
   * was flushed, so a crash loses the entries applied since, until they are applied again.
   * @param flush whether to flush the DB first, as a graceful stop does
   */
  void stop(boolean flush) throws IOException {
    if (!running) {
      return;
    }
    if (flush) {
      flush();
    }
    running = false;
    installing = false;
    reloadPending = false;
    stuck = false;
    datanodeServerStarted = false;
    catchUpTarget = Long.MAX_VALUE;
    timers.forEach(sim.scheduler()::cancel);
    timers.clear();
    wakeUps.forEach(SimScheduler.WakeUp::cancel);
    wakeUps.clear();
    eventQueue.removeLanes();
    sim.scheduler().removeLanes(Collections.singletonList(stateMachine));
    ratis.memberDown(this);
    scm.stop();
    nodeManager.close();
  }

  /** Cuts (or restores) the network between this SCM and the other SCMs and the datanodes. */
  void setPartitioned(boolean value) {
    if (partitioned == value) {
      return;
    }
    partitioned = value;
    if (!running) {
      return;
    }
    if (value) {
      ratis.memberDown(this);
    } else {
      ratis.memberUp(this);
    }
  }

  void flush() throws IOException {
    buffer.flush();
  }

  // ---- state machine ----

  void registerInvoker(ScmInvoker<?> invoker) {
    invokers.put(invoker.getType(), invoker);
  }

  void received(long index) {
    receivedIndex = Math.max(receivedIndex, index);
  }

  /** Applies the entries up to the given one on the state machine lane, behind the entries queued before. */
  void scheduleApply(long index) {
    stateMachine.add("apply " + index, () -> applyUpTo(index));
  }

  /** Applies the entries this SCM has, up to the given one, in order. */
  void applyUpTo(long index) {
    if (reloadPending) {
      // Ratis paused the state machine to reload it from a snapshot.
      return;
    }
    long target = Math.min(index, receivedIndex);
    while (running && appliedIndex < target) {
      long next = appliedIndex + 1;
      if (next < logStart) {
        if (!stuck) {
          stuck = true;
          sim.record("scm.stuck", name + " applied " + appliedIndex + ", but its log starts at " + logStart);
        }
        return;
      }
      try {
        apply(next);
      } catch (Exception e) {
        if (!isRejection(e)) {
          sim.violation(name + " failed to apply log entry " + next + ": " + e);
        }
      }
    }
  }

  /** Applies one entry like SCMStateMachine#applyTransaction, returning its result. */
  Message apply(long index) throws Exception {
    buffer.beginApplyingTransaction();
    try {
      SCMRatisRequest request = SCMRatisRequest.decode(ratis.entry(index));
      return invokers.get(request.getType()).invokeLocal(request.getOperation(), request.getArguments());
    } finally {
      buffer.updateLatestTrxInfo(TransactionInfo.valueOf(ratis.termOf(index), index));
      appliedIndex = index;
      buffer.flushIfPendingLimitReached();
      buffer.endApplyingTransaction();
      maybeStartDatanodeServer();
      if (!snapshotPending && appliedIndex - snapshotIndex >= sim.config().getScmSnapshotThreshold()) {
        // Ratis takes a snapshot after the apply, on the same thread.
        snapshotPending = true;
        stateMachine.add("takeSnapshot", this::takeSnapshot);
      }
    }
  }

  /** Like SCMStateMachine#takeSnapshot, which Ratis calls every snapshot threshold entries: a flush of the DB. */
  private void takeSnapshot() {
    snapshotPending = false;
    if (!running) {
      return;
    }
    buffer.beginApplyingTransaction();
    try {
      buffer.flush();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      buffer.endApplyingTransaction();
    }
    snapshotIndex = buffer.getLatestTrxInfo().getTransactionIndex();
    // Then Ratis purges the log: up to the snapshot if log purge is on, otherwise only what every SCM has.
    SimConfig config = sim.config();
    long purgeIndex = config.isScmLogPurge() ? snapshotIndex : Math.min(snapshotIndex, ratis.minReceivedIndex());
    if (purgeIndex - (logStart - 1) >= config.getScmLogPurgeGap() && purgeIndex >= logStart) {
      logStart = purgeIndex + 1;
      sim.record("scm.purge", name + " purged its log up to " + purgeIndex);
    }
  }

  /**
   * The leader purged entries this SCM still needs, so Ratis notifies it to install a snapshot. Like
   * SCMStateMachine#notifyInstallSnapshotFromLeader, it downloads a checkpoint of the leader's DB; when that is done,
   * Ratis pauses the state machine and SCMStateMachine#reinitialize reloads the state from the checkpoint.
   */
  void installSnapshot(SimScm from) {
    installing = true;
    sim.count("scm.installSnapshot");
    sim.record("scm.installSnapshot", name + " applied " + appliedIndex + ", " + from.getName()
        + " has entries from " + from.getLogStart());
    DBCheckpoint checkpoint;
    try {
      checkpoint = from.serveCheckpoint(dir.resolve("snapshot").resolve("checkpoint-" + ++checkpoints));
    } catch (IOException e) {
      installFailed(from.getName() + " could not serve a checkpoint: " + e);
      return;
    }
    timers.add(sim.scheduler().schedule(prefix + "installSnapshot",
        sim.config().getCheckpointDownloadMs(sim.random(SimRandom.SCM_HA)), () -> finishInstall(from, checkpoint)));
  }

  /** Like InterSCMGrpcService#download: flushes the buffer, then sends a checkpoint of the DB. */
  DBCheckpoint serveCheckpoint(Path target) throws IOException {
    flush();
    DBCheckpoint checkpoint = scm.getScmMetadataStore().getStore().getCheckpoint(true);
    try {
      FileUtils.copyDirectory(checkpoint.getCheckpointLocation().toFile(), target.toFile());
    } finally {
      checkpoint.cleanupCheckpoint();
    }
    return new RocksDBCheckpoint(target);
  }

  private void finishInstall(SimScm from, DBCheckpoint checkpoint) {
    if (!running || !installing) {
      FileUtils.deleteQuietly(checkpoint.getCheckpointLocation().toFile());
      return;
    }
    long checkpointIndex = -1;
    if (from.isConnected() && !partitioned) {
      try {
        // Like SCMHAManagerImpl#verifyCheckpointFromLeader: only a checkpoint after the applied entries is installed.
        checkpointIndex = HAUtils.getTrxnInfoFromCheckpoint(scm.getConfiguration(), checkpoint.getCheckpointLocation(),
            SCMDBDefinition.get()).getTransactionIndex();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    if (checkpointIndex <= appliedIndex) {
      FileUtils.deleteQuietly(checkpoint.getCheckpointLocation().toFile());
      installFailed(checkpointIndex < 0 ? "the download from " + from.getName() + " failed"
          : "the checkpoint is at " + checkpointIndex + ", but " + name + " applied " + appliedIndex);
      return;
    }
    // On the leader's next notification, Ratis acknowledges the snapshot before the state machine reloads it: it purges
    // this SCM's log up to the checkpoint, and the leader sends the entries after it. The reload runs on the state
    // machine thread.
    installing = false;
    reloadPending = true;
    logStart = checkpointIndex + 1;
    receivedIndex = checkpointIndex;
    stateMachine.add("reinitialize", () -> reload(from, checkpoint));
    ratis.memberUp(this);
  }

  /** Like SCMStateMachine#reinitialize: installs the checkpoint, then resumes after its last entry. */
  private void reload(SimScm from, DBCheckpoint checkpoint) {
    try {
      if (sim.config().hasWorkaround("HDDS-16930")) {
        // The flush that DeletedBlockLogStateManagerImpl#reinitialize expects to happen before.
        flush();
      }
      TermIndex installed = scm.getScmHAManager().installCheckpoint(checkpoint);
      buffer.init();
      appliedIndex = installed.getIndex();
      snapshotIndex = appliedIndex;
      reloadPending = false;
      sim.record("scm.snapshotInstalled", name + " at " + appliedIndex + " from " + from.getName());
      sim.snapshotInstalled(this);
    } catch (Exception e) {
      sim.violation(name + " failed to install the snapshot of " + from.getName() + ", so SCM exits: " + e + " at "
          + Arrays.stream(e.getStackTrace()).filter(f -> f.getClassName().startsWith("org.apache.hadoop")).findFirst()
          .orElse(null));
      try {
        stop(false);
      } catch (IOException stopFailure) {
        throw new UncheckedIOException(stopFailure);
      }
      return;
    } finally {
      FileUtils.deleteQuietly(checkpoint.getCheckpointLocation().toFile());
    }
    applyUpTo(receivedIndex);
  }

  private void installFailed(String reason) {
    installing = false;
    sim.count("scm.installSnapshot.failed");
    sim.record("scm.installSnapshot.failed", name + ": " + reason);
    ratis.retryCatchUp(this);
  }

  /**
   * An SCMException other than INTERNAL_ERROR or IO_EXCEPTION is a logical rejection: the state machine returns it to
   * the caller and goes on. Anything else makes SCM exit.
   */
  static boolean isRejection(Exception e) {
    Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
    if (!(cause instanceof SCMException)) {
      return false;
    }
    SCMException.ResultCodes result = ((SCMException) cause).getResult();
    return result != SCMException.ResultCodes.INTERNAL_ERROR && result != SCMException.ResultCodes.IO_EXCEPTION;
  }

  /**
   * Like SCMStateMachine#notifyLeaderChanged on the new leader, then notifyLeaderReady once all entries are applied.
   */
  void becomeLeader(long term) {
    context.updateLeaderAndTerm(true, term);
    scm.getSequenceIdGen().invalidateBatch();
    maybeStartDatanodeServer();
    ((DeletedBlockLogImpl) scm.getScmBlockManager().getDeletedBlockLog()).onBecomeLeader();
    scm.getScmDecommissionManager().onBecomeLeader();
    stateMachine.add("notifyLeaderReady", () -> {
      if (isLeaderOfTerm(term)) {
        applyUpTo(receivedIndex);
        try {
          flush();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        ((DeletedBlockLogImpl) scm.getScmBlockManager().getDeletedBlockLog()).onLeaderReady();
        context.setLeaderReady();
        scm.getSCMServiceManager().notifyStatusChanged();
        sim.record("scm.leaderReady", name + " term " + term);
      }
    });
  }

  /** Like SCMStateMachine#notifyNotLeader. */
  void stepDown() {
    if (!running) {
      return;
    }
    context.updateLeaderAndTerm(false, 0);
    scm.getSCMServiceManager().notifyStatusChanged();
  }

  /** Like SCMStateMachine#notifyLeaderChanged on a follower: it serves datanodes once caught up with the leader. */
  void followLeader() {
    catchUpTarget = ratis.size();
    maybeStartDatanodeServer();
  }

  private void maybeStartDatanodeServer() {
    if (datanodeServerStarted || !running) {
      return;
    }
    if (context.isLeader() || ratis.getLeader() != null && appliedIndex >= catchUpTarget) {
      datanodeServerStarted = true;
      scm.getScmSafeModeManager().refreshAndValidate();
    }
  }

  // ---- datanode boundary, through the datanode protocol server ----

  SCMRegisteredResponseProto register(ExtendedDatanodeDetailsProto details, NodeReportProto nodeReport,
      ContainerReportsProto containerReport, PipelineReportsProto pipelineReports, DatanodeVersionProto version)
      throws IOException {
    return scm.getDatanodeProtocolServer().register(details, nodeReport, containerReport, pipelineReports, version);
  }

  SCMHeartbeatResponseProto sendHeartbeat(SCMHeartbeatRequestProto request) throws IOException, TimeoutException {
    return scm.getDatanodeProtocolServer().sendHeartbeat(request);
  }

  List<String> getHandlerFailures() {
    List<String> failures = eventQueue.getHandlerFailures();
    List<String> result = new ArrayList<>(failures.subList(handlerFailuresSeen, failures.size()));
    handlerFailuresSeen = failures.size();
    return result;
  }

  // ---- services ----

  /**
   * SCM runs its periodic services on their own threads. Stop those threads, and run each service's task on the
   * scheduler instead, at the same interval and behind the same checks, woken up by the same requests.
   */
  private void runServices() {
    handlerFailuresSeen = 0;
    scm.getSCMServiceManager().stop();
    NodeDecommissionManager decommissionManager = scm.getScmDecommissionManager();
    decommissionManager.stop();
    ReplicationManager rm = scm.getReplicationManager();
    PipelineManagerImpl pipelineManager = (PipelineManagerImpl) scm.getPipelineManager();
    BackgroundPipelineCreator creator = pipelineManager.getBackgroundPipelineCreator();
    // Like the pipeline scrubber, the expired replica op scrubber is a BackgroundSCMService, which waits as long.
    BackgroundSCMService scrubber = pipelineManager.getBackgroundPipelineScrubber();
    SimConfig config = sim.config();

    replicationWakeUp = newWakeUp("replicationMonitor.wakeUp", rm::processAll);
    pipelineCreatorWakeUp = newWakeUp("pipelineCreator.wakeUp", () -> PipelineSimSupport.runPipelineCreator(creator));
    scm.getSCMServiceManager().register(new PipelineCreatorTrigger());
    eventQueue.addHandler(SCMEvents.REPLICATION_MANAGER_NOTIFY, new ReplicationMonitorTrigger());

    every("nodeHealthCheck", config.getHeartbeatProcessIntervalMs(),
        () -> NodeSimSupport.checkNodesHealth(nodeManager));
    every("replicationMonitor", config.getReplicationIntervalMs(), rm::processAll);
    every("underReplicatedProcessor", config.getUnderReplicatedIntervalMs(),
        () -> ReplicationSimSupport.processUnderReplicated(rm));
    every("overReplicatedProcessor", config.getOverReplicatedIntervalMs(),
        () -> ReplicationSimSupport.processOverReplicated(rm));
    every("pipelineCreator", config.getPipelineCreationIntervalMs(),
        () -> PipelineSimSupport.runPipelineCreator(creator));
    every("pipelineScrubber", config.getPipelineScrubIntervalMs(), () -> {
      if (scrubber.shouldRun()) {
        pipelineManager.scrubAndClosePipelinesMissingDataStreamPort();
      }
    });
    every("replicaOpScrubber", config.getReplicaOpScrubIntervalMs(), () -> {
      if (scrubber.shouldRun()) {
        rm.getContainerReplicaPendingOps().removeExpiredEntries();
      }
    });
    every("datanodeAdminMonitor", config.getAdminMonitorIntervalMs(), decommissionManager.getMonitor()::run);
    // Like the safe mode refresh thread: while in safe mode, the rules look at the current containers and pipelines
    // again, and are validated.
    every("safeModeRefresh", config.getSafeModeRefreshIntervalMs(), () -> {
      if (scm.getScmSafeModeManager().getInSafeMode()) {
        scm.getScmSafeModeManager().refreshAndValidate();
      }
    });
    // Like SCMHAManagerImpl#createStartTransactionBufferMonitor: the leader, once ready and out of safe mode, flushes
    // its buffer every interval, so what it writes reaches readers of the DB tables such as the block deleting service.
    // The other SCMs flush when they take a snapshot.
    long flushIntervalMs = config.getTransactionBufferFlushIntervalMs();
    SCMHATransactionBufferMonitorTask flushTask = new SCMHATransactionBufferMonitorTask(buffer, flushIntervalMs);
    BackgroundSCMService bufferMonitor = new BackgroundSCMService.Builder().setClock(scm.getSystemClock())
        .setScmContext(context).setServiceName("SCMHATransactionMonitor").setIntervalInMillis(flushIntervalMs)
        .setWaitTimeInMillis(flushIntervalMs).setPeriodicalTask(flushTask).build();
    scm.getSCMServiceManager().register(bufferMonitor);
    every("transactionBufferFlush", flushIntervalMs, () -> {
      if (bufferMonitor.shouldRun()) {
        flushTask.run();
      }
    });
    SCMBlockDeletingService blockDeletingService = scm.getScmBlockManager().getSCMBlockDeletingService();
    every("blockDeletingService", config.getBlockDeletionIntervalMs(), () -> {
      try {
        blockDeletingService.getTasks().poll().call();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      sim.blockDeletingServiceRan(this);
    });
  }

  private void every(String task, long intervalMs, Runnable action) {
    timers.add(sim.scheduler().scheduleEvery(prefix + task, intervalMs, intervalMs, action));
  }

  private SimScheduler.WakeUp newWakeUp(String task, Runnable action) {
    SimScheduler.WakeUp wakeUp = sim.scheduler().newWakeUp(prefix + task, action);
    wakeUps.add(wakeUp);
    return wakeUp;
  }

  /**
   * The SCM that managers reach through {@link SCMContext#getScm()}. The context has to exist before SCM, which is
   * looked up only when called.
   */
  private OzoneStorageContainerManager scmProxy() {
    return lazy(OzoneStorageContainerManager.class, () -> scm);
  }

  /** An object of the given interface that calls the supplier's object, for what SCM needs before it exists. */
  private <T> T lazy(Class<T> type, Supplier<?> target) {
    return type.cast(Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {type},
        (proxy, method, args) -> {
          try {
            return method.invoke(target.get(), args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        }));
  }

  @Override
  public String toString() {
    return name;
  }

  /**
   * Wakes up the replication monitor as ReplicationManager#notifyNodeStateChange wakes up its waiting thread: only when
   * no replication work is queued.
   */
  private final class ReplicationMonitorTrigger implements EventHandler<DatanodeDetails> {
    @Override
    public void onMessage(DatanodeDetails node, EventPublisher publisher) {
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
