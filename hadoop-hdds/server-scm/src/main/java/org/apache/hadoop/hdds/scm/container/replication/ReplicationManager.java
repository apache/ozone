/**
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

package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.health.MismatchedReplicasHandler;
import org.apache.hadoop.hdds.scm.container.replication.health.ClosedWithUnhealthyReplicasHandler;
import org.apache.hadoop.hdds.scm.container.replication.health.ClosingContainerHandler;
import org.apache.hadoop.hdds.scm.container.replication.health.DeletingContainerHandler;
import org.apache.hadoop.hdds.scm.container.replication.health.ECReplicationCheckHandler;
import org.apache.hadoop.hdds.scm.container.replication.health.EmptyContainerHandler;
import org.apache.hadoop.hdds.scm.container.replication.health.HealthCheck;
import org.apache.hadoop.hdds.scm.container.replication.health.OpenContainerHandler;
import org.apache.hadoop.hdds.scm.container.replication.health.QuasiClosedContainerHandler;
import org.apache.hadoop.hdds.scm.container.replication.health.RatisReplicationCheckHandler;
import org.apache.hadoop.hdds.scm.container.replication.health.RatisUnhealthyReplicationCheckHandler;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.ExitUtil;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;

/**
 * Replication Manager (RM) is the one which is responsible for making sure
 * that the containers are properly replicated. Replication Manager deals only
 * with Quasi Closed / Closed container.
 */
public class ReplicationManager implements SCMService {

  public static final Logger LOG =
      LoggerFactory.getLogger(ReplicationManager.class);

  /**
   * Reference to the ContainerManager.
   */
  private final ContainerManager containerManager;


  /**
   * SCMContext from StorageContainerManager.
   */
  private final SCMContext scmContext;


  /**
   * ReplicationManager specific configuration.
   */
  private final ReplicationManagerConfiguration rmConf;
  private final NodeManager nodeManager;

  /**
   * ReplicationMonitor thread is the one which wakes up at configured
   * interval and processes all the containers.
   */
  private Thread replicationMonitor;

  /**
   * Flag used for checking if the ReplicationMonitor thread is running or
   * not.
   */
  private volatile boolean running;

  /**
   * Report object that is refreshed each time replication Manager runs.
   */
  private ReplicationManagerReport containerReport;

  /**
   * Replication progress related metrics.
   */
  private ReplicationManagerMetrics metrics;


  /**
   * Legacy RM will hopefully be removed after completing refactor
   * for now, it is used to process non-EC container.
   */
  private LegacyReplicationManager legacyReplicationManager;

  /**
   * SCMService related variables.
   * After leaving safe mode, replicationMonitor needs to wait for a while
   * before really take effect.
   */
  private final Lock serviceLock = new ReentrantLock();
  private ServiceStatus serviceStatus = ServiceStatus.PAUSING;
  private final long waitTimeInMillis;
  private long lastTimeToBeReadyInMillis = 0;
  private final Clock clock;
  private final ContainerReplicaPendingOps containerReplicaPendingOps;
  private final ECReplicationCheckHandler ecReplicationCheckHandler;
  private final RatisReplicationCheckHandler ratisReplicationCheckHandler;
  private final EventPublisher eventPublisher;
  private final ReentrantLock lock = new ReentrantLock();
  private ReplicationQueue replicationQueue;
  private final ECUnderReplicationHandler ecUnderReplicationHandler;
  private final ECOverReplicationHandler ecOverReplicationHandler;
  private final ECMisReplicationHandler ecMisReplicationHandler;
  private final RatisUnderReplicationHandler ratisUnderReplicationHandler;
  private final RatisOverReplicationHandler ratisOverReplicationHandler;
  private final int maintenanceRedundancy;
  private final int ratisMaintenanceMinReplicas;
  private Thread underReplicatedProcessorThread;
  private Thread overReplicatedProcessorThread;
  private final UnderReplicatedProcessor underReplicatedProcessor;
  private final OverReplicatedProcessor overReplicatedProcessor;
  private final HealthCheck containerCheckChain;

  /**
   * Constructs ReplicationManager instance with the given configuration.
   *
   * @param conf The SCM configuration used by RM.
   * @param containerManager The containerManager instance
   * @param ratisContainerPlacement The Ratis container placement policy
   * @param ecContainerPlacement The EC container placement policy
   * @param eventPublisher The eventPublisher instance
   * @param scmContext The SCMContext instance
   * @param nodeManager The nodeManager instance
   * @param clock Clock object used to get the current time
   * @param legacyReplicationManager The legacy ReplicationManager instance
   * @param replicaPendingOps The pendingOps instance
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  public ReplicationManager(final ConfigurationSource conf,
             final ContainerManager containerManager,
             final PlacementPolicy ratisContainerPlacement,
             final PlacementPolicy ecContainerPlacement,
             final EventPublisher eventPublisher,
             final SCMContext scmContext,
             final NodeManager nodeManager,
             final Clock clock,
             final LegacyReplicationManager legacyReplicationManager,
             final ContainerReplicaPendingOps replicaPendingOps)
             throws IOException {
    this.containerManager = containerManager;
    this.scmContext = scmContext;
    this.rmConf = conf.getObject(ReplicationManagerConfiguration.class);
    this.running = false;
    this.clock = clock;
    this.containerReport = new ReplicationManagerReport();
    this.metrics = null;
    this.eventPublisher = eventPublisher;
    this.waitTimeInMillis = conf.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.containerReplicaPendingOps = replicaPendingOps;
    this.legacyReplicationManager = legacyReplicationManager;
    this.ecReplicationCheckHandler =
        new ECReplicationCheckHandler(ecContainerPlacement);
    this.ratisReplicationCheckHandler =
        new RatisReplicationCheckHandler(ratisContainerPlacement);
    this.nodeManager = nodeManager;
    this.replicationQueue = new ReplicationQueue();
    this.maintenanceRedundancy = rmConf.maintenanceRemainingRedundancy;
    this.ratisMaintenanceMinReplicas = rmConf.getMaintenanceReplicaMinimum();

    ecUnderReplicationHandler = new ECUnderReplicationHandler(
        ecContainerPlacement, conf, nodeManager, this);
    ecOverReplicationHandler =
        new ECOverReplicationHandler(ecContainerPlacement, nodeManager);
    ecMisReplicationHandler = new ECMisReplicationHandler(ecContainerPlacement,
        conf, nodeManager, rmConf.isPush());
    ratisUnderReplicationHandler = new RatisUnderReplicationHandler(
        ratisContainerPlacement, conf, nodeManager, this);
    ratisOverReplicationHandler =
        new RatisOverReplicationHandler(ratisContainerPlacement, nodeManager);
    underReplicatedProcessor =
        new UnderReplicatedProcessor(this,
            rmConf.getUnderReplicatedInterval());
    overReplicatedProcessor =
        new OverReplicatedProcessor(this,
            rmConf.getOverReplicatedInterval());

    // Chain together the series of checks that are needed to validate the
    // containers when they are checked by RM.
    containerCheckChain = new OpenContainerHandler(this);
    containerCheckChain
        .addNext(new ClosingContainerHandler(this))
        .addNext(new QuasiClosedContainerHandler(this))
        .addNext(new MismatchedReplicasHandler(this))
        .addNext(new EmptyContainerHandler(this))
        .addNext(new DeletingContainerHandler(this))
        .addNext(ecReplicationCheckHandler)
        .addNext(ratisReplicationCheckHandler)
        .addNext(new ClosedWithUnhealthyReplicasHandler(this))
        .addNext(new RatisUnhealthyReplicationCheckHandler());
    start();
  }

  /**
   * Starts Replication Monitor thread.
   */
  @Override
  public synchronized void start() {
    if (!isRunning()) {
      LOG.info("Starting Replication Monitor Thread.");
      running = true;
      metrics = ReplicationManagerMetrics.create(this);
      if (rmConf.isLegacyEnabled()) {
        legacyReplicationManager.setMetrics(metrics);
      }
      containerReplicaPendingOps.setReplicationMetrics(metrics);
      startSubServices();
    } else {
      LOG.info("Replication Monitor Thread is already running.");
    }
  }

  /**
   * Returns true if the Replication Monitor Thread is running.
   *
   * @return true if running, false otherwise
   */
  public boolean isRunning() {
    if (!running) {
      synchronized (this) {
        return replicationMonitor != null
            && replicationMonitor.isAlive();
      }
    }
    return true;
  }

  /**
   * Stops Replication Monitor thread.
   */
  public synchronized void stop() {
    if (running) {
      LOG.info("Stopping Replication Monitor Thread.");
      underReplicatedProcessorThread.interrupt();
      overReplicatedProcessorThread.interrupt();
      running = false;
      if (rmConf.isLegacyEnabled()) {
        legacyReplicationManager.clearInflightActions();
      }
      metrics.unRegister();
      replicationMonitor.interrupt();
    } else {
      LOG.info("Replication Monitor Thread is not running.");
    }
  }

  /**
   * Create Replication Manager sub services such as Over and Under Replication
   * processors.
   */
  @VisibleForTesting
  protected void startSubServices() {
    replicationMonitor = new Thread(this::run);
    replicationMonitor.setName("ReplicationMonitor");
    replicationMonitor.setDaemon(true);
    replicationMonitor.start();

    underReplicatedProcessorThread = new Thread(underReplicatedProcessor);
    underReplicatedProcessorThread.setName("Under Replicated Processor");
    underReplicatedProcessorThread.setDaemon(true);
    underReplicatedProcessorThread.start();

    overReplicatedProcessorThread = new Thread(overReplicatedProcessor);
    overReplicatedProcessorThread.setName("Over Replicated Processor");
    overReplicatedProcessorThread.setDaemon(true);
    overReplicatedProcessorThread.start();
  }

  /**
   * Process all the containers now, and wait for the processing to complete.
   * This in intended to be used in tests.
   */
  public synchronized void processAll() {
    if (!shouldRun()) {
      LOG.info("Replication Manager is not ready to run until {}ms after " +
          "safemode exit", waitTimeInMillis);
      return;
    }
    final long start = clock.millis();
    final List<ContainerInfo> containers =
        containerManager.getContainers();
    ReplicationManagerReport report = new ReplicationManagerReport();
    ReplicationQueue newRepQueue = new ReplicationQueue();
    for (ContainerInfo c : containers) {
      if (!shouldRun()) {
        break;
      }
      report.increment(c.getState());
      if (c.getReplicationType() != EC && rmConf.isLegacyEnabled()) {
        legacyReplicationManager.processContainer(c, report);
        continue;
      }
      try {
        processContainer(c, newRepQueue, report);
        // TODO - send any commands contained in the health result
      } catch (ContainerNotFoundException e) {
        LOG.error("Container {} not found", c.getContainerID(), e);
      }
    }
    report.setComplete();
    lock.lock();
    try {
      replicationQueue = newRepQueue;
    } finally {
      lock.unlock();
    }
    this.containerReport = report;
    LOG.info("Replication Monitor Thread took {} milliseconds for" +
            " processing {} containers.", clock.millis() - start,
        containers.size());
  }

  /**
   * Retrieve the new highest priority container to be replicated from the
   * under replicated queue.
   * @return The new underReplicated container to be processed, or null if the
   *         queue is empty.
   */
  public ContainerHealthResult.UnderReplicatedHealthResult
      dequeueUnderReplicatedContainer() {
    lock.lock();
    try {
      return replicationQueue.dequeueUnderReplicatedContainer();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Retrieve the new highest priority container to be replicated from the
   * under replicated queue.
   * @return The next over-replicated container to be processed, or null if the
   *         queue is empty.
   */
  public ContainerHealthResult.OverReplicatedHealthResult
      dequeueOverReplicatedContainer() {
    lock.lock();
    try {
      return replicationQueue.dequeueOverReplicatedContainer();
    } finally {
      lock.unlock();
    }
  }

  public void sendCloseContainerEvent(ContainerID containerID) {
    eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER, containerID);
  }

  /**
   * Sends delete container command for the given container to the given
   * datanode.
   *
   * @param container Container to be deleted
   * @param replicaIndex Index of the container replica to be deleted
   * @param datanode  The datanode on which the replica should be deleted
   * @param force true to force delete a container that is open or not empty
   * @throws NotLeaderException when this SCM is not the leader
   */
  public void sendDeleteCommand(final ContainerInfo container, int replicaIndex,
      final DatanodeDetails datanode, boolean force) throws NotLeaderException {
    LOG.debug("Sending delete command for container {} and index {} on {}",
        container, replicaIndex, datanode);
    final DeleteContainerCommand deleteCommand =
        new DeleteContainerCommand(container.containerID(), force);
    deleteCommand.setReplicaIndex(replicaIndex);
    sendDatanodeCommand(deleteCommand, container, datanode);
  }

  /**
   * Send a push replication command to the given source datanode, instructing
   * it to copy the given container to the target.
   * @param container Container to replicate.
   * @param replicaIndex Replica Index of the container to replicate. Zero for
   *                     Ratis and greater than zero for EC.
   * @param source The source hosting the container, which is where the command
   *               will be sent.
   * @param target The target to push container replica to
   * @param scmDeadlineEpochMs The epoch time in ms, after which the command
   *                           will be discarded from the SCMPendingOps table.
   * @param datanodeDeadlineEpochMs The epoch time in ms, after which the
   *                                command will be discarded on the data if
   *                                it has not been processed. In general should
   *                                be less than scmDeadlineEpochMs.
   * @throws NotLeaderException
   */
  public void sendReplicateContainerCommand(final ContainerInfo container,
      int replicaIndex, DatanodeDetails source, DatanodeDetails target,
      long scmDeadlineEpochMs, long datanodeDeadlineEpochMs)
      throws NotLeaderException {
    final ReplicateContainerCommand command = ReplicateContainerCommand
        .toTarget(container.getContainerID(), target);
    command.setReplicaIndex(replicaIndex);
    sendDatanodeCommand(command, container, source, scmDeadlineEpochMs,
        datanodeDeadlineEpochMs);
  }
  /**
   * Sends a command to a datanode with the command deadline set to the default
   * in ReplicationManager config.
   * @param command The command to send.
   * @param containerInfo The container the command is for.
   * @param target The datanode which will receive the command.
   * @throws NotLeaderException
   */
  public void sendDatanodeCommand(SCMCommand<?> command,
      ContainerInfo containerInfo, DatanodeDetails target)
      throws NotLeaderException {
    long scmDeadline = clock.millis() + rmConf.eventTimeout;
    long datanodeDeadline = clock.millis() +
        Math.round(rmConf.eventTimeout * rmConf.commandDeadlineFactor);
    sendDatanodeCommand(command, containerInfo, target, scmDeadline,
        datanodeDeadline);
  }

  /**
   * Sends a command to a datanode with a user defined deadline for the
   * commands.
   * @param command The command to send
   * @param containerInfo The container the command is for.
   * @param target The datanode which will receive the command.
   * @param scmDeadlineEpochMs The epoch time in ms, after which the command
   *                           will be discarded from the SCMPendingOps table.
   * @param datanodeDeadlineEpochMs The epoch time in ms, after which the
   *                                command will be discarded on the datanode if
   *                                it has not been processed.
   * @throws NotLeaderException
   */
  public void sendDatanodeCommand(SCMCommand<?> command,
      ContainerInfo containerInfo, DatanodeDetails target,
      long scmDeadlineEpochMs, long datanodeDeadlineEpochMs)
      throws NotLeaderException {
    LOG.info("Sending command [{}] for container {} to {} with datanode "
        + "deadline {} and scm deadline {}",
        command, containerInfo, target, datanodeDeadlineEpochMs,
        scmDeadlineEpochMs);
    command.setTerm(getScmTerm());
    command.setDeadline(datanodeDeadlineEpochMs);
    final CommandForDatanode<?> datanodeCommand =
        new CommandForDatanode<>(target.getUuid(), command);
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
    adjustPendingOpsAndMetrics(containerInfo, command, target,
        scmDeadlineEpochMs);
  }

  private void adjustPendingOpsAndMetrics(ContainerInfo containerInfo,
      SCMCommand<?> cmd, DatanodeDetails targetDatanode,
      long scmDeadlineEpochMs) {
    if (cmd.getType() == Type.deleteContainerCommand) {
      DeleteContainerCommand rcc = (DeleteContainerCommand) cmd;
      containerReplicaPendingOps.scheduleDeleteReplica(
          containerInfo.containerID(), targetDatanode, rcc.getReplicaIndex(),
          scmDeadlineEpochMs);
      if (rcc.getReplicaIndex() > 0) {
        getMetrics().incrEcDeletionCmdsSentTotal();
      } else if (rcc.getReplicaIndex() == 0) {
        getMetrics().incrNumDeletionCmdsSent();
        getMetrics().incrNumDeletionBytesTotal(containerInfo.getUsedBytes());
      }
    } else if (cmd.getType() == Type.reconstructECContainersCommand) {
      ReconstructECContainersCommand rcc = (ReconstructECContainersCommand) cmd;
      List<DatanodeDetails> targets = rcc.getTargetDatanodes();
      byte[] targetIndexes = rcc.getMissingContainerIndexes();
      for (int i = 0; i < targetIndexes.length; i++) {
        containerReplicaPendingOps.scheduleAddReplica(
            containerInfo.containerID(), targets.get(i), targetIndexes[i],
            scmDeadlineEpochMs);
      }
      getMetrics().incrEcReconstructionCmdsSentTotal();
    } else if (cmd.getType() == Type.replicateContainerCommand) {
      ReplicateContainerCommand rcc = (ReplicateContainerCommand) cmd;
      containerReplicaPendingOps.scheduleAddReplica(containerInfo.containerID(),
          targetDatanode, rcc.getReplicaIndex(), scmDeadlineEpochMs);
      if (rcc.getReplicaIndex() > 0) {
        getMetrics().incrEcReplicationCmdsSentTotal();
      } else if (rcc.getReplicaIndex() == 0) {
        getMetrics().incrNumReplicationCmdsSent();
      }
    }
  }

  /**
   * update container state.
   *
   * @param containerID Container to be updated
   * @param event the event to update the container
   */
  public void updateContainerState(ContainerID containerID,
                                   HddsProtos.LifeCycleEvent event) {
    try {
      containerManager.updateContainerState(containerID, event);
    } catch (IOException | InvalidStateTransitionException |
             TimeoutException e) {
      LOG.error("Failed to update the state of container {}, update Event {}",
          containerID, event, e);
    }
  }


  /**
   * Add an under replicated container back to the queue if it was unable to
   * be processed. Its retry count will be incremented before it is re-queued,
   * reducing its priority.
   * Note that the queue could have been rebuilt and replaced after this
   * message was removed but before it is added back. This will result in a
   * duplicate entry on the queue. However, when it is processed again, the
   * result of the processing will end up with pending replicas scheduled. If
   * instance 1 is processed and creates the pending replicas, when instance 2
   * is processed, it will find the pending containers and know it has no work
   * to do, and be discarded. Additionally, the queue will be refreshed
   * periodically removing any duplicates.
   * @param underReplicatedHealthResult
   */
  public void requeueUnderReplicatedContainer(ContainerHealthResult
      .UnderReplicatedHealthResult underReplicatedHealthResult) {
    underReplicatedHealthResult.incrementRequeueCount();
    lock.lock();
    try {
      replicationQueue.enqueue(underReplicatedHealthResult);
    } finally {
      lock.unlock();
    }
  }

  public void requeueOverReplicatedContainer(ContainerHealthResult
      .OverReplicatedHealthResult overReplicatedHealthResult) {
    lock.lock();
    try {
      replicationQueue.enqueue(overReplicatedHealthResult);
    } finally {
      lock.unlock();
    }
  }

  Set<Pair<DatanodeDetails, SCMCommand<?>>> processUnderReplicatedContainer(
      final ContainerHealthResult result) throws IOException {
    ContainerID containerID = result.getContainerInfo().containerID();
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(
        containerID);
    List<ContainerReplicaOp> pendingOps =
        containerReplicaPendingOps.getPendingOps(containerID);
    if (result.getContainerInfo().getReplicationType() == EC) {
      if (result.getHealthState()
          == ContainerHealthResult.HealthState.UNDER_REPLICATED) {
        return ecUnderReplicationHandler.processAndCreateCommands(replicas,
            pendingOps, result, maintenanceRedundancy);
      } else if (result.getHealthState()
          == ContainerHealthResult.HealthState.MIS_REPLICATED) {
        return ecMisReplicationHandler.processAndCreateCommands(replicas,
            pendingOps, result, maintenanceRedundancy);
      } else {
        throw new IllegalArgumentException("Unexpected health state: "
            + result.getHealthState());
      }
    }
    return ratisUnderReplicationHandler.processAndCreateCommands(replicas,
        pendingOps, result, ratisMaintenanceMinReplicas);
  }

  Set<Pair<DatanodeDetails, SCMCommand<?>>> processOverReplicatedContainer(
      final ContainerHealthResult result) throws IOException {
    ContainerID containerID = result.getContainerInfo().containerID();
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(
        containerID);
    List<ContainerReplicaOp> pendingOps =
        containerReplicaPendingOps.getPendingOps(containerID);
    if (result.getContainerInfo().getReplicationType() == EC) {
      return ecOverReplicationHandler.processAndCreateCommands(replicas,
          pendingOps, result, maintenanceRedundancy);
    }
    return ratisOverReplicationHandler.processAndCreateCommands(replicas,
        pendingOps, result, ratisMaintenanceMinReplicas);
  }

  public long getScmTerm() throws NotLeaderException {
    return scmContext.getTermOfLeader();
  }

  /**
   * Notify ReplicationManager that the command counts on a datanode have been
   * updated via a heartbeat received. This will allow RM to consider the node
   * for container operations if it was previously excluded due to load.
   * @param datanodeDetails The datanode for which the commands have been
   *                        updated.
   */
  public void datanodeCommandCountUpdated(DatanodeDetails datanodeDetails) {
    // For now this is a NOOP, as the plan is to use this notification in a
    // future change to limit the number of commands scheduled against a DN by
    // RM.
    LOG.debug("Received a notification that the DN command count " +
        "has been updated for {}", datanodeDetails);
  }

  protected void processContainer(ContainerInfo containerInfo,
      ReplicationQueue repQueue, ReplicationManagerReport report)
      throws ContainerNotFoundException {

    ContainerID containerID = containerInfo.containerID();
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(
        containerID);
    List<ContainerReplicaOp> pendingOps =
        containerReplicaPendingOps.getPendingOps(containerID);

    // There is a different config for EC and Ratis maintenance
    // minimum replicas, so we must pass through the correct one.
    int maintRedundancy = maintenanceRedundancy;
    if (containerInfo.getReplicationType() == RATIS) {
      maintRedundancy = ratisMaintenanceMinReplicas;
    }
    ContainerCheckRequest checkRequest = new ContainerCheckRequest.Builder()
        .setContainerInfo(containerInfo)
        .setContainerReplicas(replicas)
        .setMaintenanceRedundancy(maintRedundancy)
        .setReport(report)
        .setPendingOps(pendingOps)
        .setReplicationQueue(repQueue)
        .build();
    // This will call the chain of container health handlers in turn which
    // will issue commands as needed, update the report and perhaps add
    // containers to the over and under replicated queue.
    boolean handled = containerCheckChain.handleChain(checkRequest);
    if (!handled) {
      LOG.debug("Container {} had no actions after passing through the " +
          "check chain", containerInfo.containerID());
    }
  }

  /**
   * Sends close container command for the given container to the given
   * datanode.
   *
   * @param container Container to be closed
   * @param datanode The datanode on which the container
   *                  has to be closed
   * @param force Should be set to true if we want to force close.
   */
  public void sendCloseContainerReplicaCommand(final ContainerInfo container,
      final DatanodeDetails datanode, final boolean force) {

    ContainerID containerID = container.containerID();
    CloseContainerCommand closeContainerCommand =
        new CloseContainerCommand(container.getContainerID(),
            container.getPipelineID(), force);
    closeContainerCommand.setEncodedToken(getContainerToken(containerID));
    try {
      sendDatanodeCommand(closeContainerCommand, container, datanode);
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending close container command,"
          + " since current SCM is not leader.", nle);
    }
  }

  private String getContainerToken(ContainerID containerID) {
    if (scmContext.getScm() instanceof StorageContainerManager) {
      StorageContainerManager scm =
          (StorageContainerManager) scmContext.getScm();
      return scm.getContainerTokenGenerator().generateEncodedToken(containerID);
    }
    return ""; // unit test
  }

  public ReplicationManagerReport getContainerReport() {
    return containerReport;
  }

  /**
   * ReplicationMonitor thread runnable. This wakes up at configured
   * interval and processes all the containers in the system.
   */
  private synchronized void run() {
    try {
      while (running) {
        processAll();
        wait(rmConf.getInterval());
      }
    } catch (Throwable t) {
      if (t instanceof InterruptedException) {
        LOG.info("Replication Monitor Thread is stopped");
        Thread.currentThread().interrupt();
      } else {
        // When we get runtime exception, we should terminate SCM.
        LOG.error("Exception in Replication Monitor Thread.", t);
        ExitUtil.terminate(1, t);
      }
    }
  }

  /**
   * Given a ContainerID, lookup the ContainerInfo and then return a
   * ContainerReplicaCount object for the container.
   * @param containerID The ID of the container
   * @return ContainerReplicaCount for the given container
   * @throws ContainerNotFoundException
   */
  public ContainerReplicaCount getContainerReplicaCount(ContainerID containerID)
      throws ContainerNotFoundException {
    ContainerInfo container = containerManager.getContainer(containerID);
    if (container.getReplicationType() == EC) {
      return getECContainerReplicaCount(container);
    }

    if (rmConf.isLegacyEnabled()) {
      return legacyReplicationManager.getContainerReplicaCount(container);
    } else {
      return getRatisContainerReplicaCount(container);
    }
  }

  /**
   * An open container is healthy if all its replicas are in the same state as
   * the container.
   * @param container The container to check
   * @param replicas The replicas belonging to the container
   * @return True if the container is healthy, false otherwise
   */
  private boolean isOpenContainerHealthy(
      ContainerInfo container, Set<ContainerReplica> replicas) {
    HddsProtos.LifeCycleState state = container.getState();
    return replicas.stream()
        .allMatch(r -> compareState(state, r.getState()));
  }

  /**
   * Compares the container state with the replica state.
   *
   * @param containerState ContainerState
   * @param replicaState ReplicaState
   * @return true if the state matches, false otherwise
   */
  public static boolean compareState(
      final HddsProtos.LifeCycleState containerState,
      final ContainerReplicaProto.State replicaState) {
    switch (containerState) {
    case OPEN:
      return replicaState == ContainerReplicaProto.State.OPEN;
    case CLOSING:
      return replicaState == ContainerReplicaProto.State.CLOSING;
    case QUASI_CLOSED:
      return replicaState == ContainerReplicaProto.State.QUASI_CLOSED;
    case CLOSED:
      return replicaState == ContainerReplicaProto.State.CLOSED;
    default:
      return false;
    }
  }

  /**
   * Configuration used by the Replication Manager.
   */
  @ConfigGroup(prefix = "hdds.scm.replication")
  public static class ReplicationManagerConfiguration {
    /**
     * True if LegacyReplicationManager should be used for RATIS containers.
     */
    @Config(key = "enable.legacy",
        type = ConfigType.BOOLEAN,
        defaultValue = "true",
        tags = {SCM, OZONE},
        description = "This configuration decides if " +
            "LegacyReplicationManager should be used to handle RATIS " +
            "containers. Default is true, which means " +
            "LegacyReplicationManager will handle RATIS containers while " +
            "ReplicationManager will handle EC containers. If false, " +
            "ReplicationManager will handle both RATIS and EC."
    )
    private boolean enableLegacy = true;

    public boolean isLegacyEnabled() {
      return enableLegacy;
    }

    public void setEnableLegacy(boolean enableLegacy) {
      this.enableLegacy = enableLegacy;
    }

    /**
     * The frequency in which ReplicationMonitor thread should run.
     */
    @Config(key = "thread.interval",
        type = ConfigType.TIME,
        defaultValue = "300s",
        tags = {SCM, OZONE},
        description = "There is a replication monitor thread running inside " +
            "SCM which takes care of replicating the containers in the " +
            "cluster. This property is used to configure the interval in " +
            "which that thread runs."
    )
    private long interval = Duration.ofSeconds(300).toMillis();

    /**
     * The frequency in which the Under Replicated queue is processed.
     */
    @Config(key = "under.replicated.interval",
        type = ConfigType.TIME,
        defaultValue = "30s",
        tags = {SCM, OZONE},
        description = "How frequently to check if there are work to process " +
            " on the under replicated queue"
    )
    private long underReplicatedInterval = Duration.ofSeconds(30).toMillis();

    /**
     * The frequency in which the Over Replicated queue is processed.
     */
    @Config(key = "over.replicated.interval",
        type = ConfigType.TIME,
        defaultValue = "30s",
        tags = {SCM, OZONE},
        description = "How frequently to check if there are work to process " +
            " on the over replicated queue"
    )
    private long overReplicatedInterval = Duration.ofSeconds(30).toMillis();

    /**
     * Timeout for container replication & deletion command issued by
     * ReplicationManager.
     */
    @Config(key = "event.timeout",
        type = ConfigType.TIME,
        defaultValue = "30m",
        tags = {SCM, OZONE},
        description = "Timeout for the container replication/deletion commands "
            + "sent  to datanodes. After this timeout the command will be "
            + "retried.")
    private long eventTimeout = Duration.ofMinutes(30).toMillis();
    public void setInterval(Duration interval) {
      this.interval = interval.toMillis();
    }

    public void setEventTimeout(Duration timeout) {
      this.eventTimeout = timeout.toMillis();
    }

    /**
     * Deadline which should be set on commands sent from ReplicationManager
     * to the datanodes, as a percentage of the event.timeout. If the command
     * has not been processed on the datanode by this time, it will be dropped
     * by the datanode and Replication Manager will need to resend it.
     */
    @Config(key = "command.deadline.factor",
        type = ConfigType.DOUBLE,
        defaultValue = "0.9",
        tags = {SCM, OZONE},
        description = "Fraction of the hdds.scm.replication.event.timeout "
            + "from the current time which should be set as a deadline for "
            + "commands sent from ReplicationManager to datanodes. "
            + "Commands which are not processed before this deadline will be "
            + "dropped by the datanodes. Should be a value > 0 and <= 1.")
    private double commandDeadlineFactor = 0.9;
    public double getCommandDeadlineFactor() {
      return commandDeadlineFactor;
    }

    public void setCommandDeadlineFactor(double val) {
      commandDeadlineFactor = val;
    }

    /**
     * The number of container replica which must be available for a node to
     * enter maintenance.
     */
    @Config(key = "maintenance.replica.minimum",
        type = ConfigType.INT,
        defaultValue = "2",
        tags = {SCM, OZONE},
        description = "The minimum number of container replicas which must " +
            " be available for a node to enter maintenance. If putting a " +
            " node into maintenance reduces the available replicas for any " +
            " container below this level, the node will remain in the " +
            " entering maintenance state until a new replica is created.")
    private int maintenanceReplicaMinimum = 2;

    public void setMaintenanceReplicaMinimum(int replicaCount) {
      this.maintenanceReplicaMinimum = replicaCount;
    }

    /**
     * Defines how many redundant replicas of a container must be online for a
     * node to enter maintenance. Currently, only used for EC containers. We
     * need to consider removing the "maintenance.replica.minimum" setting
     * and having both Ratis and EC use this new one.
     */
    @Config(key = "maintenance.remaining.redundancy",
        type = ConfigType.INT,
        defaultValue = "1",
        tags = {SCM, OZONE},
        description = "The number of redundant containers in a group which" +
            " must be available for a node to enter maintenance. If putting" +
            " a node into maintenance reduces the redundancy below this value" +
            " , the node will remain in the ENTERING_MAINTENANCE state until" +
            " a new replica is created. For Ratis containers, the default" +
            " value of 1 ensures at least two replicas are online, meaning 1" +
            " more can be lost without data becoming unavailable. For any EC" +
            " container it will have at least dataNum + 1 online, allowing" +
            " the loss of 1 more replica before data becomes unavailable." +
            " Currently only EC containers use this setting. Ratis containers" +
            " use hdds.scm.replication.maintenance.replica.minimum. For EC," +
            " if nodes are in maintenance, it is likely reconstruction reads" +
            " will be required if some of the data replicas are offline. This" +
            " is seamless to the client, but will affect read performance."
    )
    private int maintenanceRemainingRedundancy = 1;

    @Config(key = "push",
        type = ConfigType.BOOLEAN,
        defaultValue = "false",
        tags = { SCM, DATANODE },
        description = "If false, replication happens by asking the target to " +
            "pull from source nodes.  If true, the source node is asked to " +
            "push to the target node."
    )
    private boolean push;

    @PostConstruct
    public void validate() {
      if (!(commandDeadlineFactor > 0) || (commandDeadlineFactor > 1)) {
        throw new IllegalArgumentException("command.deadline.factor is set to "
            + commandDeadlineFactor
            + " and must be greater than 0 and less than equal to 1");
      }
    }

    public void setMaintenanceRemainingRedundancy(int redundancy) {
      this.maintenanceRemainingRedundancy = redundancy;
    }

    public int getMaintenanceRemainingRedundancy() {
      return maintenanceRemainingRedundancy;
    }

    public long getInterval() {
      return interval;
    }

    public long getUnderReplicatedInterval() {
      return underReplicatedInterval;
    }

    public void setUnderReplicatedInterval(Duration duration) {
      this.underReplicatedInterval = duration.toMillis();
    }

    public void setOverReplicatedInterval(Duration duration) {
      this.overReplicatedInterval = duration.toMillis();
    }

    public long getOverReplicatedInterval() {
      return overReplicatedInterval;
    }

    public long getEventTimeout() {
      return eventTimeout;
    }

    public int getMaintenanceReplicaMinimum() {
      return maintenanceReplicaMinimum;
    }

    public boolean isPush() {
      return push;
    }
  }

  @Override
  public void notifyStatusChanged() {
    serviceLock.lock();
    try {
      // 1) SCMContext#isLeaderReady returns true.
      // 2) not in safe mode.
      if (scmContext.isLeaderReady() && !scmContext.isInSafeMode()) {
        // transition from PAUSING to RUNNING
        if (serviceStatus != ServiceStatus.RUNNING) {
          LOG.info("Service {} transitions to RUNNING.", getServiceName());
          lastTimeToBeReadyInMillis = clock.millis();
          serviceStatus = ServiceStatus.RUNNING;
        }
        if (rmConf.isLegacyEnabled()) {
          //now, as the current scm is leader and it`s state is up-to-date,
          //we need to take some action about replicated inflight move options.
          legacyReplicationManager.notifyStatusChanged();
        }
      } else {
        serviceStatus = ServiceStatus.PAUSING;
      }
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public boolean shouldRun() {
    serviceLock.lock();
    try {
      // If safe mode is off, then this SCMService starts to run with a delay.
      return serviceStatus == ServiceStatus.RUNNING &&
          clock.millis() - lastTimeToBeReadyInMillis >= waitTimeInMillis;
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public String getServiceName() {
    return ReplicationManager.class.getSimpleName();
  }

  public synchronized ReplicationManagerMetrics getMetrics() {
    return metrics;
  }

  public ReplicationManagerConfiguration getConfig() {
    return rmConf;
  }


  /**
  * following functions will be refactored in a separate jira.
  */
  public CompletableFuture<LegacyReplicationManager.MoveResult> move(
      ContainerID cid, DatanodeDetails src, DatanodeDetails tgt)
      throws NodeNotFoundException, ContainerNotFoundException,
      TimeoutException {
    CompletableFuture<LegacyReplicationManager.MoveResult> ret =
        new CompletableFuture<>();
    if (!isRunning()) {
      ret.complete(LegacyReplicationManager.MoveResult.FAIL_NOT_RUNNING);
      return ret;
    }

    return legacyReplicationManager.move(cid, src, tgt);
  }

  public Map<ContainerID,
      CompletableFuture<LegacyReplicationManager.MoveResult>>
      getInflightMove() {
    return legacyReplicationManager.getInflightMove();
  }

  public LegacyReplicationManager.MoveScheduler getMoveScheduler() {
    return legacyReplicationManager.getMoveScheduler();
  }

  @VisibleForTesting
  public LegacyReplicationManager getLegacyReplicationManager() {
    return legacyReplicationManager;
  }

  public boolean isContainerReplicatingOrDeleting(ContainerID containerID) {
    return legacyReplicationManager
        .isContainerReplicatingOrDeleting(containerID);
  }

  private ECContainerReplicaCount getECContainerReplicaCount(
      ContainerInfo containerInfo) throws ContainerNotFoundException {
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(
        containerInfo.containerID());
    List<ContainerReplicaOp> pendingOps =
        containerReplicaPendingOps.getPendingOps(containerInfo.containerID());
    return new ECContainerReplicaCount(
        containerInfo, replicas, pendingOps, maintenanceRedundancy);
  }

  private RatisContainerReplicaCount getRatisContainerReplicaCount(
      ContainerInfo containerInfo) throws ContainerNotFoundException {
    Set<ContainerReplica> replicas =
        containerManager.getContainerReplicas(containerInfo.containerID());
    List<ContainerReplicaOp> pendingOps =
        containerReplicaPendingOps.getPendingOps(containerInfo.containerID());
    return new RatisContainerReplicaCount(containerInfo, replicas, pendingOps,
        ratisMaintenanceMinReplicas, false);
  }
  
  public ContainerReplicaPendingOps getContainerReplicaPendingOps() {
    return containerReplicaPendingOps;
  }

  /**
   * Wrap the call to nodeManager.getNodeStatus, catching any
   * NodeNotFoundException and instead throwing an IllegalStateException.
   * @param dn The datanodeDetails to obtain the NodeStatus for
   * @return NodeStatus corresponding to the given Datanode.
   */
  static NodeStatus getNodeStatus(DatanodeDetails dn, NodeManager nm) {
    try {
      return nm.getNodeStatus(dn);
    } catch (NodeNotFoundException e) {
      throw new IllegalStateException("Unable to find NodeStatus for " + dn, e);
    }
  }
}

