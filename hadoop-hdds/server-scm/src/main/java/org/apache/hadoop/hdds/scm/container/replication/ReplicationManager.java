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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.balancer.MoveManager;
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
import org.apache.hadoop.ozone.container.replication.ReplicationServer;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.isDecommission;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.isMaintenance;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;

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
  /**
   * Datanodes' replication configuration.
   */
  private final ReplicationServer.ReplicationConfig replicationServerConf;
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
  private final LegacyReplicationManager legacyReplicationManager;

  /**
   * Set of nodes which have been excluded for replication commands due to the
   * number of commands queued on a datanode. This can be used when generating
   * reconstruction commands to avoid nodes which are already overloaded. When
   * the datanode heartbeat is received, the node is removed from this set if
   * the command count has dropped below the limit.
   */
  private final Map<DatanodeDetails, Integer> excludedNodes =
      new ConcurrentHashMap<>();

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
  private final AtomicReference<ReplicationQueue> replicationQueue
      = new AtomicReference<>(new ReplicationQueue());
  private final ECUnderReplicationHandler ecUnderReplicationHandler;
  private final ECOverReplicationHandler ecOverReplicationHandler;
  private final ECMisReplicationHandler ecMisReplicationHandler;
  private final RatisUnderReplicationHandler ratisUnderReplicationHandler;
  private final RatisOverReplicationHandler ratisOverReplicationHandler;
  private final RatisMisReplicationHandler ratisMisReplicationHandler;
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
    this.replicationServerConf =
        conf.getObject(ReplicationServer.ReplicationConfig.class);
    this.running = false;
    this.clock = clock;
    this.containerReport = new ReplicationManagerReport();
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

    ecUnderReplicationHandler = new ECUnderReplicationHandler(
        ecContainerPlacement, conf, this);
    ecOverReplicationHandler =
        new ECOverReplicationHandler(ecContainerPlacement, this);
    ecMisReplicationHandler = new ECMisReplicationHandler(ecContainerPlacement,
        conf, this);
    ratisUnderReplicationHandler = new RatisUnderReplicationHandler(
        ratisContainerPlacement, conf, this);
    ratisOverReplicationHandler =
        new RatisOverReplicationHandler(ratisContainerPlacement, this);
    ratisMisReplicationHandler = new RatisMisReplicationHandler(
        ratisContainerPlacement, conf, this);
    underReplicatedProcessor =
        new UnderReplicatedProcessor(this, rmConf::getUnderReplicatedInterval);
    overReplicatedProcessor =
        new OverReplicatedProcessor(this, rmConf::getOverReplicatedInterval);

    // Chain together the series of checks that are needed to validate the
    // containers when they are checked by RM.
    containerCheckChain = new OpenContainerHandler(this);
    containerCheckChain
        .addNext(new ClosingContainerHandler(this, clock))
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
      if (rmConf.isLegacyEnabled() && !isEC(c.getReplicationConfig())) {
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
    replicationQueue.set(newRepQueue);
    this.containerReport = report;
    LOG.info("Replication Monitor Thread took {} milliseconds for" +
            " processing {} containers.", clock.millis() - start,
        containers.size());
  }

  public void sendCloseContainerEvent(ContainerID containerID) {
    eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER, containerID);
  }

  /**
   * Returns the maximum number of inflight replications allowed across the
   * cluster at any given time. If zero is returned, there is no limit.
   * @return zero if not limit defined, otherwise the maximum number of
   *         inflight replications allowed across the cluster at any given time.
   */
  public long getReplicationInFlightLimit() {
    final double factor = rmConf.getInflightReplicationLimitFactor();
    if (factor <= 0) {
      return 0;
    }
    // Any healthy node in the cluster can participate in replication by being
    // as source. Eg, even decommissioned hosts can be a source if they are
    // still online. If the host is offline, then it will be quickly stale or
    // dead. Therefore we simply count the number of healthy nodes and include
    // those which are not in service.
    int healthyNodes = nodeManager.getNodeCount(null, HEALTHY);
    return (long) Math.ceil(healthyNodes * rmConf.getDatanodeReplicationLimit()
        * factor);
  }

  /**
   * Returns the number of inflight replications currently in progress across
   * the cluster.
   */
  public long getInflightReplicationCount() {
    return containerReplicaPendingOps
        .getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD);
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
   * Send a delete command with a deadline for the specified container.
   * @param container container to be deleted
   * @param replicaIndex index of the replica to be deleted
   * @param datanode datanode that hosts the replica to be deleted
   * @param force true to force delete a container that is open or not empty
   * @param scmDeadlineEpochMs The epoch time in ms, after which the command
   *                           will be discarded from the SCMPendingOps table.
   * @throws NotLeaderException when this SCM is not the leader
   */
  public void sendDeleteCommand(final ContainerInfo container,
      int replicaIndex, final DatanodeDetails datanode, boolean force,
      long scmDeadlineEpochMs)
      throws NotLeaderException {
    LOG.debug("Sending delete command for container {} and index {} on {} " +
        "with SCM deadline {}.",
        container, replicaIndex, datanode, scmDeadlineEpochMs);

    final DeleteContainerCommand deleteCommand =
        new DeleteContainerCommand(container.containerID(), force);
    deleteCommand.setReplicaIndex(replicaIndex);
    sendDatanodeCommand(deleteCommand, container, datanode,
        scmDeadlineEpochMs);
  }

  /**
   * Sends delete container command for the given container to the given
   * datanode, provided that the datanode is not overloaded with delete
   * container commands. If the datanode is overloaded, an exception will be
   * thrown.
   * @param container Container to be deleted
   * @param replicaIndex Index of the container replica to be deleted
   * @param datanode  The datanode on which the replica should be deleted
   * @param force true to force delete a container that is open or not empty
   * @throws NotLeaderException when this SCM is not the leader
   * @throws CommandTargetOverloadedException If the target datanode is has too
   *                                          many pending commands.
   */
  public void sendThrottledDeleteCommand(final ContainerInfo container,
      int replicaIndex, final DatanodeDetails datanode, boolean force)
      throws NotLeaderException, CommandTargetOverloadedException {
    try {
      int commandCount = nodeManager.getTotalDatanodeCommandCount(datanode,
          Type.deleteContainerCommand);
      int deleteLimit = rmConf.getDatanodeDeleteLimit();
      if (commandCount >= deleteLimit) {
        metrics.incrDeleteContainerCmdsDeferredTotal();
        throw new CommandTargetOverloadedException("Cannot schedule a delete " +
            "container command for container " + container.containerID() +
            " on datanode " + datanode + " as it has too many pending delete " +
            "commands (" + commandCount + " > " + deleteLimit + ")");
      }
      sendDeleteCommand(container, replicaIndex, datanode, force);
    } catch (NodeNotFoundException e) {
      throw new IllegalArgumentException("Datanode " + datanode + " not " +
          "found in NodeManager. Should not happen");
    }
  }

  /**
   * Create a ReplicateContainerCommand for the given container and to push the
   * container to the target datanode. The list of sources are checked to ensure
   * the datanode has sufficient capacity to accept the container command, and
   * then the command is sent to the datanode with the fewest pending commands.
   * If all sources are overloaded, a CommandTargetOverloadedException is
   * thrown.
   * @param containerInfo The container to be replicated
   * @param sources The list of datanodes that can be used as sources
   * @param target The target datanode where the container should be replicated
   * @param replicaIndex The index of the container replica to be replicated
   */
  public void sendThrottledReplicationCommand(ContainerInfo containerInfo,
      List<DatanodeDetails> sources, DatanodeDetails target, int replicaIndex)
      throws CommandTargetOverloadedException, NotLeaderException {
    long containerID = containerInfo.getContainerID();
    List<Pair<Integer, DatanodeDetails>> sourceWithCmds =
        getAvailableDatanodesForReplication(sources);
    if (sourceWithCmds.isEmpty()) {
      metrics.incrReplicateContainerCmdsDeferredTotal();
      throw new CommandTargetOverloadedException("No sources with capacity " +
          "available for replication of container " + containerID + " to " +
          target);
    }
    DatanodeDetails source = selectAndOptionallyExcludeDatanode(
        1, sourceWithCmds);

    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.toTarget(containerID, target);
    cmd.setReplicaIndex(replicaIndex);
    sendDatanodeCommand(cmd, containerInfo, source);
  }

  public void sendThrottledReconstructionCommand(ContainerInfo containerInfo,
      ReconstructECContainersCommand command)
      throws CommandTargetOverloadedException, NotLeaderException {
    List<DatanodeDetails> targets = command.getTargetDatanodes();
    List<Pair<Integer, DatanodeDetails>> targetWithCmds =
        getAvailableDatanodesForReplication(targets);
    if (targetWithCmds.isEmpty()) {
      metrics.incrECReconstructionCmdsDeferredTotal();
      throw new CommandTargetOverloadedException("No target with capacity " +
          "available for reconstruction of " + containerInfo.getContainerID());
    }
    DatanodeDetails target = selectAndOptionallyExcludeDatanode(
        rmConf.getReconstructionCommandWeight(), targetWithCmds);
    sendDatanodeCommand(command, containerInfo, target);
  }

  private DatanodeDetails selectAndOptionallyExcludeDatanode(
      int additionalCmdCount, List<Pair<Integer, DatanodeDetails>> datanodes) {
    if (datanodes.isEmpty()) {
      return null;
    }
    // Put the least loaded datanode first
    datanodes.sort(Comparator.comparingInt(Pair::getLeft));
    DatanodeDetails datanode = datanodes.get(0).getRight();
    int currentCount = datanodes.get(0).getLeft();
    if (currentCount + additionalCmdCount >= getReplicationLimit(datanode)) {
      addExcludedNode(datanode);
    }
    return datanode;
  }

  /**
   * For the given datanodes, lookup the current queued command count for
   * replication and reconstruction and return a list of datanodes with the
   * total queued count which are less than the limit.
   * Any datanode is at or beyond the limit, then it will not be included in the
   * returned list.
   * @param datanodes List of datanodes to check for available capacity
   * @return List of datanodes with the current command count that are not over
   *         the limit.
   */
  private List<Pair<Integer, DatanodeDetails>>
      getAvailableDatanodesForReplication(List<DatanodeDetails> datanodes) {
    List<Pair<Integer, DatanodeDetails>> datanodeWithCommandCount
        = new ArrayList<>();
    for (DatanodeDetails dn : datanodes) {
      try {
        int totalCount = getQueuedReplicationCount(dn);
        int replicationLimit = getReplicationLimit(dn);
        if (totalCount >= replicationLimit) {
          LOG.debug("Datanode {} has reached the maximum of {} queued " +
              "commands for state {}: {}",
              dn, replicationLimit, dn.getPersistedOpState(), totalCount);
          addExcludedNode(dn);
          continue;
        }
        datanodeWithCommandCount.add(Pair.of(totalCount, dn));
      } catch (NodeNotFoundException e) {
        LOG.error("Node {} not found in NodeManager. Should not happen",
            dn, e);
      }
    }
    return datanodeWithCommandCount;
  }

  private int getQueuedReplicationCount(DatanodeDetails datanode)
      throws NodeNotFoundException {
    Map<Type, Integer> counts = nodeManager.getTotalDatanodeCommandCounts(
        datanode, Type.replicateContainerCommand,
        Type.reconstructECContainersCommand);
    int replicateCount = counts.get(Type.replicateContainerCommand);
    int reconstructCount = counts.get(Type.reconstructECContainersCommand);
    return replicateCount +
        reconstructCount * rmConf.getReconstructionCommandWeight();
  }

  /**
   * Send a push replication command to the given source datanode, instructing
   * it to copy the given container to the target. The command is sent as a low
   * priority command, meaning it will only run on the DNs when there are not
   * normal priority commands queued.
   * @param container Container to replicate.
   * @param replicaIndex Replica Index of the container to replicate. Zero for
   *                     Ratis and greater than zero for EC.
   * @param source The source hosting the container, which is where the command
   *               will be sent.
   * @param target The target to push container replica to
   * @param scmDeadlineEpochMs The epoch time in ms, after which the command
   *                           will be discarded from the SCMPendingOps table.
   */
  public void sendLowPriorityReplicateContainerCommand(
      final ContainerInfo container, int replicaIndex, DatanodeDetails source,
      DatanodeDetails target, long scmDeadlineEpochMs)
      throws NotLeaderException {
    final ReplicateContainerCommand command = ReplicateContainerCommand
        .toTarget(container.getContainerID(), target);
    command.setReplicaIndex(replicaIndex);
    command.setPriority(ReplicationCommandPriority.LOW);
    sendDatanodeCommand(command, container, source, scmDeadlineEpochMs);
  }
  /**
   * Sends a command to a datanode with the command deadline set to the default
   * in ReplicationManager config.
   * @param command The command to send.
   * @param containerInfo The container the command is for.
   * @param target The datanode which will receive the command.
   */
  public void sendDatanodeCommand(SCMCommand<?> command,
      ContainerInfo containerInfo, DatanodeDetails target)
      throws NotLeaderException {
    long scmDeadline = clock.millis() + rmConf.eventTimeout;
    sendDatanodeCommand(command, containerInfo, target, scmDeadline);
  }

  /**
   * Sends a command to a datanode with a user defined deadline for the
   * commands.
   * @param command The command to send
   * @param containerInfo The container the command is for.
   * @param target The datanode which will receive the command.
   * @param scmDeadlineEpochMs The epoch time in ms, after which the command
   *                           will be discarded from the SCMPendingOps table.
   */
  public void sendDatanodeCommand(SCMCommand<?> command,
      ContainerInfo containerInfo, DatanodeDetails target,
      long scmDeadlineEpochMs)
      throws NotLeaderException {
    long datanodeDeadline =
        scmDeadlineEpochMs - rmConf.getDatanodeTimeoutOffset();
    LOG.info("Sending command [{}] for container {} to {} with datanode "
        + "deadline {} and scm deadline {}",
        command, containerInfo, target, datanodeDeadline,
        scmDeadlineEpochMs);
    command.setTerm(getScmTerm());
    command.setDeadline(datanodeDeadline);
    nodeManager.addDatanodeCommand(target.getUuid(), command);
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

      if (rcc.getTargetDatanode() == null) {
        /*
        This means the target will pull a replica from a source, so the
        op's target Datanode should be the Datanode this command is being
        sent to.
         */
        containerReplicaPendingOps.scheduleAddReplica(
            containerInfo.containerID(),
            targetDatanode, rcc.getReplicaIndex(), scmDeadlineEpochMs);
      } else {
        /*
        This means the source will push replica to the target, so the op's
        target Datanode should be the Datanode the replica will be pushed to.
         */
        containerReplicaPendingOps.scheduleAddReplica(
            containerInfo.containerID(),
            rcc.getTargetDatanode(), rcc.getReplicaIndex(), scmDeadlineEpochMs);
      }

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

  int processUnderReplicatedContainer(
      final ContainerHealthResult result) throws IOException {
    ContainerID containerID = result.getContainerInfo().containerID();
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(
        containerID);
    List<ContainerReplicaOp> pendingOps =
        containerReplicaPendingOps.getPendingOps(containerID);

    final boolean isEC = isEC(result.getContainerInfo().getReplicationConfig());
    final UnhealthyReplicationHandler handler;

    if (result.getHealthState()
        == ContainerHealthResult.HealthState.UNDER_REPLICATED) {
      handler = isEC ? ecUnderReplicationHandler
          : ratisUnderReplicationHandler;
    } else if (result.getHealthState()
        == ContainerHealthResult.HealthState.MIS_REPLICATED) {
      handler = isEC ? ecMisReplicationHandler : ratisMisReplicationHandler;
    } else {
      throw new IllegalArgumentException("Unexpected health state: "
          + result.getHealthState());
    }

    return handler.processAndSendCommands(replicas, pendingOps, result,
        getRemainingMaintenanceRedundancy(isEC));
  }

  int processOverReplicatedContainer(
      final ContainerHealthResult result) throws IOException {
    ContainerID containerID = result.getContainerInfo().containerID();
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(
        containerID);
    List<ContainerReplicaOp> pendingOps =
        containerReplicaPendingOps.getPendingOps(containerID);

    final boolean isEC = isEC(result.getContainerInfo().getReplicationConfig());
    final UnhealthyReplicationHandler handler = isEC ? ecOverReplicationHandler
        : ratisOverReplicationHandler;

    return handler.processAndSendCommands(replicas,
          pendingOps, result, getRemainingMaintenanceRedundancy(isEC));
  }

  public long getScmTerm() throws NotLeaderException {
    return scmContext.getTermOfLeader();
  }

  /**
   * Notify ReplicationManager that the command counts on a datanode have been
   * updated via a heartbeat received. This will allow RM to consider the node
   * for container operations if it was previously excluded due to load.
   * @param datanode The datanode for which the commands have been updated.
   */
  public void datanodeCommandCountUpdated(DatanodeDetails datanode) {
    LOG.debug("Received a notification that the DN command count " +
        "has been updated for {}", datanode);
    // If there is an existing mapping, we may need to remove it
    excludedNodes.computeIfPresent(datanode, (dn, v) -> {
      try {
        if (getQueuedReplicationCount(dn) < getReplicationLimit(dn)) {
          // Returning null removes the entry from the map
          return null;
        } else {
          return 1;
        }
      } catch (NodeNotFoundException e) {
        LOG.warn("Unable to find datanode {} in nodeManager. " +
            "Should not happen.", datanode);
        return null;
      }
    });
  }

  /**
   * Returns the list of datanodes that are currently excluded from being
   * targets for container replication due to queued commands.
   * @return Set of excluded DatanodeDetails.
   */
  public Set<DatanodeDetails> getExcludedNodes() {
    return excludedNodes.keySet();
  }

  private void addExcludedNode(DatanodeDetails dn) {
    excludedNodes.put(dn, 1);
  }

  protected void processContainer(ContainerInfo containerInfo,
      ReplicationQueue repQueue, ReplicationManagerReport report)
      throws ContainerNotFoundException {
    synchronized (containerInfo) {
      ContainerID containerID = containerInfo.containerID();
      final boolean isEC = isEC(containerInfo.getReplicationConfig());

      Set<ContainerReplica> replicas = containerManager.getContainerReplicas(
          containerID);
      List<ContainerReplicaOp> pendingOps =
          containerReplicaPendingOps.getPendingOps(containerID);

      ContainerCheckRequest checkRequest = new ContainerCheckRequest.Builder()
          .setContainerInfo(containerInfo)
          .setContainerReplicas(replicas)
          .setMaintenanceRedundancy(getRemainingMaintenanceRedundancy(isEC))
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
        wait(rmConf.getInterval().toMillis());
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
   */
  public ContainerReplicaCount getContainerReplicaCount(ContainerID containerID)
      throws ContainerNotFoundException {
    ContainerInfo container = containerManager.getContainer(containerID);
    final boolean isEC = isEC(container.getReplicationConfig());

    if (!isEC && rmConf.isLegacyEnabled()) {
      return legacyReplicationManager.getContainerReplicaCount(container);
    } else {
      return getContainerReplicaCount(container, isEC);
    }
  }

  /**
   * For a given container and a set of replicas, check the container's
   * replication health and return the health status.
   * @param containerInfo The container to check
   * @param replicas The set of replicas to use to check for the check
   */
  public ContainerHealthResult getContainerReplicationHealth(
      ContainerInfo containerInfo, Set<ContainerReplica> replicas) {
    final boolean isEC = isEC(containerInfo.getReplicationConfig());
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setContainerInfo(containerInfo)
        .setContainerReplicas(replicas)
        .setPendingOps(getPendingReplicationOps(containerInfo.containerID()))
        .setMaintenanceRedundancy(getRemainingMaintenanceRedundancy(isEC))
        .build();

    if (isEC) {
      return ecReplicationCheckHandler.checkHealth(request);
    } else {
      return ratisReplicationCheckHandler.checkHealth(request);
    }
  }

  /**
   * Retrieve a list of any pending container replications or deletes for the
   * given containerID.
   * @param containerID The containerID to retrieve the pending ops for.
   * @return A list of ContainerReplicaOp for the container, or an empty list if
   *         there are none.
   */
  public List<ContainerReplicaOp> getPendingReplicationOps(
      ContainerID containerID) {
    return containerReplicaPendingOps.getPendingOps(containerID);
  }

  /**
   * Queries the NodeManager for the NodeStatus of the given node.
   * @param datanode The datanode for which to retrieve the NodeStatus.
   * @return The NodeStatus of the requested Node.
   * @throws NodeNotFoundException If the node is not registered with SCM.
   */
  public NodeStatus getNodeStatus(DatanodeDetails datanode)
      throws NodeNotFoundException {
    return nodeManager.getNodeStatus(datanode);
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

  ReplicationQueue getQueue() {
    return replicationQueue.get();
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
        defaultValue = "false",
        tags = {SCM, OZONE},
        description =
            "If true, LegacyReplicationManager will handle RATIS containers " +
            "while ReplicationManager will handle EC containers. If false, " +
            "ReplicationManager will handle both RATIS and EC."
    )
    private boolean enableLegacy;

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
        defaultValue = "10m",
        tags = {SCM, OZONE},
        description = "Timeout for the container replication/deletion commands "
            + "sent to datanodes. After this timeout the command will be "
            + "retried.")
    private long eventTimeout = Duration.ofMinutes(10).toMillis();
    public void setInterval(Duration interval) {
      this.interval = interval.toMillis();
    }

    public void setEventTimeout(Duration timeout) {
      this.eventTimeout = timeout.toMillis();
    }

    /**
     * When a command has a deadline in SCM, the datanode timeout should be
     * slightly less. This duration is the number of seconds to subtract from
     * the SCM deadline to give a datanode deadline.
     */
    @Config(key = "event.timeout.datanode.offset",
        type = ConfigType.TIME,
        defaultValue = "30s",
        tags = {SCM, OZONE},
        description = "The amount of time to subtract from "
            + "hdds.scm.replication.event.timeout to give a deadline on the "
            + "datanodes which is less than the SCM timeout. This ensures "
            + "the datanodes will not process a command after SCM believes it "
            + "should have expired.")
    private long datanodeTimeoutOffset = Duration.ofSeconds(30).toMillis();
    public long getDatanodeTimeoutOffset() {
      return datanodeTimeoutOffset;
    }

    public void setDatanodeTimeoutOffset(long val) {
      datanodeTimeoutOffset = val;
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
        defaultValue = "true",
        tags = { SCM, DATANODE },
        description = "If false, replication happens by asking the target to " +
            "pull from source nodes.  If true, the source node is asked to " +
            "push to the target node."
    )
    private boolean push = true;

    @Config(key = "datanode.replication.limit",
        type = ConfigType.INT,
        defaultValue = "20",
        tags = { SCM, DATANODE },
        description = "A limit to restrict the total number of replication " +
            "and reconstruction commands queued on a datanode. Note this is " +
            "intended to be a temporary config until we have a more dynamic " +
            "way of limiting load."
    )
    private int datanodeReplicationLimit = 20;

    public int getDatanodeReplicationLimit() {
      return datanodeReplicationLimit;
    }

    @Config(key = "datanode.reconstruction.weight",
        type = ConfigType.INT,
        defaultValue = "3",
        tags = { SCM, DATANODE },
        description = "When counting the number of replication commands on a " +
            "datanode, the number of reconstruction commands is multiplied " +
            "by this weight to ensure reconstruction commands use more of " +
            "the capacity, as they are more expensive to process."
    )
    private int reconstructionCommandWeight = 3;

    public int getReconstructionCommandWeight() {
      return reconstructionCommandWeight;
    }

    @Config(key = "datanode.delete.container.limit",
        type = ConfigType.INT,
        defaultValue = "40",
        tags = { SCM, DATANODE },
        description = "A limit to restrict the total number of delete " +
            "container commands queued on a datanode. Note this is intended " +
            "to be a temporary config until we have a more dynamic way of " +
            "limiting load"
    )
    private int datanodeDeleteLimit = 40;

    public int getDatanodeDeleteLimit() {
      return datanodeDeleteLimit;
    }

    @Config(key = "inflight.limit.factor",
        type = ConfigType.DOUBLE,
        defaultValue = "0.75",
        tags = { SCM },
        description = "The overall replication task limit on a cluster is the" +
            " number healthy nodes, times the datanode.replication.limit." +
            " This factor, which should be between zero and 1, scales that" +
            " limit down to reduce the overall number of replicas pending" +
            " creation on the cluster. A setting of zero disables global" +
            " limit checking. A setting of 1 effectively disables it, by" +
            " making the limit equal to the above equation. However if there" +
            " are many decommissioning nodes on the cluster, the decommission" +
            " nodes will have a higher than normal limit, so the setting of 1" +
            " may still provide some limit in extreme circumstances."
    )
    private double inflightReplicationLimitFactor = 0.75;

    public double getInflightReplicationLimitFactor() {
      return inflightReplicationLimitFactor;
    }

    public void setInflightReplicationLimitFactor(double factor) {
      this.inflightReplicationLimitFactor = factor;
    }

    public void setDatanodeReplicationLimit(int limit) {
      this.datanodeReplicationLimit = limit;
    }

    public void setMaintenanceRemainingRedundancy(int redundancy) {
      this.maintenanceRemainingRedundancy = redundancy;
    }

    public int getMaintenanceRemainingRedundancy() {
      return maintenanceRemainingRedundancy;
    }

    public Duration getInterval() {
      return Duration.ofMillis(interval);
    }

    public Duration getUnderReplicatedInterval() {
      return Duration.ofMillis(underReplicatedInterval);
    }

    public void setUnderReplicatedInterval(Duration duration) {
      this.underReplicatedInterval = duration.toMillis();
    }

    public void setOverReplicatedInterval(Duration duration) {
      this.overReplicatedInterval = duration.toMillis();
    }

    public Duration getOverReplicatedInterval() {
      return Duration.ofMillis(overReplicatedInterval);
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

    @PostConstruct
    public void validate() {
      if (datanodeTimeoutOffset < 0) {
        throw new IllegalArgumentException("event.timeout.datanode.offset is"
            + " set to " + datanodeTimeoutOffset + " and must be >= 0");
      }
      if (datanodeTimeoutOffset >= eventTimeout) {
        throw new IllegalArgumentException("event.timeout.datanode.offset is"
            + " set to " + datanodeTimeoutOffset + " and must be <"
            + " event.timeout, which is set to " + eventTimeout);
      }
      if (reconstructionCommandWeight <= 0) {
        throw new IllegalArgumentException("reconstructionCommandWeight is"
            + " set to " + reconstructionCommandWeight + " and must be > 0");
      }
      if (inflightReplicationLimitFactor < 0) {
        throw new IllegalArgumentException(
            "inflight.limit.factor is set to " + inflightReplicationLimitFactor
                + " and must be >= 0");
      }
      if (inflightReplicationLimitFactor > 1) {
        throw new IllegalArgumentException(
            "inflight.limit.factor is set to " + inflightReplicationLimitFactor
                + " and must be <= 1");
      }
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
          // It this SCM was previously a leader and transitioned to a follower
          // and then back to a leader in a short time, there may be old pending
          // Ops in the ContainerReplicaPendingOps table. They are no longer
          // needed as the DN will discard any commands when the term changes.
          // Therefore we should clear the table so RM starts from a clean
          // state.
          containerReplicaPendingOps.clear();
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

  @SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
  public ReplicationManagerMetrics getMetrics() {
    return metrics;
  }

  public ReplicationManagerConfiguration getConfig() {
    return rmConf;
  }

  public Clock getClock() {
    return clock;
  }

  /**
  * following functions will be refactored in a separate jira.
  */
  public CompletableFuture<MoveManager.MoveResult> move(
      ContainerID cid, DatanodeDetails src, DatanodeDetails tgt)
      throws NodeNotFoundException, ContainerNotFoundException,
      TimeoutException {
    CompletableFuture<MoveManager.MoveResult> ret =
        new CompletableFuture<>();
    if (!isRunning()) {
      ret.complete(MoveManager.MoveResult.FAIL_UNEXPECTED_ERROR);
      LOG.warn("Failing move because Replication Monitor thread's " +
          "running state is {}", isRunning());
      return ret;
    }

    return legacyReplicationManager.move(cid, src, tgt);
  }

  public Map<ContainerID, CompletableFuture<MoveManager.MoveResult>>
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
    if (rmConf.isLegacyEnabled()) {
      return legacyReplicationManager
          .isContainerReplicatingOrDeleting(containerID);
    } else {
      return !getPendingReplicationOps(containerID).isEmpty();
    }
  }

  private ContainerReplicaCount getContainerReplicaCount(
      ContainerInfo container, boolean isEC) throws ContainerNotFoundException {

    ContainerID id = container.containerID();
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(id);
    List<ContainerReplicaOp> pendingOps =
        containerReplicaPendingOps.getPendingOps(id);
    final int redundancy = getRemainingMaintenanceRedundancy(isEC);

    return isEC
        ? new ECContainerReplicaCount(container, replicas, pendingOps,
            redundancy)
        : new RatisContainerReplicaCount(container, replicas, pendingOps,
            redundancy, false);
  }
  
  public ContainerReplicaPendingOps getContainerReplicaPendingOps() {
    return containerReplicaPendingOps;
  }

  private int getReplicationLimit(DatanodeDetails datanode) {
    HddsProtos.NodeOperationalState state = datanode.getPersistedOpState();
    int limit = rmConf.getDatanodeReplicationLimit();
    if (isMaintenance(state) || isDecommission(state)) {
      limit = replicationServerConf.scaleOutOfServiceLimit(limit);
    }
    return limit;
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

  private int getRemainingMaintenanceRedundancy(boolean isEC) {
    return isEC
        ? rmConf.getMaintenanceRemainingRedundancy()
        : rmConf.getMaintenanceReplicaMinimum();
  }

  private static boolean isEC(ReplicationConfig replicationConfig) {
    return replicationConfig.getReplicationType() == EC;
  }
}

