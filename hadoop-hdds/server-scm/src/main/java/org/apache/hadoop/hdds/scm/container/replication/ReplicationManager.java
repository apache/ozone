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
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport.HealthState;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.ExitUtil;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;
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
  private final ContainerHealthCheck ecContainerHealthCheck;
  private final EventPublisher eventPublisher;
  private final ReentrantLock lock = new ReentrantLock();
  private Queue<ContainerHealthResult.UnderReplicatedHealthResult>
      underRepQueue;
  private Queue<ContainerHealthResult.OverReplicatedHealthResult>
      overRepQueue;
  private final ECUnderReplicationHandler ecUnderReplicationHandler;
  private final ECOverReplicationHandler ecOverReplicationHandler;
  private final int maintenanceRedundancy;

  /**
   * Constructs ReplicationManager instance with the given configuration.
   *
   * @param conf OzoneConfiguration
   * @param containerManager ContainerManager
   * @param containerPlacement PlacementPolicy
   * @param eventPublisher EventPublisher
   */
  @SuppressWarnings("parameternumber")
  public ReplicationManager(final ConfigurationSource conf,
             final ContainerManager containerManager,
             final PlacementPolicy containerPlacement,
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
    this.ecContainerHealthCheck = new ECContainerHealthCheck();
    this.nodeManager = nodeManager;
    this.underRepQueue = createUnderReplicatedQueue();
    this.overRepQueue = new LinkedList<>();
    this.maintenanceRedundancy = rmConf.maintenanceRemainingRedundancy;
    ecUnderReplicationHandler = new ECUnderReplicationHandler(
        containerPlacement, conf, nodeManager);
    ecOverReplicationHandler =
        new ECOverReplicationHandler(containerPlacement, nodeManager);
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
      legacyReplicationManager.setMetrics(metrics);
      replicationMonitor = new Thread(this::run);
      replicationMonitor.setName("ReplicationMonitor");
      replicationMonitor.setDaemon(true);
      replicationMonitor.start();
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
      running = false;
      legacyReplicationManager.clearInflightActions();
      metrics.unRegister();
      replicationMonitor.interrupt();
    } else {
      LOG.info("Replication Monitor Thread is not running.");
    }
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
    Queue<ContainerHealthResult.UnderReplicatedHealthResult>
        underReplicated = createUnderReplicatedQueue();
    Queue<ContainerHealthResult.OverReplicatedHealthResult> overReplicated =
        new LinkedList<>();

    for (ContainerInfo c : containers) {
      if (!shouldRun()) {
        break;
      }
      report.increment(c.getState());
      if (c.getReplicationType() != EC) {
        legacyReplicationManager.processContainer(c, report);
        continue;
      }
      try {
        processContainer(c, underReplicated, overReplicated, report);
        // TODO - send any commands contained in the health result
      } catch (ContainerNotFoundException e) {
        LOG.error("Container {} not found", c.getContainerID(), e);
      }
    }
    report.setComplete();
    lock.lock();
    try {
      underRepQueue = underReplicated;
      overRepQueue = overReplicated;
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
      return underRepQueue.poll();
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
      return overRepQueue.poll();
    } finally {
      lock.unlock();
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
      underRepQueue.add(underReplicatedHealthResult);
    } finally {
      lock.unlock();
    }
  }

  public void requeueOverReplicatedContainer(ContainerHealthResult
      .OverReplicatedHealthResult overReplicatedHealthResult) {
    lock.lock();
    try {
      overRepQueue.add(overReplicatedHealthResult);
    } finally {
      lock.unlock();
    }
  }

  public Map<DatanodeDetails, SCMCommand<?>> processUnderReplicatedContainer(
      final ContainerHealthResult result) throws IOException {
    ContainerID containerID = result.getContainerInfo().containerID();
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(
        containerID);
    List<ContainerReplicaOp> pendingOps =
        containerReplicaPendingOps.getPendingOps(containerID);
    return ecUnderReplicationHandler.processAndCreateCommands(replicas,
        pendingOps, result, maintenanceRedundancy);
  }

  public Map<DatanodeDetails, SCMCommand<?>> processOverReplicatedContainer(
      final ContainerHealthResult result) throws IOException {
    ContainerID containerID = result.getContainerInfo().containerID();
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(
        containerID);
    List<ContainerReplicaOp> pendingOps =
        containerReplicaPendingOps.getPendingOps(containerID);
    return ecOverReplicationHandler.processAndCreateCommands(replicas,
        pendingOps, result, maintenanceRedundancy);
  }

  public long getScmTerm() throws NotLeaderException {
    return scmContext.getTermOfLeader();
  }

  protected ContainerHealthResult processContainer(ContainerInfo containerInfo,
      Queue<ContainerHealthResult.UnderReplicatedHealthResult> underRep,
      Queue<ContainerHealthResult.OverReplicatedHealthResult> overRep,
      ReplicationManagerReport report) throws ContainerNotFoundException {

    ContainerID containerID = containerInfo.containerID();
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(
        containerID);

    if (containerInfo.getState() == HddsProtos.LifeCycleState.OPEN) {
      if (!isOpenContainerHealthy(containerInfo, replicas)) {
        report.incrementAndSample(
            HealthState.OPEN_UNHEALTHY, containerID);
        eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER, containerID);
        return new ContainerHealthResult.UnHealthyResult(containerInfo);
      }
      return new ContainerHealthResult.HealthyResult(containerInfo);
    }

    if (containerInfo.getState() == HddsProtos.LifeCycleState.CLOSED) {
      List<ContainerReplica> unhealthyReplicas = replicas.stream()
          .filter(r -> !compareState(containerInfo.getState(), r.getState()))
          .collect(Collectors.toList());

      if (unhealthyReplicas.size() > 0) {
        handleUnhealthyReplicas(containerInfo, unhealthyReplicas);
      }
    }

    List<ContainerReplicaOp> pendingOps =
        containerReplicaPendingOps.getPendingOps(containerID);
    ContainerHealthResult health = ecContainerHealthCheck.checkHealth(
        containerInfo, replicas, pendingOps, maintenanceRedundancy);
      // TODO - should the report have a HEALTHY state, rather than just bad
      //        states? It would need to be added to legacy RM too.
    if (health.getHealthState()
        == ContainerHealthResult.HealthState.UNDER_REPLICATED) {
      report.incrementAndSample(HealthState.UNDER_REPLICATED, containerID);
      ContainerHealthResult.UnderReplicatedHealthResult underHealth
          = ((ContainerHealthResult.UnderReplicatedHealthResult) health);
      if (underHealth.isUnrecoverable()) {
        // TODO - do we need a new health state for unrecoverable EC?
        report.incrementAndSample(HealthState.MISSING, containerID);
      }
      if (!underHealth.isSufficientlyReplicatedAfterPending() &&
          !underHealth.isUnrecoverable()) {
        underRep.add(underHealth);
      }
    } else if (health.getHealthState()
        == ContainerHealthResult.HealthState.OVER_REPLICATED) {
      report.incrementAndSample(HealthState.OVER_REPLICATED, containerID);
      ContainerHealthResult.OverReplicatedHealthResult overHealth
          = ((ContainerHealthResult.OverReplicatedHealthResult) health);
      if (!overHealth.isSufficientlyReplicatedAfterPending()) {
        overRep.add(overHealth);
      }
    }
    return health;
  }

  /**
   * Handles unhealthy container.
   * A container is inconsistent if any of the replica state doesn't
   * match the container state. We have to take appropriate action
   * based on state of the replica.
   *
   * @param container ContainerInfo
   * @param unhealthyReplicas List of ContainerReplica
   */
  private void handleUnhealthyReplicas(final ContainerInfo container,
      List<ContainerReplica> unhealthyReplicas) {
    Iterator<ContainerReplica> iterator = unhealthyReplicas.iterator();
    while (iterator.hasNext()) {
      final ContainerReplica replica = iterator.next();
      final ContainerReplicaProto.State state = replica.getState();
      if (state == State.OPEN || state == State.CLOSING) {
        sendCloseCommand(container, replica.getDatanodeDetails(), true);
        iterator.remove();
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
  private void sendCloseCommand(final ContainerInfo container,
      final DatanodeDetails datanode, final boolean force) {

    ContainerID containerID = container.containerID();
    LOG.info("Sending close container command for container {}" +
        " to datanode {}.", containerID, datanode);
    CloseContainerCommand closeContainerCommand =
        new CloseContainerCommand(container.getContainerID(),
            container.getPipelineID(), force);
    try {
      closeContainerCommand.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending close container command,"
          + " since current SCM is not leader.", nle);
      return;
    }
    closeContainerCommand.setEncodedToken(getContainerToken(containerID));
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
        new CommandForDatanode<>(datanode.getUuid(), closeContainerCommand));
  }

  private String getContainerToken(ContainerID containerID) {
    if (scmContext.getScm() instanceof StorageContainerManager) {
      StorageContainerManager scm =
          (StorageContainerManager) scmContext.getScm();
      return scm.getContainerTokenGenerator().generateEncodedToken(containerID);
    }
    return ""; // unit test
  }

  /**
   * Creates a priority queue of UnderReplicatedHealthResult, where the elements
   * are ordered by the weighted redundancy of the container. This means that
   * containers with the least remaining redundancy are at the front of the
   * queue, and will be processed first.
   * @return An empty instance of a PriorityQueue.
   */
  protected PriorityQueue<ContainerHealthResult.UnderReplicatedHealthResult>
      createUnderReplicatedQueue() {
    return new PriorityQueue<>(Comparator.comparing(ContainerHealthResult
            .UnderReplicatedHealthResult::getWeightedRedundancy)
        .thenComparing(ContainerHealthResult
            .UnderReplicatedHealthResult::getRequeueCount));
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
    return legacyReplicationManager.getContainerReplicaCount(container);
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
        //now, as the current scm is leader and it`s state is up-to-date,
        //we need to take some action about replicated inflight move options.
        legacyReplicationManager.notifyStatusChanged();
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

  public ReplicationManagerMetrics getMetrics() {
    return metrics;
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

