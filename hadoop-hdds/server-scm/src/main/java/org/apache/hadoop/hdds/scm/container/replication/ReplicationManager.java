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
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.util.ExitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;

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
             final SCMServiceManager serviceManager,
             final NodeManager nodeManager,
             final Clock clock,
             final SCMHAManager scmhaManager,
             final Table<ContainerID, MoveDataNodePair> moveTable)
             throws IOException {
    this.containerManager = containerManager;
    this.scmContext = scmContext;
    this.rmConf = conf.getObject(ReplicationManagerConfiguration.class);
    this.running = false;
    this.clock = clock;
    this.containerReport = new ReplicationManagerReport();
    this.metrics = null;
    this.waitTimeInMillis = conf.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.legacyReplicationManager = new LegacyReplicationManager(
        conf, containerManager, containerPlacement, eventPublisher,
        scmContext, nodeManager, scmhaManager, clock, moveTable);

    // register ReplicationManager to SCMServiceManager.
    serviceManager.register(this);

    // start ReplicationManager.
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
    for (ContainerInfo c : containers) {
      if (!shouldRun()) {
        break;
      }
      switch (c.getReplicationType()) {
      case EC:
        break;
      default:
        legacyReplicationManager.processContainer(c, report);
      }
    }
    report.setComplete();
    this.containerReport = report;
    LOG.info("Replication Monitor Thread took {} milliseconds for" +
            " processing {} containers.", clock.millis() - start,
        containers.size());
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
    return legacyReplicationManager.getContainerReplicaCount(containerID);
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

    public long getInterval() {
      return interval;
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
  * following functions will be refactored in a seperate jira.
  */
  public CompletableFuture<LegacyReplicationManager.MoveResult> move(
      ContainerID cid, DatanodeDetails src, DatanodeDetails tgt)
      throws NodeNotFoundException, ContainerNotFoundException {
    CompletableFuture<LegacyReplicationManager.MoveResult> ret =
        new CompletableFuture<>();
    if (!isRunning()) {
      ret.complete(LegacyReplicationManager.MoveResult.FAIL_NOT_RUNNING);
      return ret;
    }

    return legacyReplicationManager.move(cid, src, tgt);
  }

  public Map<ContainerID, List<InflightAction>>  getInflightReplication() {
    return legacyReplicationManager.getInflightReplication();
  }

  public Map<ContainerID, List<InflightAction>> getInflightDeletion() {
    return legacyReplicationManager.getInflightDeletion();
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
}

