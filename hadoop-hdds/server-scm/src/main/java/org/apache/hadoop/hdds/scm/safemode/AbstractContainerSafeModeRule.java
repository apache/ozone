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

package org.apache.hadoop.hdds.scm.safemode;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_CONTAINER_RULE_REFRESH_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_CONTAINER_RULE_REFRESH_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;

/**
 * Abstract class for Container Safe mode exit rule.
 */
public abstract class AbstractContainerSafeModeRule extends SafeModeExitRule<NodeRegistrationContainerReport> {

  private final ContainerManager containerManager;
  private final Map<ContainerID, Integer> containers = new ConcurrentHashMap<>();
  private final double safeModeCutoff;
  private final AtomicInteger totalContainers = new AtomicInteger();
  private final AtomicInteger containersWithMinReplicas = new AtomicInteger();
  private final Map<ContainerID, ContainerID> openContainers = new ConcurrentHashMap<>();
  private final Map<ContainerID, ContainerID> closedContainers = new ConcurrentHashMap<>();
  private final Map<ContainerID, ContainerID> processedContainers = new ConcurrentHashMap<>();

  private final long refreshInterval;
  private volatile ScheduledExecutorService refreshExecutor;
  public AbstractContainerSafeModeRule(ConfigurationSource conf, SCMSafeModeManager safeModeManager,
      ContainerManager containerManager, EventQueue eventQueue) {
    super(safeModeManager, eventQueue);
    this.containerManager = containerManager;
    this.safeModeCutoff = getSafeModeCutoff(conf);
    this.refreshInterval = conf.getTimeDuration(
        HDDS_SCM_SAFEMODE_CONTAINER_RULE_REFRESH_INTERVAL,
        HDDS_SCM_SAFEMODE_CONTAINER_RULE_REFRESH_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    initializeRule();
    startRefreshExecutor();
  }

  public ContainerManager getContainerManager() {
    return containerManager;
  }

  public Map<ContainerID, ContainerID> getClosedContainers() {
    return closedContainers;
  }

  public Map<ContainerID, ContainerID> getOpenContainers() {
    return openContainers;
  }

  public Map<ContainerID, ContainerID> getProcessedContainers() {
    return processedContainers;
  }

  void incrementTotalContainers() {
    totalContainers.getAndIncrement();
  }

  protected abstract ReplicationType getContainerType();

  protected abstract void handleReportedContainer(ContainerID containerID, DatanodeID datanodeID);

  protected long getNumberOfContainersWithMinReplica() {
    return containersWithMinReplicas.get();
  }

  protected final void incrementContainersWithMinReplicas() {
    containersWithMinReplicas.incrementAndGet();
  }

  protected void initializeRule() {
    containers.clear();
    openContainers.clear();
    closedContainers.clear();
    processedContainers.clear();
    containerManager.getContainers(getContainerType()).stream()
        .filter(c -> c.getNumberOfKeys() > 0)
        .forEach(c -> {
              if (isClosed(c)) {
                containers.put(c.containerID(), c.getReplicationConfig().getMinimumNodes());
                closedContainers.put(c.containerID(), c.containerID());
              }
              if (isOpen(c)) {
                openContainers.put(c.containerID(),c.containerID());
              }
            }
        );
    totalContainers.set(containers.size());
    final long cutOff = (long) Math.ceil(getTotalNumberOfContainers() * getSafeModeCutoff());
    getSafeModeMetrics().setNumContainerReportedThreshold(getContainerType(), cutOff);
    SCMSafeModeManager.getLogger().info("Refreshed {} Containers threshold count to {}.", getContainerType(), cutOff);
  }

  protected Map<ContainerID, Integer> getContainers() {
    return containers;
  }

  protected int getTotalNumberOfContainers() {
    return totalContainers.get();
  }

  protected void addContainer(ContainerInfo containerInfo) {
    if (containers.putIfAbsent(containerInfo.containerID(), containerInfo.getReplicationConfig().getMinimumNodes()) == null) {
      totalContainers.getAndIncrement();
    }
  }

  protected void removeContainer(ContainerInfo containerInfo) {
    if (containers.remove(containerInfo.containerID()) != null) {
      totalContainers.getAndDecrement();
    }
  }

  protected double getSafeModeCutoff() {
    return safeModeCutoff;
  }

  @Override
  protected TypedEvent<NodeRegistrationContainerReport> getEventType() {
    return SCMEvents.CONTAINER_REGISTRATION_REPORT;
  }

  @Override
  protected void process(NodeRegistrationContainerReport report) {
    final DatanodeID datanodeID = report.getDatanodeDetails().getID();
    report.getReport().getReportsList().stream()
        .map(c -> ContainerID.valueOf(c.getContainerID()))
        .forEach(cid -> handleReportedContainer(cid, datanodeID));

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. {} % containers [{}] have at least one reported replica",
          getContainerType(), String.format("%.2f", getCurrentContainerThreshold() * 100));
    }
  }

  @Override
  protected synchronized boolean validate() {
    if (validateBasedOnReportProcessing()) {
      return getCurrentContainerThreshold() >= getSafeModeCutoff();
    }

    final List<ContainerInfo> containerInfos = containerManager.getContainers(getContainerType());
    return containerInfos.stream()
        .filter(this::isClosed)
        .map(ContainerInfo::containerID)
        .noneMatch(this::isMissing);
  }

  @VisibleForTesting
  public double getCurrentContainerThreshold() {
    final long total = getTotalNumberOfContainers();
    return total == 0 ? 1 : ((double) getNumberOfContainersWithMinReplica() / total);
  }



  private void startRefreshExecutor() {
    if (refreshInterval <= 0) {
      SCMSafeModeManager.getLogger().info(
          "Container safe mode rule incremental sync is disabled ({}=0).",
          HDDS_SCM_SAFEMODE_CONTAINER_RULE_REFRESH_INTERVAL);
      return;
    }
    refreshExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(
                "ContainerSafeModeRule-" + getContainerType() + "-refresh-%d")
            .build());
    refreshExecutor.scheduleAtFixedRate(
        this::runRefresh,
        refreshInterval,
        refreshInterval,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Background task: reconcile open/closed container tracking with
   * {@link ContainerManager} while SCM is in safe mode.
   */
  private void runRefresh() {
    synchronized (this) {
      if (!scmInSafeMode()) {
        return;
      }
      refreshExpectedContainers();
    }
  }

  @Override
  public synchronized void refresh(boolean forceRefresh) {
    if (forceRefresh) {
      initializeRule();
    }
  }

  /**
   * Aligns tracked open/closed sets with current {@link ContainerManager} state.
   * Runs on the incremental sync thread when configured.
   */
  private void refreshExpectedContainers() {
    if (getSafeModeManager().isScmRatisApplyCaughtUpToCommit()) {
      return;
    }
    if (!validate()) {
      // iterate through open containers and check if any of them have moved to closed state
      for(ContainerID containerID : openContainers.keySet()) {
        try {
        ContainerInfo containerInfo = containerManager.getContainer(containerID);
        if (isClosed(containerInfo)) {
          addContainer(containerInfo);
          openContainers.remove(containerID);
          closedContainers.put(containerID, containerID);
        } } catch (ContainerNotFoundException e) {
          SCMSafeModeManager.getLogger().debug(
              "Container {} not found while checking open-to-closed transition, may be transient",
              containerID);
        }
      }
      // iterate through closed containers and check if any of them have moved to deleted state
      for(ContainerID containerID : closedContainers.keySet()) {
        try {
        ContainerInfo containerInfo = containerManager.getContainer(containerID);
        if (isDeleted(containerInfo)) {
          removeContainer(containerInfo);
          closedContainers.remove(containerID);
        }} catch (ContainerNotFoundException e) {
          SCMSafeModeManager.getLogger().debug(
              "Container {} not found while checking closed-to-deleted transition, may be transient",
              containerID);
        }
      }
    }
  }


  @Override
  protected void cleanup() {
    stopIncrementalSyncExecutor();
    synchronized (this) {
      if (containers != null) {
        containers.clear();
      }
      if (openContainers != null) {
        openContainers.clear();
      }
      if (closedContainers != null) {
        closedContainers.clear();
      }
      if (totalContainers != null) {
        totalContainers.set(0);
      }
      if (processedContainers != null) {
        processedContainers.clear();
      }
    }
  }

  private void stopIncrementalSyncExecutor() {
    if (refreshExecutor != null) {
      refreshExecutor.shutdownNow();
      try {
        refreshExecutor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      refreshExecutor = null;
    }
  }

  @VisibleForTesting
  void runIncrementalContainerSyncForTesting() {
    runRefresh();
  }

  /**
   * Checks if the container has at least the minimum required number of replicas.
   */
  protected boolean isMissing(ContainerID id) {
    try {
      int minReplica = getMinReplica(id);
      return containerManager.getContainerReplicas(id).size() < minReplica;
    } catch (ContainerNotFoundException ex) {
      /*
       * This should never happen; in case this happens, the container somehow got removed from SCM.
       * Safemode rule doesn't have to log/fix this. We will just exclude this
       * from the rule validation.
       */
      return false;
    }
  }

  protected boolean isClosed(ContainerInfo container) {
    final LifeCycleState state = container.getState();
    return state == LifeCycleState.QUASI_CLOSED || state == LifeCycleState.CLOSED;
  }

  protected boolean isOpen(ContainerInfo container) {
    final LifeCycleState state = container.getState();
    // should we include CLOSING?
    return state == LifeCycleState.OPEN;
  }

  private boolean isDeleted(ContainerInfo container) {
    final LifeCycleState state = container.getState();
    // should we include DELETING ?
    return state == LifeCycleState.DELETED;
  }

  protected int getMinReplica(ContainerID id) {
    return containers.getOrDefault(id, 0);
  }

  @Override
  public String getStatusText() {
    String status = String.format("%1.2f%% of [" + getContainerType() + "] " +
            "Containers(%s / %s) with at least N reported replica (=%1.2f) >= " +
            "safeModeCutoff (=%1.2f)",
        getCurrentContainerThreshold() * 100,
        getNumberOfContainersWithMinReplica(), getTotalNumberOfContainers(),
        getCurrentContainerThreshold(), getSafeModeCutoff());

    final List<ContainerID> sampleContainers = getContainers().keySet().stream()
        .limit(SAMPLE_CONTAINER_DISPLAY_LIMIT)
        .collect(Collectors.toList());

    if (!sampleContainers.isEmpty()) {
      String sampleECContainerText = "Sample  " + getContainerType() + " Containers not satisfying the criteria : "
          + sampleContainers;
      status = status.concat("\n").concat(sampleECContainerText);
    }

    return status;
  }

  private static double getSafeModeCutoff(ConfigurationSource conf) {
    final double cutoff = conf.getDouble(HDDS_SCM_SAFEMODE_THRESHOLD_PCT,
        HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT);
    Preconditions.checkArgument((cutoff >= 0.0 && cutoff <= 1.0),
        HDDS_SCM_SAFEMODE_THRESHOLD_PCT + " value should be >= 0.0 and <= 1.0");
    return cutoff;
  }

}
