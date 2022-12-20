/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.BackgroundSCMService;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * SCM Pipeline Manager implementation.
 * All the write operations for pipelines must come via PipelineManager.
 * It synchronises all write and read operations via a ReadWriteLock.
 */
public class PipelineManagerImpl implements PipelineManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineManagerImpl.class);

  // Limit the number of on-going ratis operation to be 1.
  private final ReentrantReadWriteLock lock;
  private PipelineFactory pipelineFactory;
  private PipelineStateManager stateManager;
  private BackgroundPipelineCreator backgroundPipelineCreator;
  private BackgroundSCMService backgroundPipelineScrubber;
  private final ConfigurationSource conf;
  private final EventPublisher eventPublisher;
  // Pipeline Manager MXBean
  private ObjectName pmInfoBean;
  private final SCMPipelineMetrics metrics;
  private final long pipelineWaitDefaultTimeout;
  private final SCMHAManager scmhaManager;
  private SCMContext scmContext;
  private final NodeManager nodeManager;
  // This allows for freezing/resuming the new pipeline creation while the
  // SCM is already out of SafeMode.
  private AtomicBoolean freezePipelineCreation;
  private final Clock clock;

  @SuppressWarnings("checkstyle:parameterNumber")
  protected PipelineManagerImpl(ConfigurationSource conf,
                                SCMHAManager scmhaManager,
                                NodeManager nodeManager,
                                PipelineStateManager pipelineStateManager,
                                PipelineFactory pipelineFactory,
                                EventPublisher eventPublisher,
                                SCMContext scmContext,
                                Clock clock) {
    this.lock = new ReentrantReadWriteLock();
    this.pipelineFactory = pipelineFactory;
    this.stateManager = pipelineStateManager;
    this.conf = conf;
    this.scmhaManager = scmhaManager;
    this.nodeManager = nodeManager;
    this.eventPublisher = eventPublisher;
    this.scmContext = scmContext;
    this.clock = clock;
    this.pmInfoBean = MBeans.register("SCMPipelineManager",
        "SCMPipelineManagerInfo", this);
    this.metrics = SCMPipelineMetrics.create();
    this.pipelineWaitDefaultTimeout = conf.getTimeDuration(
        HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL,
        HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.freezePipelineCreation = new AtomicBoolean();
  }

  @SuppressWarnings("checkstyle:parameterNumber")
  public static PipelineManagerImpl newPipelineManager(
      ConfigurationSource conf,
      SCMHAManager scmhaManager,
      NodeManager nodeManager,
      Table<PipelineID, Pipeline> pipelineStore,
      EventPublisher eventPublisher,
      SCMContext scmContext,
      SCMServiceManager serviceManager,
      Clock clock) throws IOException {

    // Create PipelineStateManagerImpl
    PipelineStateManager stateManager = PipelineStateManagerImpl
        .newBuilder().setPipelineStore(pipelineStore)
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();

    // Create PipelineFactory
    PipelineFactory pipelineFactory = new PipelineFactory(
        nodeManager, stateManager, conf, eventPublisher, scmContext);

    // Create PipelineManager
    PipelineManagerImpl pipelineManager = new PipelineManagerImpl(conf,
        scmhaManager, nodeManager, stateManager, pipelineFactory,
        eventPublisher, scmContext, clock);

    // Create background thread.
    BackgroundPipelineCreator backgroundPipelineCreator =
        new BackgroundPipelineCreator(pipelineManager, conf, scmContext, clock);

    pipelineManager.setBackgroundPipelineCreator(backgroundPipelineCreator);
    serviceManager.register(backgroundPipelineCreator);

    if (FinalizationManager.shouldCreateNewPipelines(
        scmContext.getFinalizationCheckpoint())) {
      pipelineManager.resumePipelineCreation();
    } else {
      pipelineManager.freezePipelineCreation();
    }

    final long scrubberIntervalInMillis = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL,
        ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    final long safeModeWaitMs = conf.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);

    BackgroundSCMService backgroundPipelineScrubber =
        new BackgroundSCMService.Builder().setClock(clock)
            .setScmContext(scmContext)
            .setServiceName("BackgroundPipelineScrubber")
            .setIntervalInMillis(scrubberIntervalInMillis)
            .setWaitTimeInMillis(safeModeWaitMs)
            .setPeriodicalTask(() -> {
              try {
                pipelineManager.scrubPipelines();
              } catch (IOException | TimeoutException e) {
                LOG.error("Unexpected error during pipeline scrubbing", e);
              }
            }).build();

    pipelineManager.setBackgroundPipelineScrubber(backgroundPipelineScrubber);
    serviceManager.register(backgroundPipelineScrubber);

    return pipelineManager;
  }

  @Override
  public Pipeline createPipeline(ReplicationConfig replicationConfig)
      throws IOException, TimeoutException {
    return createPipeline(replicationConfig, Collections.emptyList(),
        Collections.emptyList());
  }

  @Override
  public Pipeline createPipeline(ReplicationConfig replicationConfig,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes)
      throws IOException, TimeoutException {
    if (!isPipelineCreationAllowed() && !factorOne(replicationConfig)) {
      LOG.debug("Pipeline creation is not allowed until safe mode prechecks " +
          "complete");
      throw new IOException("Pipeline creation is not allowed as safe mode " +
          "prechecks have not yet passed");
    }

    if (freezePipelineCreation.get()) {
      String message = "Cannot create new pipelines while pipeline creation " +
          "is frozen.";
      LOG.info(message);
      throw new IOException(message);
    }

    acquireWriteLock();
    try {
      Pipeline pipeline = pipelineFactory.create(replicationConfig,
          excludedNodes, favoredNodes);
      stateManager.addPipeline(pipeline.getProtobufMessage(
          ClientVersion.CURRENT_VERSION));
      recordMetricsForPipeline(pipeline);
      return pipeline;
    } catch (IOException | TimeoutException ex) {
      LOG.debug("Failed to create pipeline with replicationConfig {}.",
          replicationConfig, ex);
      metrics.incNumPipelineCreationFailed();
      throw ex;
    } finally {
      releaseWriteLock();
    }
  }

  private boolean factorOne(ReplicationConfig replicationConfig) {
    if (replicationConfig.getReplicationType() == ReplicationType.RATIS) {
      return ((RatisReplicationConfig) replicationConfig).getReplicationFactor()
          == ReplicationFactor.ONE;

    } else if (replicationConfig.getReplicationType()
        == ReplicationType.STAND_ALONE) {
      return ((StandaloneReplicationConfig) replicationConfig)
          .getReplicationFactor()
          == ReplicationFactor.ONE;
    }
    return false;
  }

  @Override
  public Pipeline createPipeline(
      ReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes
  ) {
    // This will mostly be used to create dummy pipeline for SimplePipelines.
    // We don't update the metrics for SimplePipelines.
    return pipelineFactory.create(replicationConfig, nodes);
  }

  @Override
  public Pipeline createPipelineForRead(
      ReplicationConfig replicationConfig, Set<ContainerReplica> replicas) {
    return pipelineFactory.createForRead(replicationConfig, replicas);
  }

  @Override
  public Pipeline getPipeline(PipelineID pipelineID)
      throws PipelineNotFoundException {
    return stateManager.getPipeline(pipelineID);
  }

  @Override
  public boolean containsPipeline(PipelineID pipelineID) {
    try {
      getPipeline(pipelineID);
      return true;
    } catch (PipelineNotFoundException e) {
      return false;
    }
  }

  @Override
  public List<Pipeline> getPipelines() {
    return stateManager.getPipelines();
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationConfig replicationConfig) {
    return stateManager.getPipelines(replicationConfig);
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationConfig config,
      Pipeline.PipelineState state) {
    return stateManager.getPipelines(config, state);
  }

  @Override
  public List<Pipeline> getPipelines(
      ReplicationConfig replicationConfig,
      Pipeline.PipelineState state, Collection<DatanodeDetails> excludeDns,
      Collection<PipelineID> excludePipelines) {
    return stateManager
        .getPipelines(replicationConfig, state, excludeDns, excludePipelines);
  }

  @Override
  /**
   * Returns the count of pipelines meeting the given ReplicationConfig and
   * state.
   * @param replicationConfig The ReplicationConfig of the pipelines to count
   * @param state The current state of the pipelines to count
   * @return The count of pipelines meeting the above criteria
   */
  public int getPipelineCount(ReplicationConfig config,
                                     Pipeline.PipelineState state) {
    return stateManager.getPipelineCount(config, state);
  }

  @Override
  public void addContainerToPipeline(
      PipelineID pipelineID, ContainerID containerID) throws IOException {
    // should not lock here, since no ratis operation happens.
    stateManager.addContainerToPipeline(pipelineID, containerID);
  }

  @Override
  public void addContainerToPipelineSCMStart(
      PipelineID pipelineID, ContainerID containerID) throws IOException {
    // should not lock here, since no ratis operation happens.
    stateManager.addContainerToPipelineSCMStart(pipelineID, containerID);
  }

  @Override
  public void removeContainerFromPipeline(
      PipelineID pipelineID, ContainerID containerID) throws IOException {
    // should not lock here, since no ratis operation happens.
    stateManager.removeContainerFromPipeline(pipelineID, containerID);
  }

  @Override
  public NavigableSet<ContainerID> getContainersInPipeline(
      PipelineID pipelineID) throws IOException {
    return stateManager.getContainers(pipelineID);
  }

  @Override
  public int getNumberOfContainers(PipelineID pipelineID) throws IOException {
    return stateManager.getNumberOfContainers(pipelineID);
  }

  @Override
  public void openPipeline(PipelineID pipelineId)
      throws IOException, TimeoutException {
    acquireWriteLock();
    try {
      Pipeline pipeline = stateManager.getPipeline(pipelineId);
      if (pipeline.isClosed()) {
        throw new IOException("Closed pipeline can not be opened");
      }
      if (pipeline.getPipelineState() == Pipeline.PipelineState.ALLOCATED) {
        LOG.info("Pipeline {} moved to OPEN state", pipeline);
        stateManager.updatePipelineState(
            pipelineId.getProtobuf(), HddsProtos.PipelineState.PIPELINE_OPEN);
      }
      metrics.incNumPipelineCreated();
      metrics.createPerPipelineMetrics(pipeline);
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * Removes the pipeline from the db and pipeline state map.
   *
   * @param pipeline - pipeline to be removed
   * @throws IOException
   */
  protected void removePipeline(Pipeline pipeline)
      throws IOException, TimeoutException {
    pipelineFactory.close(pipeline.getType(), pipeline);
    PipelineID pipelineID = pipeline.getId();
    acquireWriteLock();
    try {
      stateManager.removePipeline(pipelineID.getProtobuf());
      metrics.incNumPipelineDestroyed();
    } catch (IOException ex) {
      metrics.incNumPipelineDestroyFailed();
      throw ex;
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * Fire events to close all containers related to the input pipeline.
   * @param pipelineId - ID of the pipeline.
   * @throws IOException
   */
  protected void closeContainersForPipeline(final PipelineID pipelineId)
      throws IOException, TimeoutException {
    Set<ContainerID> containerIDs = stateManager.getContainers(pipelineId);
    ContainerManager containerManager = scmContext.getScm()
        .getContainerManager();
    for (ContainerID containerID : containerIDs) {
      if (containerManager.getContainer(containerID).getState()
            == HddsProtos.LifeCycleState.OPEN) {
        try {
          containerManager.updateContainerState(containerID,
              HddsProtos.LifeCycleEvent.FINALIZE);
        } catch (InvalidStateTransitionException ex) {
          throw new IOException(ex);
        }
      }
      eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER, containerID);
      LOG.info("Container {} closed for pipeline={}", containerID, pipelineId);
    }
  }

  /**
   * put pipeline in CLOSED state.
   * @param pipeline - ID of the pipeline.
   * @param onTimeout - whether to remove pipeline after some time.
   * @throws IOException
   */
  @Override
  public void closePipeline(Pipeline pipeline, boolean onTimeout)
      throws IOException, TimeoutException {
    PipelineID pipelineID = pipeline.getId();
    // close containers.
    closeContainersForPipeline(pipelineID);
    acquireWriteLock();
    try {
      if (!pipeline.isClosed()) {
        stateManager.updatePipelineState(pipelineID.getProtobuf(),
            HddsProtos.PipelineState.PIPELINE_CLOSED);
        LOG.info("Pipeline {} moved to CLOSED state", pipeline);
      }
      metrics.removePipelineMetrics(pipelineID);
    } finally {
      releaseWriteLock();
    }
    if (!onTimeout) {
      // close pipeline right away.
      removePipeline(pipeline);
    }
  }

  /** close the pipelines whose nodes' IPs are stale.
   *
   * @param datanodeDetails new datanodeDetails
   */
  @Override
  public void closeStalePipelines(DatanodeDetails datanodeDetails) {
    List<Pipeline> pipelinesWithStaleIpOrHostname =
            getStalePipelines(datanodeDetails);
    if (pipelinesWithStaleIpOrHostname.isEmpty()) {
      LOG.debug("No stale pipelines for datanode {}",
              datanodeDetails.getUuidString());
      return;
    }
    LOG.info("Found {} stale pipelines",
            pipelinesWithStaleIpOrHostname.size());
    pipelinesWithStaleIpOrHostname.forEach(p -> {
      try {
        LOG.info("Closing the stale pipeline: {}", p.getId());
        closePipeline(p, false);
        LOG.info("Closed the stale pipeline: {}", p.getId());
      } catch (IOException | TimeoutException e) {
        LOG.error("Closing the stale pipeline failed: {}", p, e);
      }
    });
  }

  @VisibleForTesting
  List<Pipeline> getStalePipelines(DatanodeDetails datanodeDetails) {
    List<Pipeline> pipelines = getPipelines();
    return pipelines.stream()
            .filter(p -> p.getNodes().stream()
                    .anyMatch(n -> n.getUuid()
                            .equals(datanodeDetails.getUuid())
                            && (!n.getIpAddress()
                            .equals(datanodeDetails.getIpAddress())
                            || !n.getHostName()
                            .equals(datanodeDetails.getHostName()))))
            .collect(Collectors.toList());
  }

  /**
   * Scrub pipelines.
   */
  @Override
  public void scrubPipelines() throws IOException, TimeoutException {
    Instant currentTime = clock.instant();
    Long pipelineScrubTimeoutInMills = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT,
        ScmConfigKeys.OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);

    List<Pipeline> candidates = stateManager.getPipelines();

    for (Pipeline p : candidates) {
      // scrub pipelines who stay ALLOCATED for too long.
      if (p.getPipelineState() == Pipeline.PipelineState.ALLOCATED &&
          (currentTime.toEpochMilli() - p.getCreationTimestamp()
              .toEpochMilli() >= pipelineScrubTimeoutInMills)) {
        LOG.info("Scrubbing pipeline: id: {} since it stays at ALLOCATED " +
            "stage for {} mins.", p.getId(),
            Duration.between(currentTime, p.getCreationTimestamp())
                .toMinutes());
        closePipeline(p, false);
      }
      // scrub pipelines who stay CLOSED for too long.
      if (p.getPipelineState() == Pipeline.PipelineState.CLOSED) {
        LOG.info("Scrubbing pipeline: id: {} since it stays at CLOSED stage.",
            p.getId());
        closeContainersForPipeline(p.getId());
        removePipeline(p);
      }
      // If a datanode is stopped and then SCM is restarted, a pipeline can get
      // stuck in an open state. For Ratis, provided some other DNs that were
      // part of the open pipeline register to SCM after the restart, the Ratis
      // pipeline close will get triggered by the DNs. For EC that will never
      // happen, as the DNs are not aware of the pipeline. Therefore we should
      // close any pipelines in the scrubber if they have nodes which are not
      // registered
      if (isOpenWithUnregisteredNodes(p)) {
        LOG.info("Scrubbing pipeline: id: {} as it has unregistered nodes",
            p.getId());
        closeContainersForPipeline(p.getId());
        closePipeline(p, true);
      }
    }
  }

  /**
   * @param pipeline The pipeline to check
   * @return True if the pipeline is open and contains unregistered nodes. False
   *         otherwise.
   */
  private boolean isOpenWithUnregisteredNodes(Pipeline pipeline) {
    if (!pipeline.isOpen()) {
      return false;
    }
    for (DatanodeDetails dn : pipeline.getNodes()) {
      if (nodeManager.getNodeByUuid(dn.getUuidString()) == null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Schedules a fixed interval job to create pipelines.
   */
  @Override
  public void startPipelineCreator() {
    throw new RuntimeException("Not supported in HA code.");
  }

  /**
   * Triggers pipeline creation after the specified time.
   */
  @Override
  public void triggerPipelineCreation() {
    throw new RuntimeException("Not supported in HA code.");
  }

  @Override
  public void incNumBlocksAllocatedMetric(PipelineID id) {
    metrics.incNumBlocksAllocated(id);
  }

  @Override
  public int minHealthyVolumeNum(Pipeline pipeline) {
    return nodeManager.minHealthyVolumeNum(pipeline.getNodes());
  }

  @Override
  public int minPipelineLimit(Pipeline pipeline) {
    return nodeManager.minPipelineLimit(pipeline.getNodes());
  }

  /**
   * Activates a dormant pipeline.
   *
   * @param pipelineID ID of the pipeline to activate.
   * @throws IOException in case of any Exception
   */
  @Override
  public void activatePipeline(PipelineID pipelineID)
      throws IOException, TimeoutException {
    acquireWriteLock();
    try {
      stateManager.updatePipelineState(pipelineID.getProtobuf(),
              HddsProtos.PipelineState.PIPELINE_OPEN);
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * Deactivates an active pipeline.
   *
   * @param pipelineID ID of the pipeline to deactivate.
   * @throws IOException in case of any Exception
   */
  @Override
  public void deactivatePipeline(PipelineID pipelineID)
      throws IOException, TimeoutException {
    acquireWriteLock();
    try {
      stateManager.updatePipelineState(pipelineID.getProtobuf(),
          HddsProtos.PipelineState.PIPELINE_DORMANT);
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * Wait a pipeline to be OPEN.
   *
   * @param pipelineID ID of the pipeline to wait for.
   * @param timeout    wait timeout, millisecond, 0 to use default value
   * @throws IOException in case of any Exception, such as timeout
   */
  @Override
  public void waitPipelineReady(PipelineID pipelineID, long timeout)
      throws IOException {
    waitOnePipelineReady(Lists.newArrayList(pipelineID), timeout);
  }

  @Override
  public Pipeline waitOnePipelineReady(Collection<PipelineID> pipelineIDs,
                                   long timeout)
          throws IOException {
    long st = clock.millis();
    if (timeout == 0) {
      timeout = pipelineWaitDefaultTimeout;
    }
    List<String> pipelineIDStrs =
            pipelineIDs.stream()
                    .map(id -> id.getId().toString())
                            .collect(Collectors.toList());
    String piplineIdsStr = String.join(",", pipelineIDStrs);
    Pipeline pipeline = null;
    do {
      boolean found = false;
      for (PipelineID pipelineID : pipelineIDs) {
        try {
          Pipeline tempPipeline = stateManager.getPipeline(pipelineID);
          found = true;
          if (tempPipeline.isOpen()) {
            pipeline = tempPipeline;
            break;
          }
        } catch (PipelineNotFoundException e) {
          LOG.warn("Pipeline {} cannot be found", pipelineID);
        }
      }

      if (!found) {
        throw new PipelineNotFoundException("The input pipeline IDs " +
                piplineIdsStr + " cannot be found");
      }

      if (pipeline == null) {
        try {
          Thread.sleep((long)100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    } while (pipeline == null && clock.millis() - st < timeout);

    if (pipeline == null) {
      throw new IOException(String.format("Pipeline %s is not ready in %d ms",
              piplineIdsStr, timeout));
    }
    return pipeline;
  }

  @Override
  public Map<String, Integer> getPipelineInfo() throws NotLeaderException {
    final Map<String, Integer> pipelineInfo = new HashMap<>();
    for (Pipeline.PipelineState state : Pipeline.PipelineState.values()) {
      pipelineInfo.put(state.toString(), 0);
    }
    stateManager.getPipelines().forEach(pipeline ->
        pipelineInfo.computeIfPresent(
            pipeline.getPipelineState().toString(), (k, v) -> v + 1));
    return pipelineInfo;
  }

  /**
   * Get SafeMode status.
   * @return boolean
   */
  @Override
  public boolean getSafeModeStatus() {
    return scmContext.isInSafeMode();
  }

  @Override
  public void reinitialize(Table<PipelineID, Pipeline> pipelineStore)
      throws IOException {
    stateManager.reinitialize(pipelineStore);
  }

  @Override
  public void freezePipelineCreation() {
    freezePipelineCreation.set(true);
    backgroundPipelineCreator.stop();
  }

  @Override
  public void resumePipelineCreation() {
    freezePipelineCreation.set(false);
    backgroundPipelineCreator.start();
  }

  @Override
  public boolean isPipelineCreationFrozen() {
    return freezePipelineCreation.get() &&
        !backgroundPipelineCreator.isRunning();
  }

  @Override
  public void close() throws IOException {
    if (backgroundPipelineCreator != null) {
      backgroundPipelineCreator.stop();
    }
    if (backgroundPipelineScrubber != null) {
      backgroundPipelineScrubber.stop();
    }

    if (pmInfoBean != null) {
      MBeans.unregister(this.pmInfoBean);
      pmInfoBean = null;
    }

    SCMPipelineMetrics.unRegister();

    // shutdown pipeline provider.
    pipelineFactory.shutdown();
    try {
      stateManager.close();
    } catch (Exception ex) {
      LOG.error("PipelineStateManagerImpl close failed", ex);
    }
  }

  @VisibleForTesting
  public boolean isPipelineCreationAllowed() {
    return scmContext.isLeader() && scmContext.isPreCheckComplete();
  }

  @VisibleForTesting
  public void setPipelineProvider(ReplicationType replicationType,
                                  PipelineProvider provider) {
    pipelineFactory.setProvider(replicationType, provider);
  }

  @VisibleForTesting
  public PipelineStateManager getStateManager() {
    return stateManager;
  }

  @VisibleForTesting
  public SCMHAManager getScmhaManager() {
    return scmhaManager;
  }

  private void setBackgroundPipelineCreator(
      BackgroundPipelineCreator backgroundPipelineCreator) {
    this.backgroundPipelineCreator = backgroundPipelineCreator;
  }

  @VisibleForTesting
  public BackgroundPipelineCreator getBackgroundPipelineCreator() {
    return this.backgroundPipelineCreator;
  }

  private void setBackgroundPipelineScrubber(
      BackgroundSCMService backgroundPipelineScrubber) {
    this.backgroundPipelineScrubber = backgroundPipelineScrubber;
  }

  @VisibleForTesting
  public BackgroundSCMService getBackgroundPipelineScrubber() {
    return this.backgroundPipelineScrubber;
  }

  @VisibleForTesting
  public PipelineFactory getPipelineFactory() {
    return pipelineFactory;
  }

  @VisibleForTesting
  public void setScmContext(SCMContext context) {
    this.scmContext = context;
  }

  private void recordMetricsForPipeline(Pipeline pipeline) {
    metrics.incNumPipelineAllocated();
    if (pipeline.isOpen()) {
      metrics.incNumPipelineCreated();
      metrics.createPerPipelineMetrics(pipeline);
    }
    switch (pipeline.getType()) {
    case STAND_ALONE:
      return;
    case RATIS:
      List<Pipeline> overlapPipelines = RatisPipelineUtils
          .checkPipelineContainSameDatanodes(stateManager, pipeline);
      if (!overlapPipelines.isEmpty()) {
        // Count 1 overlap at a time.
        metrics.incNumPipelineContainSameDatanodes();
        //TODO remove until pipeline allocation is proved equally distributed.
        for (Pipeline overlapPipeline : overlapPipelines) {
          LOG.info("Pipeline: " + pipeline.getId().toString() +
              " contains same datanodes as previous pipelines: " +
              overlapPipeline.getId().toString() + " nodeIds: " +
              pipeline.getNodes().get(0).getUuid().toString() +
              ", " + pipeline.getNodes().get(1).getUuid().toString() +
              ", " + pipeline.getNodes().get(2).getUuid().toString());
        }
      }
      return;
    case CHAINED:
      // Not supported.
    default:
      // Not supported.
      return;
    }
  }

  @Override
  public void acquireReadLock() {
    lock.readLock().lock();
  }

  @Override
  public void releaseReadLock() {
    lock.readLock().unlock();
  }

  @Override
  public void acquireWriteLock() {
    lock.writeLock().lock();
  }

  @Override
  public void releaseWriteLock() {
    lock.writeLock().unlock();
  }
}
