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
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.Scheduler;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SCM Pipeline Manager implementation.
 * All the write operations for pipelines must come via PipelineManager.
 * It synchronises all write and read operations via a ReadWriteLock.
 */
public final class PipelineManagerV2Impl implements PipelineManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMPipelineManager.class);

  private final ReadWriteLock lock;
  private PipelineFactory pipelineFactory;
  private StateManager stateManager;
  private Scheduler scheduler;
  private BackgroundPipelineCreator backgroundPipelineCreator;
  private final ConfigurationSource conf;
  private final EventPublisher eventPublisher;
  // Pipeline Manager MXBean
  private ObjectName pmInfoBean;
  private final SCMPipelineMetrics metrics;
  private long pipelineWaitDefaultTimeout;
  private final AtomicBoolean isInSafeMode;
  // Used to track if the safemode pre-checks have completed. This is designed
  // to prevent pipelines being created until sufficient nodes have registered.
  private final AtomicBoolean pipelineCreationAllowed;

  private PipelineManagerV2Impl(ConfigurationSource conf,
                               NodeManager nodeManager,
                               StateManager pipelineStateManager,
                               PipelineFactory pipelineFactory,
                                EventPublisher eventPublisher) {
    this.lock = new ReentrantReadWriteLock();
    this.pipelineFactory = pipelineFactory;
    this.stateManager = pipelineStateManager;
    this.conf = conf;
    this.eventPublisher = eventPublisher;
    this.pmInfoBean = MBeans.register("SCMPipelineManager",
        "SCMPipelineManagerInfo", this);
    this.metrics = SCMPipelineMetrics.create();
    this.pipelineWaitDefaultTimeout = conf.getTimeDuration(
        HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL,
        HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.isInSafeMode = new AtomicBoolean(conf.getBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED_DEFAULT));
    // Pipeline creation is only allowed after the safemode prechecks have
    // passed, eg sufficient nodes have registered.
    this.pipelineCreationAllowed = new AtomicBoolean(!this.isInSafeMode.get());
  }

  public static PipelineManagerV2Impl newPipelineManager(
      ConfigurationSource conf, SCMHAManager scmhaManager,
      NodeManager nodeManager, Table<PipelineID, Pipeline> pipelineStore,
      EventPublisher eventPublisher) throws IOException {
    // Create PipelineStateManager
    StateManager stateManager = PipelineStateManagerV2Impl
        .newBuilder().setPipelineStore(pipelineStore)
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .build();

    // Create PipelineFactory
    PipelineFactory pipelineFactory = new PipelineFactory(
        nodeManager, stateManager, conf, eventPublisher);
    // Create PipelineManager
    PipelineManagerV2Impl pipelineManager = new PipelineManagerV2Impl(conf,
        nodeManager, stateManager, pipelineFactory, eventPublisher);

    // Create background thread.
    Scheduler scheduler = new Scheduler(
        "RatisPipelineUtilsThread", false, 1);
    BackgroundPipelineCreator backgroundPipelineCreator =
        new BackgroundPipelineCreator(pipelineManager, scheduler, conf);
    pipelineManager.setBackgroundPipelineCreator(backgroundPipelineCreator);
    pipelineManager.setScheduler(scheduler);

    return pipelineManager;
  }

  @Override
  public synchronized Pipeline createPipeline(ReplicationType type,
                                 ReplicationFactor factor) throws IOException {
    if (!isPipelineCreationAllowed() && factor != ReplicationFactor.ONE) {
      LOG.debug("Pipeline creation is not allowed until safe mode prechecks " +
          "complete");
      throw new IOException("Pipeline creation is not allowed as safe mode " +
          "prechecks have not yet passed");
    }
    lock.writeLock().lock();
    try {
      Pipeline pipeline = pipelineFactory.create(type, factor);
      stateManager.addPipeline(pipeline.getProtobufMessage());
      recordMetricsForPipeline(pipeline);
      return pipeline;
    } catch (IOException ex) {
      LOG.error("Failed to create pipeline of type {} and factor {}. " +
          "Exception: {}", type, factor, ex.getMessage());
      metrics.incNumPipelineCreationFailed();
      throw ex;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Pipeline createPipeline(ReplicationType type, ReplicationFactor factor,
                          List<DatanodeDetails> nodes) {
    // This will mostly be used to create dummy pipeline for SimplePipelines.
    // We don't update the metrics for SimplePipelines.
    lock.writeLock().lock();
    try {
      return pipelineFactory.create(type, factor, nodes);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Pipeline getPipeline(PipelineID pipelineID)
      throws PipelineNotFoundException {
    lock.readLock().lock();
    try {
      return stateManager.getPipeline(pipelineID);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean containsPipeline(PipelineID pipelineID) {
    lock.readLock().lock();
    try {
      getPipeline(pipelineID);
      return true;
    } catch (PipelineNotFoundException e) {
      return false;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines() {
    lock.readLock().lock();
    try {
      return stateManager.getPipelines();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type) {
    try {
      return stateManager.getPipelines(type);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type,
                              ReplicationFactor factor) {
    lock.readLock().lock();
    try {
      return stateManager.getPipelines(type, factor);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type,
                              Pipeline.PipelineState state) {
    lock.readLock().lock();
    try {
      return stateManager.getPipelines(type, state);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type,
                              ReplicationFactor factor,
                              Pipeline.PipelineState state) {
    lock.readLock().lock();
    try {
      return stateManager.getPipelines(type, factor, state);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(
      ReplicationType type, ReplicationFactor factor,
      Pipeline.PipelineState state, Collection<DatanodeDetails> excludeDns,
      Collection<PipelineID> excludePipelines) {
    lock.readLock().lock();
    try {
      return stateManager
          .getPipelines(type, factor, state, excludeDns, excludePipelines);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void addContainerToPipeline(
      PipelineID pipelineID, ContainerID containerID) throws IOException {
    lock.writeLock().lock();
    try {
      stateManager.addContainerToPipeline(pipelineID, containerID);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void removeContainerFromPipeline(
      PipelineID pipelineID, ContainerID containerID) throws IOException {
    lock.writeLock().lock();
    try {
      stateManager.removeContainerFromPipeline(pipelineID, containerID);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public NavigableSet<ContainerID> getContainersInPipeline(
      PipelineID pipelineID) throws IOException {
    lock.readLock().lock();
    try {
      return stateManager.getContainers(pipelineID);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public int getNumberOfContainers(PipelineID pipelineID) throws IOException {
    return stateManager.getNumberOfContainers(pipelineID);
  }

  @Override
  public void openPipeline(PipelineID pipelineId) throws IOException {
    lock.writeLock().lock();
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
      lock.writeLock().unlock();
    }
  }

  /**
   * Removes the pipeline from the db and pipeline state map.
   *
   * @param pipeline - pipeline to be removed
   * @throws IOException
   */
  protected void removePipeline(Pipeline pipeline) throws IOException {
    pipelineFactory.close(pipeline.getType(), pipeline);
    PipelineID pipelineID = pipeline.getId();
    lock.writeLock().lock();
    try {
      closeContainersForPipeline(pipelineID);
      stateManager.removePipeline(pipelineID.getProtobuf());
      metrics.incNumPipelineDestroyed();
    } catch (IOException ex) {
      metrics.incNumPipelineDestroyFailed();
      throw ex;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Fire events to close all containers related to the input pipeline.
   * @param pipelineId - ID of the pipeline.
   * @throws IOException
   */
  protected void closeContainersForPipeline(final PipelineID pipelineId)
      throws IOException {
    Set<ContainerID> containerIDs = stateManager.getContainers(pipelineId);
    for (ContainerID containerID : containerIDs) {
      eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER, containerID);
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
      throws IOException {
    PipelineID pipelineID = pipeline.getId();
    lock.writeLock().lock();
    try {
      if (!pipeline.isClosed()) {
        stateManager.updatePipelineState(pipelineID.getProtobuf(),
            HddsProtos.PipelineState.PIPELINE_CLOSED);
        LOG.info("Pipeline {} moved to CLOSED state", pipeline);
      }
      metrics.removePipelineMetrics(pipelineID);
    } finally {
      lock.writeLock().unlock();
    }
    if (!onTimeout) {
      removePipeline(pipeline);
    }
  }

  /**
   * Scrub pipelines.
   * @param type Pipeline type
   * @param factor Pipeline factor
   * @throws IOException
   */
  @Override
  public void scrubPipeline(ReplicationType type, ReplicationFactor factor)
      throws IOException{
    if (type != ReplicationType.RATIS || factor != ReplicationFactor.THREE) {
      // Only srub pipeline for RATIS THREE pipeline
      return;
    }
    Instant currentTime = Instant.now();
    Long pipelineScrubTimeoutInMills = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT,
        ScmConfigKeys.OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);

    List<Pipeline> candidates = stateManager.getPipelines(type, factor);

    for (Pipeline p : candidates) {
      // scrub pipelines who stay ALLOCATED for too long.
      if (p.getPipelineState() == Pipeline.PipelineState.ALLOCATED &&
          (currentTime.toEpochMilli() - p.getCreationTimestamp()
              .toEpochMilli() >= pipelineScrubTimeoutInMills)) {
        LOG.info("Scrubbing pipeline: id: " + p.getId().toString() +
            " since it stays at ALLOCATED stage for " +
            Duration.between(currentTime, p.getCreationTimestamp())
                .toMinutes() + " mins.");
        closePipeline(p, false);
      }
      // scrub pipelines who stay CLOSED for too long.
      if (p.getPipelineState() == Pipeline.PipelineState.CLOSED) {
        LOG.info("Scrubbing pipeline: id: " + p.getId().toString() +
            " since it stays at CLOSED stage.");
        removePipeline(p);
      }
    }
    return;
  }

  /**
   * Schedules a fixed interval job to create pipelines.
   */
  @Override
  public void startPipelineCreator() {
    backgroundPipelineCreator.startFixedIntervalPipelineCreator();
  }

  /**
   * Triggers pipeline creation after the specified time.
   */
  @Override
  public void triggerPipelineCreation() {
    backgroundPipelineCreator.triggerPipelineCreation();
  }

  @Override
  public void incNumBlocksAllocatedMetric(PipelineID id) {
    metrics.incNumBlocksAllocated(id);
  }

  /**
   * Activates a dormant pipeline.
   *
   * @param pipelineID ID of the pipeline to activate.
   * @throws IOException in case of any Exception
   */
  @Override
  public void activatePipeline(PipelineID pipelineID)
      throws IOException {
    stateManager.updatePipelineState(pipelineID.getProtobuf(),
        HddsProtos.PipelineState.PIPELINE_OPEN);
  }

  /**
   * Deactivates an active pipeline.
   *
   * @param pipelineID ID of the pipeline to deactivate.
   * @throws IOException in case of any Exception
   */
  @Override
  public void deactivatePipeline(PipelineID pipelineID)
      throws IOException {
    stateManager.updatePipelineState(pipelineID.getProtobuf(),
        HddsProtos.PipelineState.PIPELINE_DORMANT);
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
    long st = Time.monotonicNow();
    if (timeout == 0) {
      timeout = pipelineWaitDefaultTimeout;
    }

    boolean ready;
    Pipeline pipeline;
    do {
      try {
        pipeline = stateManager.getPipeline(pipelineID);
      } catch (PipelineNotFoundException e) {
        throw new PipelineNotFoundException(String.format(
            "Pipeline %s cannot be found", pipelineID));
      }
      ready = pipeline.isOpen();
      if (!ready) {
        try {
          Thread.sleep((long)100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    } while (!ready && Time.monotonicNow() - st < timeout);

    if (!ready) {
      throw new IOException(String.format("Pipeline %s is not ready in %d ms",
          pipelineID, timeout));
    }
  }

  @Override
  public Map<String, Integer> getPipelineInfo() {
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
    return this.isInSafeMode.get();
  }

  @Override
  public void close() throws IOException {
    if (scheduler != null) {
      scheduler.close();
      scheduler = null;
    }

    if(pmInfoBean != null) {
      MBeans.unregister(this.pmInfoBean);
      pmInfoBean = null;
    }

    SCMPipelineMetrics.unRegister();

    // shutdown pipeline provider.
    pipelineFactory.shutdown();
  }

  @Override
  public void onMessage(SCMSafeModeManager.SafeModeStatus status,
                        EventPublisher publisher) {
    // TODO: #CLUTIL - handle safemode getting re-enabled
    boolean currentAllowPipelines =
        pipelineCreationAllowed.getAndSet(status.isPreCheckComplete());
    boolean currentlyInSafeMode =
        isInSafeMode.getAndSet(status.isInSafeMode());

    // Trigger pipeline creation only if the preCheck status has changed to
    // complete.
    if (isPipelineCreationAllowed() && !currentAllowPipelines) {
      triggerPipelineCreation();
    }
    // Start the pipeline creation thread only when safemode switches off
    if (!getSafeModeStatus() && currentlyInSafeMode) {
      startPipelineCreator();
    }
  }

  @VisibleForTesting
  public boolean isPipelineCreationAllowed() {
    return pipelineCreationAllowed.get();
  }

  @VisibleForTesting
  public void allowPipelineCreation() {
    this.pipelineCreationAllowed.set(true);
  }

  private void setBackgroundPipelineCreator(
      BackgroundPipelineCreator backgroundPipelineCreator) {
    this.backgroundPipelineCreator = backgroundPipelineCreator;
  }

  private void setScheduler(Scheduler scheduler) {
    this.scheduler = scheduler;
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
}
