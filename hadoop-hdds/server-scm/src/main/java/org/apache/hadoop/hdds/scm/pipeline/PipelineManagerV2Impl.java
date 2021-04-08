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
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.ClientVersions;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SCM Pipeline Manager implementation.
 * All the write operations for pipelines must come via PipelineManager.
 * It synchronises all write and read operations via a ReadWriteLock.
 */
public class PipelineManagerV2Impl implements PipelineManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineManagerV2Impl.class);

  // Limit the number of on-going ratis operation to be 1.
  private final Lock lock;
  private PipelineFactory pipelineFactory;
  private StateManager stateManager;
  private BackgroundPipelineCreatorV2 backgroundPipelineCreator;
  private final ConfigurationSource conf;
  private final EventPublisher eventPublisher;
  // Pipeline Manager MXBean
  private ObjectName pmInfoBean;
  private final SCMPipelineMetrics metrics;
  private final long pipelineWaitDefaultTimeout;
  private final SCMHAManager scmhaManager;
  private final SCMContext scmContext;
  private final NodeManager nodeManager;

  protected PipelineManagerV2Impl(ConfigurationSource conf,
                                 SCMHAManager scmhaManager,
                                 NodeManager nodeManager,
                                 StateManager pipelineStateManager,
                                 PipelineFactory pipelineFactory,
                                 EventPublisher eventPublisher,
                                 SCMContext scmContext) {
    this.lock = new ReentrantLock();
    this.pipelineFactory = pipelineFactory;
    this.stateManager = pipelineStateManager;
    this.conf = conf;
    this.scmhaManager = scmhaManager;
    this.nodeManager = nodeManager;
    this.eventPublisher = eventPublisher;
    this.scmContext = scmContext;
    this.pmInfoBean = MBeans.register("SCMPipelineManager",
        "SCMPipelineManagerInfo", this);
    this.metrics = SCMPipelineMetrics.create();
    this.pipelineWaitDefaultTimeout = conf.getTimeDuration(
        HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL,
        HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
  }

  public static PipelineManagerV2Impl newPipelineManager(
      ConfigurationSource conf,
      SCMHAManager scmhaManager,
      NodeManager nodeManager,
      Table<PipelineID, Pipeline> pipelineStore,
      EventPublisher eventPublisher,
      SCMContext scmContext,
      SCMServiceManager serviceManager) throws IOException {
    // Create PipelineStateManager
    StateManager stateManager = PipelineStateManagerV2Impl
        .newBuilder().setPipelineStore(pipelineStore)
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();

    // Create PipelineFactory
    PipelineFactory pipelineFactory = new PipelineFactory(
        nodeManager, stateManager, conf, eventPublisher, scmContext);

    // Create PipelineManager
    PipelineManagerV2Impl pipelineManager = new PipelineManagerV2Impl(conf,
        scmhaManager, nodeManager, stateManager, pipelineFactory,
        eventPublisher, scmContext);

    // Create background thread.
    BackgroundPipelineCreatorV2 backgroundPipelineCreator =
        new BackgroundPipelineCreatorV2(
            pipelineManager, conf, serviceManager, scmContext);

    pipelineManager.setBackgroundPipelineCreator(backgroundPipelineCreator);

    return pipelineManager;
  }

  @Override
  public Pipeline createPipeline(
      ReplicationConfig replicationConfig
  ) throws IOException {
    if (!isPipelineCreationAllowed() && !factorOne(replicationConfig)) {
      LOG.debug("Pipeline creation is not allowed until safe mode prechecks " +
          "complete");
      throw new IOException("Pipeline creation is not allowed as safe mode " +
          "prechecks have not yet passed");
    }
    lock.lock();
    try {
      Pipeline pipeline = pipelineFactory.create(replicationConfig);
      stateManager.addPipeline(pipeline.getProtobufMessage(
          ClientVersions.CURRENT_VERSION));
      recordMetricsForPipeline(pipeline);
      return pipeline;
    } catch (IOException ex) {
      LOG.debug("Failed to create pipeline with replicationConfig {}.",
          replicationConfig, ex);
      metrics.incNumPipelineCreationFailed();
      throw ex;
    } finally {
      lock.unlock();
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
  public void addContainerToPipeline(
      PipelineID pipelineID, ContainerID containerID) throws IOException {
    // should not lock here, since no ratis operation happens.
    stateManager.addContainerToPipeline(pipelineID, containerID);
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
  public void openPipeline(PipelineID pipelineId) throws IOException {
    lock.lock();
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
      lock.unlock();
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
    lock.lock();
    try {
      stateManager.removePipeline(pipelineID.getProtobuf());
      metrics.incNumPipelineDestroyed();
    } catch (IOException ex) {
      metrics.incNumPipelineDestroyFailed();
      throw ex;
    } finally {
      lock.unlock();
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
    lock.lock();
    try {
      if (!pipeline.isClosed()) {
        stateManager.updatePipelineState(pipelineID.getProtobuf(),
            HddsProtos.PipelineState.PIPELINE_CLOSED);
        LOG.info("Pipeline {} moved to CLOSED state", pipeline);
      }
      metrics.removePipelineMetrics(pipelineID);
    } finally {
      lock.unlock();
    }
    // close containers.
    closeContainersForPipeline(pipelineID);
    if (!onTimeout) {
      // close pipeline right away.
      removePipeline(pipeline);
    }
  }

  /**
   * Scrub pipelines.
   */
  @Override
  public void scrubPipeline(ReplicationConfig config)
      throws IOException {
    Instant currentTime = Instant.now();
    Long pipelineScrubTimeoutInMills = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT,
        ScmConfigKeys.OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);

    List<Pipeline> candidates = stateManager.getPipelines(config);

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
        closeContainersForPipeline(p.getId());
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
      throws IOException {
    lock.lock();
    try {
      stateManager.updatePipelineState(pipelineID.getProtobuf(),
              HddsProtos.PipelineState.PIPELINE_OPEN);
    } finally {
      lock.unlock();
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
      throws IOException {
    lock.lock();
    try {
      stateManager.updatePipelineState(pipelineID.getProtobuf(),
          HddsProtos.PipelineState.PIPELINE_DORMANT);
    } finally {
      lock.unlock();
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
  public void close() throws IOException {
    if (backgroundPipelineCreator != null) {
      backgroundPipelineCreator.stop();
    }

    if(pmInfoBean != null) {
      MBeans.unregister(this.pmInfoBean);
      pmInfoBean = null;
    }

    SCMPipelineMetrics.unRegister();

    // shutdown pipeline provider.
    pipelineFactory.shutdown();
    try {
      stateManager.close();
    } catch (Exception ex) {
      LOG.error("PipelineStateManager close failed", ex);
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
  public StateManager getStateManager() {
    return stateManager;
  }

  @VisibleForTesting
  public SCMHAManager getScmhaManager() {
    return scmhaManager;
  }

  private void setBackgroundPipelineCreator(
      BackgroundPipelineCreatorV2 backgroundPipelineCreator) {
    this.backgroundPipelineCreator = backgroundPipelineCreator;
  }

  @VisibleForTesting
  public BackgroundPipelineCreatorV2 getBackgroundPipelineCreator() {
    return this.backgroundPipelineCreator;
  }

  @VisibleForTesting
  public PipelineFactory getPipelineFactory() {
    return pipelineFactory;
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

  protected Lock getLock() {
    return lock;
  }
}
