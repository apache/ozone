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
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.Scheduler;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE;

/**
 * Implements api needed for management of pipelines. All the write operations
 * for pipelines must come via PipelineManager. It synchronises all write
 * and read operations via a ReadWriteLock.
 */
public class SCMPipelineManager implements PipelineManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMPipelineManager.class);

  private final ReadWriteLock lock;
  private PipelineFactory pipelineFactory;
  private PipelineStateManager stateManager;
  private final BackgroundPipelineCreator backgroundPipelineCreator;
  private Scheduler scheduler;

  private final EventPublisher eventPublisher;
  private final NodeManager nodeManager;
  private final SCMPipelineMetrics metrics;
  private final ConfigurationSource conf;
  private long pipelineWaitDefaultTimeout;
  // Pipeline Manager MXBean
  private ObjectName pmInfoBean;

  private Table<PipelineID, Pipeline> pipelineStore;

  private final AtomicBoolean isInSafeMode;
  // Used to track if the safemode pre-checks have completed. This is designed
  // to prevent pipelines being created until sufficient nodes have registered.
  private final AtomicBoolean pipelineCreationAllowed;

  public SCMPipelineManager(ConfigurationSource conf,
      NodeManager nodeManager,
      Table<PipelineID, Pipeline> pipelineStore,
      EventPublisher eventPublisher)
      throws IOException {
    this(conf, nodeManager, pipelineStore, eventPublisher, null, null);
    this.stateManager = new PipelineStateManager();
    this.pipelineFactory = new PipelineFactory(nodeManager,
        stateManager, conf, eventPublisher);
    this.pipelineStore = pipelineStore;
    initializePipelineState();
  }

  protected SCMPipelineManager(ConfigurationSource conf,
      NodeManager nodeManager,
      Table<PipelineID, Pipeline> pipelineStore,
      EventPublisher eventPublisher,
      PipelineStateManager pipelineStateManager,
      PipelineFactory pipelineFactory)
      throws IOException {
    this.lock = new ReentrantReadWriteLock();
    this.pipelineStore = pipelineStore;
    this.conf = conf;
    this.pipelineFactory = pipelineFactory;
    this.stateManager = pipelineStateManager;
    // TODO: See if thread priority needs to be set for these threads
    scheduler = new Scheduler("RatisPipelineUtilsThread", false, 1);
    this.backgroundPipelineCreator =
        new BackgroundPipelineCreator(this, scheduler, conf);
    this.eventPublisher = eventPublisher;
    this.nodeManager = nodeManager;
    this.metrics = SCMPipelineMetrics.create();
    this.pmInfoBean = MBeans.register("SCMPipelineManager",
        "SCMPipelineManagerInfo", this);
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

  public PipelineStateManager getStateManager() {
    return stateManager;
  }

  @VisibleForTesting
  public void setPipelineProvider(ReplicationType replicationType,
                                  PipelineProvider provider) {
    pipelineFactory.setProvider(replicationType, provider);
  }

  @VisibleForTesting
  public void allowPipelineCreation() {
    this.pipelineCreationAllowed.set(true);
  }

  @VisibleForTesting
  public boolean isPipelineCreationAllowed() {
    return pipelineCreationAllowed.get();
  }

  protected void initializePipelineState() throws IOException {
    if (pipelineStore.isEmpty()) {
      LOG.info("No pipeline exists in current db");
      return;
    }
    TableIterator<PipelineID, ? extends KeyValue<PipelineID, Pipeline>>
        iterator = pipelineStore.iterator();
    while (iterator.hasNext()) {
      Pipeline pipeline = nextPipelineFromIterator(iterator);
      stateManager.addPipeline(pipeline);
      nodeManager.addPipeline(pipeline);
    }
  }

  private Pipeline nextPipelineFromIterator(
      TableIterator<PipelineID, ? extends KeyValue<PipelineID, Pipeline>> it
  ) throws IOException {
    KeyValue<PipelineID, Pipeline> actual = it.next();
    Pipeline pipeline = actual.getValue();
    PipelineID pipelineID = actual.getKey();
    checkKeyAndReplaceIfObsolete(it, pipeline, pipelineID);
    return pipeline;
  }

  /**
   * This method is part of the change that happens in HDDS-3925, and we can
   * and should remove this on later on.
   * The purpose of the change is to get rid of protobuf serialization in the
   * SCM database Pipeline table keys. The keys are not used anywhere, and the
   * PipelineID that is used as a key is in the value as well, so we can detect
   * a change in the key translation to byte[] and if we have the old format
   * we refresh the table contents during SCM startup.
   *
   * If this fails in the remove, then there is an IOException coming from
   * RocksDB itself, in this case in memory structures will still be fine and
   * SCM should be operational, however we will attempt to replace the old key
   * at next startup. In this case removing of the pipeline will leave the
   * pipeline in RocksDB, and during next startup we will attempt to delete it
   * again. This does not affect any runtime operations.
   * If a Pipeline should have been deleted but remained in RocksDB, then at
   * next startup it will be replaced and added with the new key, then SCM will
   * detect that it is an invalid Pipeline and successfully delete it with the
   * new key.
   * For further info check the JIRA.
   *
   * @param it the iterator used to iterate the Pipeline table
   * @param pipeline the pipeline read already from the iterator
   * @param pipelineID the pipeline ID read from the raw data via the iterator
   */
  private void checkKeyAndReplaceIfObsolete(
      TableIterator<PipelineID, ? extends KeyValue<PipelineID, Pipeline>> it,
      Pipeline pipeline,
      PipelineID pipelineID
  ) {
    if (!pipelineID.equals(pipeline.getId())) {
      try {
        LOG.info("Found pipeline in old format key : {}", pipeline.getId());
        it.removeFromDB();
        pipelineStore.put(pipeline.getId(), pipeline);
      } catch (IOException e) {
        LOG.info("Pipeline table in RocksDB has an old key format, and "
            + "removing the pipeline with the old key was unsuccessful."
            + "Pipeline: {}", pipeline);
      }
    }
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
  public Pipeline createPipeline(ReplicationType type,
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
      if (pipelineStore != null) {
        pipelineStore.put(pipeline.getId(), pipeline);
      }
      stateManager.addPipeline(pipeline);
      nodeManager.addPipeline(pipeline);
      recordMetricsForPipeline(pipeline);
      return pipeline;
    } catch (IOException ex) {
      if (ex instanceof SCMException &&
          ((SCMException) ex).getResult() == FAILED_TO_FIND_SUITABLE_NODE) {
        // Avoid spam SCM log with errors when SCM has enough open pipelines
        LOG.debug("Can't create more pipelines of type {} and factor {}. " +
            "Reason: {}", type, factor, ex.getMessage());
      } else {
        LOG.error("Failed to create pipeline of type {} and factor {}. " +
            "Exception: {}", type, factor, ex.getMessage());
      }
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
    lock.readLock().lock();
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
      ReplicationFactor factor, Pipeline.PipelineState state) {
    lock.readLock().lock();
    try {
      return stateManager.getPipelines(type, factor, state);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(ReplicationType type,
      ReplicationFactor factor, Pipeline.PipelineState state,
      Collection<DatanodeDetails> excludeDns,
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
  public void addContainerToPipeline(PipelineID pipelineID,
      ContainerID containerID) throws IOException {
    lock.writeLock().lock();
    try {
      stateManager.addContainerToPipeline(pipelineID, containerID);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void updatePipelineStateInDb(PipelineID pipelineId,
                                       Pipeline.PipelineState state)
          throws IOException {
    // null check is here to prevent the case where SCM store
    // is closed but the staleNode handlers/pipleine creations
    // still try to access it.
    if (pipelineStore != null) {
      try {
        pipelineStore.put(pipelineId, getPipeline(pipelineId));
      } catch (IOException ex) {
        LOG.info("Pipeline {} state update failed", pipelineId);
        // revert back to old state in memory
        stateManager.updatePipelineState(pipelineId, state);
      }
    }
  }

  @Override
  public void removeContainerFromPipeline(PipelineID pipelineID,
      ContainerID containerID) throws IOException {
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
      Pipeline.PipelineState state = stateManager.
              getPipeline(pipelineId).getPipelineState();
      Pipeline pipeline = stateManager.openPipeline(pipelineId);
      updatePipelineStateInDb(pipelineId, state);
      metrics.incNumPipelineCreated();
      metrics.createPerPipelineMetrics(pipeline);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Finalizes pipeline in the SCM. Removes pipeline and makes rpc call to
   * destroy pipeline on the datanodes immediately or after timeout based on the
   * value of onTimeout parameter.
   *
   * @param pipeline        - Pipeline to be destroyed
   * @param onTimeout       - if true pipeline is removed and destroyed on
   *                        datanodes after timeout
   * @throws IOException
   */
  @Override
  public void finalizeAndDestroyPipeline(Pipeline pipeline, boolean onTimeout)
      throws IOException {
    LOG.info("Destroying pipeline:{}", pipeline);
    finalizePipeline(pipeline.getId());
    if (onTimeout) {
      long pipelineDestroyTimeoutInMillis =
          conf.getTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT,
              ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT_DEFAULT,
              TimeUnit.MILLISECONDS);
      scheduler.schedule(() -> destroyPipeline(pipeline),
          pipelineDestroyTimeoutInMillis, TimeUnit.MILLISECONDS, LOG,
          String.format("Destroy pipeline failed for pipeline:%s", pipeline));
    } else {
      destroyPipeline(pipeline);
    }
  }

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
    List<Pipeline> needToSrubPipelines = stateManager.getPipelines(type, factor,
        Pipeline.PipelineState.ALLOCATED).stream()
        .filter(p -> currentTime.toEpochMilli() - p.getCreationTimestamp()
            .toEpochMilli() >= pipelineScrubTimeoutInMills)
        .collect(Collectors.toList());
    for (Pipeline p : needToSrubPipelines) {
      LOG.info("Scrubbing pipeline: id: " + p.getId().toString() +
          " since it stays at ALLOCATED stage for " +
          Duration.between(currentTime, p.getCreationTimestamp()).toMinutes() +
          " mins.");
      finalizeAndDestroyPipeline(p, false);
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

  /**
   * Activates a dormant pipeline.
   *
   * @param pipelineID ID of the pipeline to activate.
   * @throws IOException in case of any Exception
   */
  @Override
  public void activatePipeline(PipelineID pipelineID)
      throws IOException {
    lock.writeLock().lock();
    try {
      Pipeline.PipelineState state = stateManager.
              getPipeline(pipelineID).getPipelineState();
      stateManager.activatePipeline(pipelineID);
      updatePipelineStateInDb(pipelineID, state);
    } finally {
      lock.writeLock().unlock();
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
    lock.writeLock().lock();
    try {
      Pipeline.PipelineState state = stateManager.
              getPipeline(pipelineID).getPipelineState();
      stateManager.deactivatePipeline(pipelineID);
      updatePipelineStateInDb(pipelineID, state);
    } finally {
      lock.writeLock().unlock();
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

  /**
   * Moves the pipeline to CLOSED state and sends close container command for
   * all the containers in the pipeline.
   *
   * @param pipelineId - ID of the pipeline to be moved to CLOSED state.
   * @throws IOException
   */
  private void finalizePipeline(PipelineID pipelineId) throws IOException {
    lock.writeLock().lock();
    try {
      Pipeline.PipelineState state = stateManager.
              getPipeline(pipelineId).getPipelineState();
      stateManager.finalizePipeline(pipelineId);
      updatePipelineStateInDb(pipelineId, state);
      Set<ContainerID> containerIDs = stateManager.getContainers(pipelineId);
      for (ContainerID containerID : containerIDs) {
        eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER, containerID);
      }
      metrics.removePipelineMetrics(pipelineId);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Removes pipeline from SCM. Sends ratis command to destroy pipeline on all
   * the datanodes for ratis pipelines.
   *
   * @param pipeline        - Pipeline to be destroyed
   * @throws IOException
   */
  protected void destroyPipeline(Pipeline pipeline) throws IOException {
    pipelineFactory.close(pipeline.getType(), pipeline);
    // remove the pipeline from the pipeline manager
    removePipeline(pipeline.getId());
    triggerPipelineCreation();
  }

  /**
   * Removes the pipeline from the db and pipeline state map.
   *
   * @param pipelineId - ID of the pipeline to be removed
   * @throws IOException
   */
  protected void removePipeline(PipelineID pipelineId) throws IOException {
    lock.writeLock().lock();
    try {
      if (pipelineStore != null) {
        pipelineStore.delete(pipelineId);
        Pipeline pipeline = stateManager.removePipeline(pipelineId);
        nodeManager.removePipeline(pipeline);
        metrics.incNumPipelineDestroyed();
      }
    } catch (IOException ex) {
      metrics.incNumPipelineDestroyFailed();
      throw ex;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void incNumBlocksAllocatedMetric(PipelineID id) {
    metrics.incNumBlocksAllocated(id);
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
    lock.writeLock().lock();
    try {
      pipelineStore.close();
      pipelineStore = null;
    } catch (Exception ex) {
      LOG.error("Pipeline  store close failed", ex);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * returns min number of healthy volumes from the set of
   * datanodes constituting the pipeline.
   * @param  pipeline
   * @return healthy volume count
   */
  @Override
  public int minHealthyVolumeNum(Pipeline pipeline) {
    return nodeManager.minHealthyVolumeNum(pipeline.getNodes());
  }

  /**
   * returns max count of raft log volumes from the set of
   * datanodes constituting the pipeline.
   * @param  pipeline
   * @return healthy volume count
   */
  @Override
  public int minPipelineLimit(Pipeline pipeline) {
    return nodeManager.minPipelineLimit(pipeline.getNodes());
  }

  protected ReadWriteLock getLock() {
    return lock;
  }

  @VisibleForTesting
  public PipelineFactory getPipelineFactory() {
    return pipelineFactory;
  }

  protected NodeManager getNodeManager() {
    return nodeManager;
  }

  @Override
  public boolean getSafeModeStatus() {
    return this.isInSafeMode.get();
  }

  public Table<PipelineID, Pipeline> getPipelineStore() {
    return pipelineStore;
  }

  @Override
  public void onMessage(SafeModeStatus status,
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
  protected static Logger getLog() {
    return LOG;
  }
}
