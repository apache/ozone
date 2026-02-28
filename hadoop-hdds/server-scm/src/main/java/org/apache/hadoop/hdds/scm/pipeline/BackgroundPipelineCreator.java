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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.STAND_ALONE;
import static org.apache.hadoop.hdds.scm.ha.SCMService.Event.NEW_NODE_HANDLER_TRIGGERED;
import static org.apache.hadoop.hdds.scm.ha.SCMService.Event.NODE_ADDRESS_UPDATE_HANDLER_TRIGGERED;
import static org.apache.hadoop.hdds.scm.ha.SCMService.Event.PRE_CHECK_COMPLETED;
import static org.apache.hadoop.hdds.scm.ha.SCMService.Event.UNHEALTHY_TO_HEALTHY_NODE_HANDLER_TRIGGERED;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.time.Clock;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.collections4.iterators.LoopingIterator;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements api for running background pipeline creation jobs.
 */
public class BackgroundPipelineCreator implements SCMService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BackgroundPipelineCreator.class);

  private final PipelineManager pipelineManager;
  private final ConfigurationSource conf;
  private final SCMContext scmContext;

  /**
   * SCMService related variables.
   * 1) after leaving safe mode, BackgroundPipelineCreator needs to
   *    wait for a while before really take effect.
   * 2) NewNodeHandler, NodeAddressUpdateHandler,
   *    NonHealthyToHealthyNodeHandler, PreCheckComplete
   *    will trigger a one-shot run of BackgroundPipelineCreator,
   *    no matter in safe mode or not.
   */
  private final Lock serviceLock = new ReentrantLock();
  private final String threadName;
  private ServiceStatus serviceStatus = ServiceStatus.PAUSING;
  private final boolean createPipelineInSafeMode;
  private final long waitTimeInMillis;
  private long lastTimeToBeReadyInMillis = 0;
  private boolean oneShotRun = false;

  /**
   * RatisPipelineUtilsThread is the one which wakes up at
   * configured interval and tries to create pipelines.
   */
  private Thread thread;
  private final Object monitor = new Object();
  private static final String THREAD_NAME = "RatisPipelineUtilsThread";
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final long intervalInMillis;
  private final Clock clock;
  private final boolean storageTypeAwareCreation;

  BackgroundPipelineCreator(PipelineManager pipelineManager,
      ConfigurationSource conf, SCMContext scmContext, Clock clock) {
    this.pipelineManager = pipelineManager;
    this.conf = conf;
    this.scmContext = scmContext;
    this.clock = clock;

    this.createPipelineInSafeMode = conf.getBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION_DEFAULT);

    this.waitTimeInMillis = conf.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);

    this.intervalInMillis = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL,
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    this.storageTypeAwareCreation = conf.getBoolean(
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_STORAGE_TYPE_AWARE,
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_STORAGE_TYPE_AWARE_DEFAULT);

    threadName = scmContext.threadNamePrefix() + THREAD_NAME;
  }

  /**
   * Start RatisPipelineUtilsThread.
   */
  @Override
  public void start() {
    if (!running.compareAndSet(false, true)) {
      LOG.warn("{} is already started, just ignore.", threadName);
      return;
    }

    LOG.info("Starting {}.", threadName);

    thread = new ThreadFactoryBuilder()
        .setDaemon(false)
        .setNameFormat(threadName + "-%d")
        .setUncaughtExceptionHandler((Thread t, Throwable ex) -> {
          String message = "Terminate SCM, encounter uncaught exception"
              + " in " + threadName;
          LOG.error("BackgroundPipelineCreator thread encountered an error. "
                  + "Thread: {}", t.toString(), ex);
          scmContext.getScm().shutDown(message);
        })
        .build()
        .newThread(this::run);

    thread.start();
  }

  /**
   * Stop RatisPipelineUtilsThread.
   */
  @Override
  public void stop() {
    if (!running.compareAndSet(true, false)) {
      LOG.warn("{} is not running, just ignore.", threadName);
      return;
    }

    LOG.info("Stopping {}.", threadName);

    // in case RatisPipelineUtilsThread is sleeping
    thread.interrupt();

    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted during join {}.", threadName);
      Thread.currentThread().interrupt();
    }
  }

  public boolean isRunning() {
    return running.get();
  }

  private void run() {
    while (running.get()) {
      if (shouldRun()) {
        createPipelines();
      }

      try {
        synchronized (monitor) {
          // skip wait if another one-shot run was triggered in the meantime
          if (!isOneShotRunNeeded()) {
            monitor.wait(intervalInMillis);
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("{} is interrupted.", threadName);
        running.set(false);
        Thread.currentThread().interrupt();
      }
    }
  }

  private boolean skipCreation(ReplicationConfig replicationConfig,
      boolean autoCreate) {
    if (replicationConfig.getReplicationType().equals(RATIS)) {
      return RatisReplicationConfig
          .hasFactor(replicationConfig, ReplicationFactor.ONE) && (!autoCreate);
    } else if (replicationConfig.getReplicationType().equals(STAND_ALONE)) {
      // For STAND_ALONE Replication Type, Replication Factor 3 should not be
      // used.
      return ((StandaloneReplicationConfig) replicationConfig)
          .getReplicationFactor() != ReplicationFactor.ONE;
    }
    return true;
  }

  @VisibleForTesting
  void createPipelines() throws RuntimeException {
    // TODO: #CLUTIL Different replication factor may need to be supported
    HddsProtos.ReplicationType type = HddsProtos.ReplicationType.valueOf(
        conf.get(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
            OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT));
    boolean autoCreateFactorOne = conf.getBoolean(
        ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE,
        ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE_DEFAULT);

    List<ReplicationConfig> replicationConfigs = new ArrayList<>();
    for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
        .values()) {
      if (factor == ReplicationFactor.ZERO) {
        continue; // Ignore it.
      }
      final ReplicationConfig replicationConfig;
      if (type != EC) {
        replicationConfig =
            ReplicationConfig.fromProtoTypeAndFactor(type, factor);
      } else if (factor == ReplicationFactor.ONE) {
        replicationConfig =
            ReplicationConfig.fromProtoTypeAndFactor(RATIS, factor);
      } else {
        continue;
      }
      if (skipCreation(replicationConfig, autoCreateFactorOne)) {
        // Skip this iteration for creating pipeline
        continue;
      }
      replicationConfigs.add(replicationConfig);
    }

    if (storageTypeAwareCreation) {
      createTypedPipelines(replicationConfigs);
    } else {
      createUntypedPipelines(replicationConfigs);
    }

    LOG.debug("BackgroundPipelineCreator createPipelines finished.");
  }

  private void createUntypedPipelines(List<ReplicationConfig> configs) {
    LoopingIterator it = new LoopingIterator(configs);
    while (it.hasNext()) {
      ReplicationConfig replicationConfig =
          (ReplicationConfig) it.next();

      try {
        Pipeline pipeline = pipelineManager.createPipeline(replicationConfig);
        LOG.info("Created new pipeline {}", pipeline);
      } catch (IOException ioe) {
        it.remove();
      } catch (Throwable t) {
        LOG.error("Error while creating pipelines", t);
        it.remove();
      }
    }
  }

  private void createTypedPipelines(List<ReplicationConfig> configs) {
    // Build (ReplicationConfig, StorageType) pairs: for each config,
    // one null entry (untyped) plus one per concrete StorageType.
    StorageType[] storageTypes = {
        StorageType.SSD, StorageType.DISK, StorageType.ARCHIVE
    };
    List<Map.Entry<ReplicationConfig, StorageType>> pairs = new ArrayList<>();
    for (ReplicationConfig config : configs) {
      pairs.add(new AbstractMap.SimpleEntry<>(config, null));
      for (StorageType st : storageTypes) {
        pairs.add(new AbstractMap.SimpleEntry<>(config, st));
      }
    }

    LoopingIterator it = new LoopingIterator(pairs);
    while (it.hasNext()) {
      @SuppressWarnings("unchecked")
      Map.Entry<ReplicationConfig, StorageType> entry =
          (Map.Entry<ReplicationConfig, StorageType>) it.next();

      try {
        Pipeline pipeline;
        if (entry.getValue() == null) {
          pipeline = pipelineManager.createPipeline(entry.getKey());
        } else {
          pipeline = pipelineManager.createPipeline(
              entry.getKey(), entry.getValue());
        }
        LOG.info("Created new pipeline {} with StorageType {}",
            pipeline, entry.getValue());
      } catch (IOException ioe) {
        it.remove();
      } catch (Throwable t) {
        LOG.error("Error while creating pipelines for StorageType "
            + entry.getValue(), t);
        it.remove();
      }
    }
  }

  @Override
  public void notifyStatusChanged() {
    serviceLock.lock();
    try {
      // 1) SCMContext#isLeader returns true.
      // 2) not in safe mode or createPipelineInSafeMode is true
      if (scmContext.isLeaderReady() &&
          (!scmContext.isInSafeMode() || createPipelineInSafeMode)) {
        // transition from PAUSING to RUNNING
        if (serviceStatus != ServiceStatus.RUNNING) {
          LOG.info("Service {} transitions to RUNNING.", getServiceName());
          lastTimeToBeReadyInMillis = clock.millis();
          serviceStatus = ServiceStatus.RUNNING;
        }
      } else {
        serviceStatus = ServiceStatus.PAUSING;
      }
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public void notifyEventTriggered(Event event) {
    if (!scmContext.isLeader()) {
      LOG.info("ignore, not leader SCM.");
      return;
    }
    if (event == NEW_NODE_HANDLER_TRIGGERED
            || event == NODE_ADDRESS_UPDATE_HANDLER_TRIGGERED
            || event == UNHEALTHY_TO_HEALTHY_NODE_HANDLER_TRIGGERED
            || event == PRE_CHECK_COMPLETED) {
      LOG.info("trigger a one-shot run on {}.", threadName);

      serviceLock.lock();
      try {
        oneShotRun = true;
      } finally {
        serviceLock.unlock();
      }

      synchronized (monitor) {
        monitor.notifyAll();
      }
    }
  }

  @Override
  public boolean shouldRun() {
    serviceLock.lock();
    try {
      // check one-short run
      if (oneShotRun) {
        oneShotRun = false;
        return true;
      }

      // If safe mode is off, then this SCMService starts to run with a delay.
      return serviceStatus == ServiceStatus.RUNNING && (
          createPipelineInSafeMode ||
          clock.millis() - lastTimeToBeReadyInMillis >= waitTimeInMillis);
    } finally {
      serviceLock.unlock();
    }
  }

  private boolean isOneShotRunNeeded() {
    serviceLock.lock();
    try {
      return oneShotRun;
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public String getServiceName() {
    return BackgroundPipelineCreator.class.getSimpleName();
  }
}
