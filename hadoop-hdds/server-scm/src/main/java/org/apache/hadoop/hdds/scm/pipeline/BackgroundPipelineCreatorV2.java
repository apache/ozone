/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.pipeline;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections.iterators.LoopingIterator;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.STAND_ALONE;
import static org.apache.hadoop.hdds.scm.ha.SCMService.Event.UNHEALTHY_TO_HEALTHY_NODE_HANDLER_TRIGGERED;
import static org.apache.hadoop.hdds.scm.ha.SCMService.Event.NEW_NODE_HANDLER_TRIGGERED;
import static org.apache.hadoop.hdds.scm.ha.SCMService.Event.PRE_CHECK_COMPLETED;

/**
 * Implements api for running background pipeline creation jobs.
 */
public class BackgroundPipelineCreatorV2 implements SCMService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BackgroundPipelineCreator.class);

  private final PipelineManager pipelineManager;
  private final ConfigurationSource conf;
  private final SCMContext scmContext;

  /**
   * SCMService related variables.
   * 1) after leaving safe mode, BackgroundPipelineCreator needs to
   *    wait for a while before really take effect.
   * 2) NewNodeHandler, NonHealthyToHealthyNodeHandler, PreCheckComplete
   *    will trigger a one-shot run of BackgroundPipelineCreator,
   *    no matter in safe mode or not.
   */
  private final Lock serviceLock = new ReentrantLock();
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


  BackgroundPipelineCreatorV2(PipelineManager pipelineManager,
                              ConfigurationSource conf,
                              SCMServiceManager serviceManager,
                              SCMContext scmContext) {
    this.pipelineManager = pipelineManager;
    this.conf = conf;
    this.scmContext = scmContext;

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

    // register BackgroundPipelineCreator to SCMServiceManager
    serviceManager.register(this);

    // start RatisPipelineUtilsThread
    start();
  }

  /**
   * Start RatisPipelineUtilsThread.
   */
  @Override
  public void start() {
    if (!running.compareAndSet(false, true)) {
      LOG.warn("{} is already started, just ignore.", THREAD_NAME);
      return;
    }

    LOG.info("Starting {}.", THREAD_NAME);

    thread = new ThreadFactoryBuilder()
        .setDaemon(false)
        .setNameFormat(THREAD_NAME + " - %d")
        .setUncaughtExceptionHandler((Thread t, Throwable ex) -> {
          // gracefully shutdown SCM.
          scmContext.getScm().stop();

          String message = "Terminate SCM, encounter uncaught exception"
              + " in RatisPipelineUtilsThread";
          ExitUtils.terminate(1, message, ex, LOG);
        })
        .build()
        .newThread(this::run);

    thread.start();
  }

  /**
   * Stop RatisPipelineUtilsThread.
   */
  public void stop() {
    if (running.compareAndSet(true, false)) {
      LOG.warn("{} is not running, just ignore.", THREAD_NAME);
      return;
    }

    LOG.info("Stopping {}.", THREAD_NAME);

    // in case RatisPipelineUtilsThread is sleeping
    synchronized (monitor) {
      monitor.notifyAll();
    }

    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted during join {}.", THREAD_NAME);
      Thread.currentThread().interrupt();
    }
  }

  private void run() {
    while (running.get()) {
      if (shouldRun()) {
        createPipelines();
      }

      try {
        synchronized (monitor) {
          monitor.wait(intervalInMillis);
        }
      } catch (InterruptedException e) {
        LOG.warn("{} is interrupted.", THREAD_NAME);
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

  private void createPipelines() throws RuntimeException {
    // TODO: #CLUTIL Different replication factor may need to be supported
    HddsProtos.ReplicationType type = HddsProtos.ReplicationType.valueOf(
        conf.get(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
            OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT));
    boolean autoCreateFactorOne = conf.getBoolean(
        ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE,
        ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE_DEFAULT);

    List<ReplicationConfig> list =
        new ArrayList<>();
    for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
        .values()) {
      final ReplicationConfig replicationConfig =
          ReplicationConfig.fromTypeAndFactor(type, factor);
      if (skipCreation(replicationConfig, autoCreateFactorOne)) {
        // Skip this iteration for creating pipeline
        continue;
      }
      list.add(replicationConfig);
      if (!pipelineManager.getSafeModeStatus()) {
        try {
          pipelineManager.scrubPipeline(replicationConfig);
        } catch (IOException e) {
          LOG.error("Error while scrubbing pipelines.", e);
        }
      }
    }

    LoopingIterator it = new LoopingIterator(list);
    while (it.hasNext()) {
      ReplicationConfig replicationConfig =
          (ReplicationConfig) it.next();

      try {
        pipelineManager.createPipeline(replicationConfig);
      } catch (IOException ioe) {
        it.remove();
      } catch (Throwable t) {
        LOG.error("Error while creating pipelines", t);
        it.remove();
      }
    }

    LOG.debug("BackgroundPipelineCreator createPipelines finished.");
  }

  @Override
  public void notifyStatusChanged() {
    serviceLock.lock();
    try {
      // 1) SCMContext#isLeader returns true.
      // 2) not in safe mode or createPipelineInSafeMode is true
      if (scmContext.isLeader() &&
          (!scmContext.isInSafeMode() || createPipelineInSafeMode)) {
        // transition from PAUSING to RUNNING
        if (serviceStatus != ServiceStatus.RUNNING) {
          LOG.info("Service {} transitions to RUNNING.", getServiceName());
          lastTimeToBeReadyInMillis = Time.monotonicNow();
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
        || event == UNHEALTHY_TO_HEALTHY_NODE_HANDLER_TRIGGERED
        || event == PRE_CHECK_COMPLETED) {
      LOG.info("trigger a one-shot run on {}.", THREAD_NAME);
      oneShotRun = true;

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
          Time.monotonicNow() - lastTimeToBeReadyInMillis >= waitTimeInMillis);
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public String getServiceName() {
    return BackgroundPipelineCreator.class.getSimpleName();
  }
}
