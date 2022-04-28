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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Background service to clean up pipelines with following conditions.
 * - CLOSED
 * - ALLOCATED for too long
 */
public class BackgroundPipelineScrubber implements SCMService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BackgroundPipelineScrubber.class);

  private static final String THREAD_NAME = "PipelineScrubberThread";

  private final PipelineManager pipelineManager;
  private final ConfigurationSource conf;
  private final SCMContext scmContext;

  private final Lock serviceLock = new ReentrantLock();
  private ServiceStatus serviceStatus = ServiceStatus.PAUSING;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private Thread scrubThread;
  private final long intervalInMillis;
  private final long waitTimeInMillis;
  private long lastTimeToBeReadyInMillis = 0;
  private volatile boolean runImmediately = false;

  public BackgroundPipelineScrubber(PipelineManager pipelineManager,
      ConfigurationSource conf, SCMContext scmContext) {
    this.pipelineManager = pipelineManager;
    this.conf = conf;
    this.scmContext = scmContext;

    this.intervalInMillis = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL,
        ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.waitTimeInMillis = conf.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);

    start();
  }

  @Override
  public void notifyStatusChanged() {
    serviceLock.lock();
    try {
      if (scmContext.isLeaderReady() && !scmContext.isInSafeMode()) {
        if (serviceStatus != ServiceStatus.RUNNING) {
          LOG.info("Service {} transitions to RUNNING.", getServiceName());
          serviceStatus = ServiceStatus.RUNNING;
          lastTimeToBeReadyInMillis = Time.monotonicNow();
        }
      } else {
        if (serviceStatus != ServiceStatus.PAUSING) {
          LOG.info("Service {} transitions to PAUSING.", getServiceName());
          serviceStatus = ServiceStatus.PAUSING;
        }
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
          Time.monotonicNow() - lastTimeToBeReadyInMillis >= waitTimeInMillis;
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public String getServiceName() {
    return BackgroundPipelineScrubber.class.getSimpleName();
  }

  @Override
  public void start() {
    if (!running.compareAndSet(false, true)) {
      LOG.info("Pipeline Scrubber Service is already running, skip start.");
      return;
    }
    LOG.info("Starting Pipeline Scrubber Service.");

    scrubThread = new Thread(this::run);
    scrubThread.setName(THREAD_NAME);
    scrubThread.setDaemon(true);
    scrubThread.start();
  }

  @Override
  public void stop() {
    synchronized (this) {
      if (!running.compareAndSet(true, false)) {
        LOG.info("Pipeline Scrubber Service is not running, skip stop.");
        return;
      }
      scrubThread.interrupt();
    }
    LOG.info("Stopping Pipeline Scrubber Service.");
  }

  @VisibleForTesting
  public boolean getRunning() {
    return running.get();
  }

  private void run() {
    while (running.get()) {
      try {
        if (shouldRun()) {
          scrubAllPipelines();
        }
        synchronized (this) {
          if (!runImmediately) {
            wait(intervalInMillis);
          }
          runImmediately = false;
        }
      } catch (InterruptedException e) {
        LOG.warn("{} is interrupted, exit", THREAD_NAME);
        Thread.currentThread().interrupt();
        running.set(false);
      }
    }
  }

  public synchronized void runImmediately() {
    runImmediately = true;
    notify();
  }

  private void scrubAllPipelines() {
    try {
      pipelineManager.scrubPipelines();
    } catch (IOException e) {
      LOG.error("Unexpected error during pipeline scrubbing", e);
    }
  }
}
