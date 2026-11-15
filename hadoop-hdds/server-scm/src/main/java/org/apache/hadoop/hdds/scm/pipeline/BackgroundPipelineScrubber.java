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

import static org.apache.hadoop.hdds.scm.ha.SCMService.Event.CONTAINER_SAFE_MODE_RULES_VALIDATED;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background service which periodically scrubs pipelines on the leader SCM.
 *
 * Open pipelines with datanodes which are not registered must be closed
 * before SCM exits safe mode, so that those datanodes are not handed out for
 * reads or writes. Therefore, besides running out of safe mode, this service
 * also runs in safe mode once:
 * 1) all container safe mode rules have passed, so that the datanodes which
 *    are coming back have mostly registered, and
 * 2) the stale node interval has elapsed since the service was created at SCM
 *    startup, so that datanodes which are still unregistered are treated like
 *    stale nodes, whose pipelines are closed by StaleNodeHandler.
 */
public class BackgroundPipelineScrubber implements SCMService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BackgroundPipelineScrubber.class);

  private static final String SERVICE_NAME = "BackgroundPipelineScrubber";

  private final Runnable scrubTask;
  private final SCMContext scmContext;
  private volatile Clock clock;
  private final long intervalInMillis;
  private final long safeModeRunDelayInMillis;
  private final long createdTimeInMillis;
  private final String threadName;

  private final Lock serviceLock = new ReentrantLock();
  private ServiceStatus serviceStatus = ServiceStatus.PAUSING;
  private boolean containerSafeModeRulesValidated = false;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private Thread thread;
  private final Object monitor = new Object();
  private boolean runImmediately = false;

  BackgroundPipelineScrubber(Runnable scrubTask, ConfigurationSource conf,
      SCMContext scmContext, Clock clock) {
    this.scrubTask = scrubTask;
    this.scmContext = scmContext;
    this.clock = clock;
    this.intervalInMillis = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL,
        ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.safeModeRunDelayInMillis = HddsServerUtil.getStaleNodeInterval(conf);
    this.createdTimeInMillis = clock.millis();
    this.threadName = scmContext.threadNamePrefix() + SERVICE_NAME;
  }

  @Override
  public void start() {
    if (!running.compareAndSet(false, true)) {
      LOG.info("{} is already started, just ignore.", threadName);
      return;
    }
    LOG.info("Starting {}.", threadName);
    thread = new Thread(this::run);
    thread.setName(threadName);
    thread.setDaemon(true);
    thread.start();
  }

  @Override
  public void stop() {
    if (!running.compareAndSet(true, false)) {
      LOG.info("{} is not running, just ignore.", threadName);
      return;
    }
    LOG.info("Stopping {}.", threadName);
    thread.interrupt();
  }

  @VisibleForTesting
  public boolean isRunning() {
    return running.get();
  }

  private void run() {
    while (running.get()) {
      try {
        final boolean shouldRun;
        final long waitInMillis;
        serviceLock.lock();
        try {
          // Decide whether to run and how long to wait from one check, so the safe mode run delay cannot elapse in
          // between and leave the service waiting for a full interval. While the delay is pending, wake up as soon as
          // it elapses instead of waiting for the full interval.
          final long remainingDelay = getRemainingSafeModeRunDelayInMillis();
          shouldRun = (serviceStatus == ServiceStatus.RUNNING) && (remainingDelay <= 0);
          waitInMillis = remainingDelay > 0 ? Math.min(intervalInMillis, remainingDelay) : intervalInMillis;
        } finally {
          serviceLock.unlock();
        }
        if (shouldRun) {
          try {
            scrubTask.run();
          } catch (Throwable e) {
            LOG.error("Caught unhandled exception in {}. The task will be re-tried in {}ms",
                threadName, intervalInMillis, e);
          }
        }
        synchronized (monitor) {
          if (!runImmediately) {
            monitor.wait(waitInMillis);
          }
          runImmediately = false;
        }
      } catch (InterruptedException e) {
        LOG.warn("{} is interrupted, exit", threadName);
        Thread.currentThread().interrupt();
        running.set(false);
      }
    }
  }

  @Override
  public void notifyStatusChanged() {
    boolean transitionedToRunning = false;
    serviceLock.lock();
    try {
      if (scmContext.isLeaderReady() && (!scmContext.isInSafeMode() || containerSafeModeRulesValidated)) {
        if (serviceStatus != ServiceStatus.RUNNING) {
          LOG.info("Service {} transitions to RUNNING.", getServiceName());
          serviceStatus = ServiceStatus.RUNNING;
          transitionedToRunning = true;
        }
      } else if (serviceStatus != ServiceStatus.PAUSING) {
        LOG.info("Service {} transitions to PAUSING.", getServiceName());
        serviceStatus = ServiceStatus.PAUSING;
      }
    } finally {
      serviceLock.unlock();
    }
    // Do not sit out the current interval once the service is allowed to run.
    if (transitionedToRunning) {
      runImmediately();
    }
  }

  @Override
  public void notifyEventTriggered(Event event) {
    if (event != CONTAINER_SAFE_MODE_RULES_VALIDATED) {
      return;
    }
    // Recorded on followers too, so that the service can run in safe mode if
    // this SCM becomes the leader later.
    serviceLock.lock();
    try {
      if (containerSafeModeRulesValidated) {
        return;
      }
      LOG.info("Service {} is allowed to run in safe mode after event {}.", getServiceName(), event);
      containerSafeModeRulesValidated = true;
    } finally {
      serviceLock.unlock();
    }
    notifyStatusChanged();
  }

  @Override
  public boolean shouldRun() {
    serviceLock.lock();
    try {
      return serviceStatus == ServiceStatus.RUNNING && getRemainingSafeModeRunDelayInMillis() <= 0;
    } finally {
      serviceLock.unlock();
    }
  }

  /**
   * @return The time left before the service may scrub in safe mode, or 0 if it is not waiting.
   */
  private long getRemainingSafeModeRunDelayInMillis() {
    if (serviceStatus != ServiceStatus.RUNNING || !scmContext.isInSafeMode()) {
      return 0;
    }
    return safeModeRunDelayInMillis - (clock.millis() - createdTimeInMillis);
  }

  /**
   * Replaces the clock used to check the safe mode run delay, and wakes up the service to check it again. The delay is
   * still measured from the time the service was created.
   */
  @VisibleForTesting
  public void setClock(Clock clock) {
    this.clock = clock;
    runImmediately();
  }

  private void runImmediately() {
    synchronized (monitor) {
      runImmediately = true;
      monitor.notifyAll();
    }
  }

  @Override
  public String getServiceName() {
    return SERVICE_NAME;
  }
}
