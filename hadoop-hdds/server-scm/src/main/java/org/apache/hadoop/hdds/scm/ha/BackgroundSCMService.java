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

package org.apache.hadoop.hdds.scm.ha;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A common implementation for background SCMService.
 * */
public final class BackgroundSCMService implements SCMService {
  private final Logger log;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private Thread backgroundThread;
  private final Lock serviceLock = new ReentrantLock();
  private final SCMContext scmContext;
  private ServiceStatus serviceStatus = ServiceStatus.PAUSING;
  private final long intervalInMillis;
  private final long waitTimeInMillis;
  private long lastTimeToBeReadyInMillis = 0;
  private final Clock clock;
  private final String serviceName;
  private final Runnable periodicalTask;
  private volatile boolean runImmediately = false;

  private BackgroundSCMService(Builder b) {
    scmContext = b.scmContext;
    clock = b.clock;
    periodicalTask = b.periodicalTask;
    serviceName = b.serviceName;
    log = LoggerFactory.getLogger(serviceName);
    intervalInMillis = b.intervalInMillis;
    waitTimeInMillis = b.waitTimeInMillis;
    start();
  }

  @Override
  public void start() {
    if (!running.compareAndSet(false, true)) {
      log.info("{} Service is already running, skip start.", getServiceName());
      return;
    }
    log.info("Starting {} Service.", getServiceName());

    backgroundThread = new Thread(this::run);
    backgroundThread.setName(scmContext.threadNamePrefix() + serviceName);
    backgroundThread.setDaemon(true);
    backgroundThread.start();
  }

  @Override
  public void notifyStatusChanged() {
    serviceLock.lock();
    try {
      if (scmContext.isLeaderReady() && !scmContext.isInSafeMode()) {
        if (serviceStatus != ServiceStatus.RUNNING) {
          log.info("Service {} transitions to RUNNING.", getServiceName());
          serviceStatus = ServiceStatus.RUNNING;
          lastTimeToBeReadyInMillis = clock.millis();
        }
      } else {
        if (serviceStatus != ServiceStatus.PAUSING) {
          log.info("Service {} transitions to PAUSING.", getServiceName());
          serviceStatus = ServiceStatus.PAUSING;
        }
      }
    } finally {
      serviceLock.unlock();
    }
  }

  private void run() {
    while (running.get()) {
      try {
        if (shouldRun()) {
          try {
            periodicalTask.run();
          } catch (Throwable e) {
            log.error("Caught Unhandled exception in {}. The task will be " +
                "re-tried in {}ms", getServiceName(), intervalInMillis, e);
          }
        }
        synchronized (this) {
          if (!runImmediately) {
            wait(intervalInMillis);
          }
          runImmediately = false;
        }
      } catch (InterruptedException e) {
        log.warn("{} is interrupted, exit", serviceName);
        Thread.currentThread().interrupt();
        running.set(false);
      }
    }
  }

  @Override
  public void stop() {
    synchronized (this) {
      if (!running.compareAndSet(true, false)) {
        log.info("{} Service is not running, skip stop.", getServiceName());
        return;
      }
    }
    backgroundThread.interrupt();
    log.info("Stopping {} Service.", getServiceName());
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
    return serviceName;
  }

  @VisibleForTesting
  public synchronized void runImmediately() {
    runImmediately = true;
    notify();
  }

  @VisibleForTesting
  public boolean getRunning() {
    return running.get();
  }

  /**
   * Builder for BackgroundSCMService.
   * */
  public static class Builder {
    private long intervalInMillis;
    private long waitTimeInMillis;
    private String serviceName;
    private Runnable periodicalTask;
    private SCMContext scmContext;
    private Clock clock;

    public Builder setIntervalInMillis(final long intervalInMillis) {
      this.intervalInMillis = intervalInMillis;
      return this;
    }

    public Builder setWaitTimeInMillis(final long waitTimeInMillis) {
      this.waitTimeInMillis = waitTimeInMillis;
      return this;
    }

    public Builder setClock(final Clock clock) {
      this.clock = clock;
      return this;
    }

    public Builder setServiceName(final String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    public Builder setScmContext(final SCMContext scmContext) {
      this.scmContext = scmContext;
      return this;
    }

    public Builder setPeriodicalTask(final Runnable periodicalTask) {
      this.periodicalTask = periodicalTask;
      return this;
    }

    public BackgroundSCMService build() {
      Preconditions.assertNotNull(scmContext, "scmContext is null");
      Preconditions.assertNotNull(periodicalTask, "periodicalTask is null");
      Preconditions.assertNotNull(clock, "clock is null");
      Preconditions.assertNotNull(serviceName, "serviceName is null");

      return new BackgroundSCMService(this);
    }
  }
}
