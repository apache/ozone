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

package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for scheduled scanners on a Datanode.
 */
public abstract class AbstractBackgroundContainerScanner implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractBackgroundContainerScanner.class);

  private final long dataScanInterval;
  private final Thread scannerThread;
  private final AtomicBoolean stopping;
  private final AtomicBoolean pausing = new AtomicBoolean();

  public AbstractBackgroundContainerScanner(String name,
      long dataScanInterval) {
    this.dataScanInterval = dataScanInterval;
    this.stopping = new AtomicBoolean(false);

    this.scannerThread = new Thread(this, name);
    this.scannerThread.setDaemon(true);
  }

  public void start() {
    scannerThread.start();
  }

  @Override
  public final void run() {
    AbstractContainerScannerMetrics metrics = getMetrics();
    try {
      while (!stopping.get()) {
        runIteration();
        metrics.resetNumContainersScanned();
        metrics.resetNumUnhealthyContainers();
      }
      LOG.info("{} exiting.", this);
    } catch (Exception e) {
      LOG.error("{} exiting because of exception ", this, e);
    } finally {
      if (metrics != null) {
        metrics.unregister();
      }
    }
  }

  @VisibleForTesting
  public final void runIteration() {
    final boolean paused = pausing.get();
    long startTime = System.nanoTime();
    if (!paused) {
      scanContainers();
    }
    long totalDuration = System.nanoTime() - startTime;
    if (stopping.get()) {
      return;
    }
    if (paused) {
      LOG.debug("Skipped iteration due to pause");
    } else {
      AbstractContainerScannerMetrics metrics = getMetrics();
      metrics.incNumScanIterations();
      LOG.info("Completed an iteration in {} minutes." +
              " Number of iterations (since the data-node restart) : {}" +
              ", Number of containers scanned in this iteration : {}" +
              ", Number of unhealthy containers found in this iteration : {}",
          TimeUnit.NANOSECONDS.toMinutes(totalDuration),
          metrics.getNumScanIterations(),
          metrics.getNumContainersScanned(),
          metrics.getNumUnHealthyContainers());
    }
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(totalDuration);
    long remainingSleep = dataScanInterval - elapsedMillis;
    handleRemainingSleep(remainingSleep);
  }

  private void scanContainers() {
    Iterator<Container<?>> itr = getContainerIterator();
    while (itr.hasNext()) {
      final boolean stopped = stopping.get();
      final boolean paused = pausing.get();
      if (stopped || paused) {
        LOG.info("{} exits scan loop stop={} pause={}", this, stopped, paused);
        break;
      }

      Container<?> c = itr.next();
      try {
        scanContainer(c);
      } catch (InterruptedException ex) {
        stopping.set(true);
      } catch (IOException ex) {
        LOG.warn("Unexpected exception while scanning container "
            + c.getContainerData().getContainerID(), ex);
      }
    }
  }

  public abstract Iterator<Container<?>> getContainerIterator();

  public abstract void scanContainer(Container<?> c)
      throws IOException, InterruptedException;

  public final void handleRemainingSleep(long remainingSleep) {
    if (remainingSleep > 0) {
      try {
        Thread.sleep(remainingSleep);
      } catch (InterruptedException ignored) {
        stopping.set(true);
        LOG.warn("Background container scan was interrupted.");
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Shutdown the current container scanning thread.
   * If the thread is already being shutdown, the call will block until the
   * shutdown completes.
   */
  public synchronized void shutdown() {
    if (stopping.compareAndSet(false, true)) {
      scannerThread.interrupt();
      try {
        scannerThread.join();
      } catch (InterruptedException ex) {
        LOG.warn("Unexpected exception while stopping data scanner.", ex);
        Thread.currentThread().interrupt();
      }
    }
  }

  public boolean isAlive() {
    return scannerThread.isAlive();
  }

  public void pause() {
    pausing.getAndSet(true);
  }

  public void unpause() {
    pausing.getAndSet(false);
  }

  @VisibleForTesting
  public abstract AbstractContainerScannerMetrics getMetrics();
}
