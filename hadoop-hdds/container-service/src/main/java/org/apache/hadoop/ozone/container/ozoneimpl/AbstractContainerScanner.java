/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Base class for scheduled scanners on a Datanode.
 */
public abstract class AbstractContainerScanner extends Thread {
  public static final Logger LOG =
      LoggerFactory.getLogger(AbstractContainerScanner.class);

  private final long dataScanInterval;

  /**
   * True if the thread is stopping.<p/>
   * Protected by this object's lock.
   */
  private volatile boolean stopping = false;

  public AbstractContainerScanner(String name, long dataScanInterval) {
    this.dataScanInterval = dataScanInterval;
    setName(name);
    setDaemon(true);
  }

  @Override
  public final void run() {
    AbstractContainerScannerMetrics metrics = getMetrics();
    try {
      while (!stopping) {
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
    long startTime = System.nanoTime();
    scanContainers();
    long totalDuration = System.nanoTime() - startTime;
    if (stopping) {
      return;
    }
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
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(totalDuration);
    long remainingSleep = dataScanInterval - elapsedMillis;
    handleRemainingSleep(remainingSleep);
  }

  public final void scanContainers() {
    Iterator<Container<?>> itr = getContainerIterator();
    while (!stopping && itr.hasNext()) {
      Container<?> c = itr.next();
      try {
        scanContainer(c);
      } catch (IOException ex) {
        LOG.warn("Unexpected exception while scanning container "
            + c.getContainerData().getContainerID(), ex);
      }
    }
  }

  public abstract Iterator<Container<?>> getContainerIterator();

  public abstract void scanContainer(Container<?> c) throws IOException;

  public final void handleRemainingSleep(long remainingSleep) {
    if (remainingSleep > 0) {
      try {
        Thread.sleep(remainingSleep);
      } catch (InterruptedException ignored) {
        this.stopping = true;
        LOG.warn("Background container scan was interrupted.");
        Thread.currentThread().interrupt();
      }
    }
  }

  public synchronized void shutdown() {
    this.stopping = true;
    this.interrupt();
    try {
      this.join();
    } catch (InterruptedException ex) {
      LOG.warn("Unexpected exception while stopping data scanner.", ex);
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  public abstract AbstractContainerScannerMetrics getMetrics();
}
