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
package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Class for performing on demand scans of containers.
 */
public final class OnDemandContainerScanner {
  public static final Logger LOG =
      LoggerFactory.getLogger(OnDemandContainerScanner.class);

  private static OnDemandContainerScanner scanner;

  private final ContainerController controller;
  private final DataTransferThrottler throttler;
  private final Canceler canceler;
  private final ExecutorService scanExecutor;
  private final ConcurrentHashMap
      .KeySetView<Container<?>, Boolean> toBeScannedContainers;
  private final OnDemandScannerMetrics metrics;

  public static synchronized OnDemandContainerScanner getInstance() {
    Preconditions.checkNotNull(scanner);
    return scanner;
  }

  public static synchronized void init(
      ContainerScannerConfiguration conf, ContainerController controller) {
    if (scanner != null) {
      LOG.warn("Trying to initialize on demand scanner" +
          " a second time on a datanode.");
      return;
    }
    scanner = new OnDemandContainerScanner(conf, controller);
  }

  private OnDemandContainerScanner(ContainerScannerConfiguration conf,
                                   ContainerController controller) {
    this.controller = controller;
    this.throttler = new DataTransferThrottler(conf.getBandwidthPerVolume());
    this.canceler = new Canceler();
    this.metrics = OnDemandScannerMetrics.create();
    scanExecutor = Executors.newSingleThreadExecutor();
    toBeScannedContainers = ConcurrentHashMap.newKeySet();
  }

  public void scanContainer(Container<?> container) {
    if (toBeScannedContainers.add(container)) {
      scanExecutor.execute(() -> {
        long containerId = container.getContainerData().getContainerID();
        try {
          ContainerData containerData = container.getContainerData();
          logScanStart(containerData);
          if (!container.scanData(throttler, canceler)) {
            controller.markContainerUnhealthy(containerId);
            metrics.incNumUnHealthyContainers();
          } else {
            Instant now = Instant.now();
            logScanCompleted(containerData, now);
            controller.updateDataScanTimestamp(containerId, now);
          }
          metrics.incNumContainersScanned();
        } catch (IOException e) {
          LOG.warn("Unexpected exception while scanning container "
              + containerId, e);
        } finally {
          toBeScannedContainers.remove(container);
        }
      });
    }
  }

  private static void logScanStart(ContainerData containerData) {
    if (LOG.isDebugEnabled()) {
      Optional<Instant> scanTimestamp = containerData.lastDataScanTime();
      Object lastScanTime = scanTimestamp.map(ts -> "at " + ts).orElse("never");
      LOG.debug("Scanning container {}, last scanned {}",
          containerData.getContainerID(), lastScanTime);
    }
  }

  private static void logScanCompleted(
      ContainerData containerData, Instant timestamp) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Completed scan of container {} at {}",
          containerData.getContainerID(), timestamp);
    }
  }

  public synchronized void shutdown() {
    scanExecutor.shutdown();
    try {
      long timeoutSeconds = 5;
      if (!scanExecutor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
        LOG.warn("On demand scanner shut down forcefully after {} seconds",
            timeoutSeconds);
        scanExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOG.warn("On demand scanner interrupted while waiting for shut down.");
      scanExecutor.shutdownNow();
      throw new RuntimeException(e);
    }
  }
}
