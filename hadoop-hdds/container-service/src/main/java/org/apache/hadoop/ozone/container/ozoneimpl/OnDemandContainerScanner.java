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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Class for performing on demand scans of containers.
 */
public final class OnDemandContainerScanner {
  public static final Logger LOG =
      LoggerFactory.getLogger(OnDemandContainerScanner.class);

  private static volatile OnDemandContainerScanner instance;

  private final ExecutorService scanExecutor;
  private final ContainerController containerController;
  private final DataTransferThrottler throttler;
  private final Canceler canceler;
  private final ConcurrentHashMap
      .KeySetView<Long, Boolean> containerRescheduleCheckSet;
  private final OnDemandScannerMetrics metrics;

  private OnDemandContainerScanner(
      ContainerScannerConfiguration conf, ContainerController controller) {
    containerController = controller;
    throttler = new DataTransferThrottler(
        conf.getOnDemandBandwidthPerVolume());
    canceler = new Canceler();
    metrics = OnDemandScannerMetrics.create();
    scanExecutor = Executors.newSingleThreadExecutor();
    containerRescheduleCheckSet = ConcurrentHashMap.newKeySet();
  }

  public static synchronized void init(
      ContainerScannerConfiguration conf, ContainerController controller) {
    if (instance != null) {
      LOG.warn("Trying to initialize on demand scanner" +
          " a second time on a datanode.");
      return;
    }
    instance = new OnDemandContainerScanner(conf, controller);
  }

  public static Optional<Future<?>> scanContainer(Container<?> container) {
    if (instance == null || !container.shouldScanData()) {
      return Optional.empty();
    }
    Future<?> resultFuture = null;
    long containerId = container.getContainerData().getContainerID();
    if (addContainerToScheduledContainers(containerId)) {
      resultFuture = instance.scanExecutor.submit(() -> {
        if (container.shouldScanData()) {
          performOnDemandScan(container);
        }
        removeContainerFromScheduledContainers(containerId);
      });
    }
    return Optional.ofNullable(resultFuture);
  }

  private static boolean addContainerToScheduledContainers(long containerId) {
    return instance.containerRescheduleCheckSet.add(containerId);
  }

  private static void removeContainerFromScheduledContainers(
      long containerId) {
    instance.containerRescheduleCheckSet.remove(containerId);
  }

  private static void performOnDemandScan(Container<?> container) {
    long containerId = container.getContainerData().getContainerID();
    try {
      ContainerData containerData = container.getContainerData();
      logScanStart(containerData);
      if (container.scanData(instance.throttler, instance.canceler)) {
        Instant now = Instant.now();
        logScanCompleted(containerData, now);
        instance.containerController.updateDataScanTimestamp(containerId, now);
      } else {
        instance.containerController.markContainerUnhealthy(containerId);
        instance.metrics.incNumUnHealthyContainers();
      }
      instance.metrics.incNumContainersScanned();
    } catch (IOException e) {
      LOG.warn("Unexpected exception while scanning container "
          + containerId, e);
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
    LOG.debug("Completed scan of container {} at {}",
        containerData.getContainerID(), timestamp);
  }

  public static OnDemandScannerMetrics getMetrics() {
    return instance.metrics;
  }

  @VisibleForTesting
  public static DataTransferThrottler getThrottler() {
    return instance.throttler;
  }

  @VisibleForTesting
  public static Canceler getCanceler() {
    return instance.canceler;
  }

  public static synchronized void shutdown() {
    if (instance == null) {
      return;
    }
    instance.shutdownScanner();
  }

  private synchronized void shutdownScanner() {
    instance = null;
    metrics.unregister();
    this.canceler.cancel("On-demand container" +
        " scanner is shutting down.");
    if (!scanExecutor.isShutdown()) {
      scanExecutor.shutdown();
    }
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
      Thread.currentThread().interrupt();
    }
  }
}
