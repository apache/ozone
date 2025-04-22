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

import static org.apache.hadoop.ozone.container.ozoneimpl.AbstractBackgroundContainerScanner.logUnhealthyScanResult;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for performing on demand scans of containers.
 */
public final class OnDemandContainerDataScanner {
  public static final Logger LOG =
      LoggerFactory.getLogger(OnDemandContainerDataScanner.class);

  private final ExecutorService scanExecutor;
  private final ContainerController containerController;
  private final DataTransferThrottler throttler;
  private final Canceler canceler;
  private final ConcurrentHashMap
      .KeySetView<Long, Boolean> containerRescheduleCheckSet;
  private final OnDemandScannerMetrics metrics;
  private final long minScanGap;

  public OnDemandContainerDataScanner(
      ContainerScannerConfiguration conf, ContainerController controller) {
    containerController = controller;
    throttler = new DataTransferThrottler(
        conf.getOnDemandBandwidthPerVolume());
    canceler = new Canceler();
    metrics = OnDemandScannerMetrics.create();
    scanExecutor = Executors.newSingleThreadExecutor();
    containerRescheduleCheckSet = ConcurrentHashMap.newKeySet();
    minScanGap = conf.getContainerScanMinGap();
  }

  private boolean shouldScan(Container<?> container) {
    if (container == null) {
      return false;
    }
    long containerID = container.getContainerData().getContainerID();

    HddsVolume containerVolume = container.getContainerData().getVolume();
    if (containerVolume.isFailed()) {
      LOG.debug("Skipping on demand scan for container {} since its volume {}" +
          " has failed.", containerID, containerVolume);
      return false;
    }

    return !ContainerUtils.recentlyScanned(container, minScanGap,
        LOG) && container.shouldScanData();
  }

  public Optional<Future<?>> scanContainer(Container<?> container) {
    if (!shouldScan(container)) {
      return Optional.empty();
    }

    Future<?> resultFuture = null;
    long containerId = container.getContainerData().getContainerID();
    if (addContainerToScheduledContainers(containerId)) {
      resultFuture = scanExecutor.submit(() -> {
        performOnDemandScan(container);
        removeContainerFromScheduledContainers(containerId);
      });
    }
    return Optional.ofNullable(resultFuture);
  }

  private boolean addContainerToScheduledContainers(long containerId) {
    return containerRescheduleCheckSet.add(containerId);
  }

  private void removeContainerFromScheduledContainers(
      long containerId) {
    containerRescheduleCheckSet.remove(containerId);
  }

  private void performOnDemandScan(Container<?> container) {
    if (!shouldScan(container)) {
      return;
    }

    long containerId = container.getContainerData().getContainerID();
    try {
      ContainerData containerData = container.getContainerData();
      logScanStart(containerData);

      ScanResult result = container.scanData(throttler, canceler);
      // Metrics for skipped containers should not be updated.
      if (result.isDeleted()) {
        LOG.debug("Container [{}] has been deleted during the data scan.", containerId);
      } else {
        if (!result.isHealthy()) {
          logUnhealthyScanResult(containerId, result, LOG);
          boolean containerMarkedUnhealthy = containerController
              .markContainerUnhealthy(containerId, result);
          if (containerMarkedUnhealthy) {
            metrics.incNumUnHealthyContainers();
          }
        }
        // TODO HDDS-10374 will need to update the merkle tree here as well.
        metrics.incNumContainersScanned();
      }

      // Even if the container was deleted, mark the scan as completed since we already logged it as starting.
      Instant now = Instant.now();
      logScanCompleted(containerData, now);

      if (!result.isDeleted()) {
        containerController.updateDataScanTimestamp(containerId, now);
      }
    } catch (IOException e) {
      LOG.warn("Unexpected exception while scanning container "
          + containerId, e);
    } catch (InterruptedException ex) {
      // This should only happen as part of shutdown, which will stop the
      // ExecutorService.
      LOG.info("On demand container scan interrupted.");
    }
  }

  private void logScanStart(ContainerData containerData) {
    if (LOG.isDebugEnabled()) {
      Optional<Instant> scanTimestamp = containerData.lastDataScanTime();
      Object lastScanTime = scanTimestamp.map(ts -> "at " + ts).orElse("never");
      LOG.debug("Scanning container {}, last scanned {}",
          containerData.getContainerID(), lastScanTime);
    }
  }

  private void logScanCompleted(
      ContainerData containerData, Instant timestamp) {
    LOG.debug("Completed scan of container {} at {}",
        containerData.getContainerID(), timestamp);
  }

  public OnDemandScannerMetrics getMetrics() {
    return metrics;
  }

  public synchronized void shutdown() {
    metrics.unregister();
    String shutdownMessage = "On-demand container scanner is shutting down.";
    LOG.info(shutdownMessage);
    this.canceler.cancel(shutdownMessage);
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
