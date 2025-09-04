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

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for performing on demand scans of containers.
 * Note: [OPEN] containers are scanned only for metadata,
 *       [CLOSED, QUASI_CLOSED] containers are scanned for metadata and data.
 */
public final class OnDemandContainerScanner {
  private static final Logger LOG =
      LoggerFactory.getLogger(OnDemandContainerScanner.class);

  private final ExecutorService scanExecutor;
  private final DataTransferThrottler throttler;
  private final Canceler canceler;
  private final ConcurrentHashMap
      .KeySetView<Long, Boolean> containerRescheduleCheckSet;
  private final OnDemandScannerMetrics metrics;
  private final ContainerScanHelper scannerHelper;
  private final ContainerScanHelper scannerHelperWithoutGap;

  public OnDemandContainerScanner(
      ContainerScannerConfiguration conf, ContainerController controller) {
    throttler = new DataTransferThrottler(
        conf.getOnDemandBandwidthPerVolume());
    canceler = new Canceler();
    metrics = OnDemandScannerMetrics.create();
    scanExecutor = Executors.newSingleThreadExecutor();
    containerRescheduleCheckSet = ConcurrentHashMap.newKeySet();
    this.scannerHelper = ContainerScanHelper.withScanGap(LOG, controller, metrics, conf);
    this.scannerHelperWithoutGap = ContainerScanHelper.withoutScanGap(LOG, controller, metrics);
  }

  /**
   * Triggers an on-demand scan of this container.
   * @return An Optional containing a Future representing the pending scan task if the task is queued.
   *   The optional is empty if the task is not queued due to an ongoing scan.
   */
  public Optional<Future<?>> scanContainer(Container<?> container, String reasonForScan) {
    return scanContainer(container, scannerHelper, reasonForScan);
  }

  /**
   * Triggers an on-demand scan of this container regardless of whether it was recently scanned.
   * @return An Optional containing a Future representing the pending scan task if the task is queued.
   *   The optional is empty if the task is not queued due to an ongoing scan.
   */
  public Optional<Future<?>> scanContainerWithoutGap(Container<?> container, String reasonForScan) {
    return scanContainer(container, scannerHelperWithoutGap, reasonForScan);
  }

  private Optional<Future<?>> scanContainer(Container<?> container, ContainerScanHelper helper, String reasonForScan) {
    if (!helper.shouldScanMetadata(container)) {
      return Optional.empty();
    }

    Future<?> resultFuture = null;
    long containerId = container.getContainerData().getContainerID();
    if (addContainerToScheduledContainers(containerId)) {
      resultFuture = scanExecutor.submit(() -> {
        performOnDemandScan(container, helper, reasonForScan);
        removeContainerFromScheduledContainers(containerId);
      });
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping OnDemandScan for Container {} triggered due to '{}'. Reason: Already scheduled.",
            containerId, reasonForScan);
      }
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

  private void performOnDemandScan(Container<?> container, ContainerScanHelper helper, String reasonForScan) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Scheduling OnDemandScan for Container {}, Reason: {}",
          container.getContainerData().getContainerID(), reasonForScan);
    }
    try {
      if (helper.shouldScanData(container)) {
        helper.scanData(container, throttler, canceler);
      } else {
        // for containers that qualify for metadata scan and not data scan,
        // like OPEN containers, trigger a metadata-only scan
        helper.scanMetadata(container);
      }
    } catch (IOException e) {
      LOG.warn("Unexpected exception while scanning container "
          + container.getContainerData().getContainerID(), e);
    } catch (InterruptedException ex) {
      // This should only happen as part of shutdown, which will stop the
      // ExecutorService.
      LOG.info("On demand container scan interrupted.");
    }
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
