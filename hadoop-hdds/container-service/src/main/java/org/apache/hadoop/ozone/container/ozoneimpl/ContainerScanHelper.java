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
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.slf4j.Logger;

/**
 * Mixin to handle common data and metadata scan operations among background and on-demand scanners.
 */
public final class ContainerScanHelper {
  private final Logger log;
  private final ContainerController controller;
  private final AbstractContainerScannerMetrics metrics;
  private final long minScanGap;

  public static ContainerScanHelper withoutScanGap(Logger log, ContainerController controller,
      AbstractContainerScannerMetrics metrics) {
    return new ContainerScanHelper(log, controller, metrics, 0);
  }

  public static ContainerScanHelper withScanGap(Logger log, ContainerController controller,
      AbstractContainerScannerMetrics metrics, ContainerScannerConfiguration conf) {
    return new ContainerScanHelper(log, controller, metrics, conf.getContainerScanMinGap());
  }

  private ContainerScanHelper(Logger log, ContainerController controller,
                             AbstractContainerScannerMetrics metrics, long minScanGap) {
    this.log = log;
    this.controller = controller;
    this.metrics = metrics;
    this.minScanGap = minScanGap;
  }

  public void scanData(Container<?> container, DataTransferThrottler throttler, Canceler canceler)
      throws IOException, InterruptedException {
    if (!shouldScanData(container)) {
      return;
    }
    ContainerData containerData = container.getContainerData();
    long containerId = containerData.getContainerID();
    logScanStart(containerData, "data");
    DataScanResult result = container.scanData(throttler, canceler);

    if (result.isDeleted()) {
      log.debug("Container [{}] has been deleted during the data scan.", containerId);
    } else {
      try {
        controller.updateContainerChecksum(containerId, result.getDataTree());
      } catch (IOException ex) {
        log.warn("Failed to update container checksum after scan of container {}", containerId, ex);
      }
      if (result.hasErrors()) {
        handleUnhealthyScanResult(containerData, result);
      }
      metrics.incNumContainersScanned();
    }

    Instant now = Instant.now();
    if (!result.isDeleted()) {
      controller.updateDataScanTimestamp(containerId, now);
    }
    // Even if the container was deleted, mark the scan as completed since we already logged it as starting.
    logScanCompleted(containerData, now);
  }

  public void scanMetadata(Container<?> container)
      throws IOException, InterruptedException {
    if (!shouldScanMetadata(container)) {
      return;
    }
    ContainerData containerData = container.getContainerData();
    long containerId = containerData.getContainerID();
    logScanStart(containerData, "only metadata");

    MetadataScanResult result = container.scanMetaData();
    if (result.isDeleted()) {
      log.debug("Container [{}] has been deleted during metadata scan.", containerId);
      return;
    }
    if (result.hasErrors()) {
      handleUnhealthyScanResult(containerData, result);
    }

    Instant now = Instant.now();
    // Do not update the scan timestamp after the scan since this was just a
    // metadata scan, not a full data scan.
    metrics.incNumContainersScanned();
    // Even if the container was deleted, mark the scan as completed since we already logged it as starting.
    logScanCompleted(containerData, now);
  }

  public void handleUnhealthyScanResult(ContainerData containerData, ScanResult result) throws IOException {
    long containerID = containerData.getContainerID();
    log.error("Corruption detected in container [{}]. Marking it UNHEALTHY. {}", containerID, result);
    if (log.isDebugEnabled()) {
      StringBuilder allErrorString = new StringBuilder();
      result.getErrors().forEach(r -> allErrorString.append(r).append('\n'));
      log.debug("Complete list of errors detected while scanning container {}:\n{}", containerID, allErrorString);
    }

    // Only increment the number of unhealthy containers if the container was not already unhealthy.
    // TODO HDDS-11593: Scanner counters will start from zero
    //  at the beginning of each run, so this will need to be incremented for every unhealthy container seen
    //  regardless of its previous state.
    boolean containerMarkedUnhealthy = controller.markContainerUnhealthy(containerID, result);
    if (containerMarkedUnhealthy) {
      metrics.incNumUnHealthyContainers();
      // triggering a volume scan for the unhealthy container
      triggerVolumeScan(containerData);
    }
  }

  public void triggerVolumeScan(ContainerData containerData) {
    HddsVolume volume = containerData.getVolume();
    if (volume != null && !volume.isFailed()) {
      log.info("Triggering scan of volume [{}] with unhealthy container [{}]",
          volume, containerData.getContainerID());
      StorageVolumeUtil.onFailure(volume);
    } else if (volume == null) {
      log.warn("Cannot trigger volume scan for container {} since its volume is null",
          containerData.getContainerID());
    } else {
      log.debug("Skipping volume scan for container {} since its volume {} has failed.",
          containerData.getContainerID(), volume);
    }
  }

  public boolean shouldScanMetadata(Container<?> container) {
    if (container == null) {
      return false;
    }
    long containerID = container.getContainerData().getContainerID();

    HddsVolume containerVolume = container.getContainerData().getVolume();
    if (containerVolume.isFailed()) {
      log.debug("Skipping scan for container {} since its volume {} has failed.", containerID, containerVolume);
      return false;
    }

    return !recentlyScanned(container.getContainerData());
  }

  public boolean shouldScanData(Container<?> container) {
    return shouldScanMetadata(container) && container.shouldScanData();
  }

  private boolean recentlyScanned(ContainerData containerData) {
    Optional<Instant> lastScanTime = containerData.lastDataScanTime();
    Instant now = Instant.now();
    // Container is considered recently scanned if it was scanned within the
    // configured time frame. If the optional is empty, the container was
    // never scanned.
    boolean recentlyScanned = lastScanTime.map(scanInstant ->
            Duration.between(now, scanInstant).abs()
                .compareTo(Duration.ofMillis(minScanGap)) < 0)
        .orElse(false);

    if (recentlyScanned && log.isDebugEnabled()) {
      log.debug("Skipping scan for container {} which was last " +
              "scanned at {}. Current time is {}.",
          containerData.getContainerID(), lastScanTime.get(),
          now);
    }

    return recentlyScanned;
  }

  private void logScanStart(ContainerData containerData, String scanType) {
    if (log.isDebugEnabled()) {
      Optional<Instant> scanTimestamp = containerData.lastDataScanTime();
      Object lastScanTime = scanTimestamp.map(ts -> "at " + ts).orElse("never");
      log.debug("Scanning {} of container {}, last scanned {}",
          scanType, containerData.getContainerID(), lastScanTime);
    }
  }

  private void logScanCompleted(
      ContainerData containerData, Instant timestamp) {
    log.debug("Completed scan of container {} at {}",
        containerData.getContainerID(), timestamp);
  }
}
