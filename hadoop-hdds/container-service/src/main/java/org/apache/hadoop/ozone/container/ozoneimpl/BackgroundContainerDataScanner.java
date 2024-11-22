/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Optional;

/**
 * Data scanner that full checks a volume. Each volume gets a separate thread.
 */
public class BackgroundContainerDataScanner extends
    AbstractBackgroundContainerScanner {
  public static final Logger LOG =
      LoggerFactory.getLogger(BackgroundContainerDataScanner.class);

  /**
   * The volume that we're scanning.
   */
  private final HddsVolume volume;
  private final ContainerController controller;
  private final DataTransferThrottler throttler;
  private final Canceler canceler;
  private static final String NAME_FORMAT = "ContainerDataScanner(%s)";
  private final ContainerDataScannerMetrics metrics;
  private final long minScanGap;
  private final ContainerChecksumTreeManager checksumManager;

  public BackgroundContainerDataScanner(ContainerScannerConfiguration conf,
                                        ContainerController controller,
                                        HddsVolume volume, ContainerChecksumTreeManager checksumManager) {
    super(String.format(NAME_FORMAT, volume), conf.getDataScanInterval());
    this.controller = controller;
    this.volume = volume;
    throttler = new HddsDataTransferThrottler(conf.getBandwidthPerVolume());
    canceler = new Canceler();
    this.metrics = ContainerDataScannerMetrics.create(volume.toString());
    this.minScanGap = conf.getContainerScanMinGap();
    this.checksumManager = checksumManager;
  }

  private boolean shouldScan(Container<?> container) {
    return container.shouldScanData() &&
        !ContainerUtils.recentlyScanned(container, minScanGap, LOG);
  }

  @Override
  public void scanContainer(Container<?> c)
      throws IOException, InterruptedException {
    // There is one background container data scanner per volume.
    // If the volume fails, its scanning thread should terminate.
    if (volume.isFailed()) {
      shutdown("The volume has failed.");
      return;
    }

    if (!shouldScan(c)) {
      return;
    }
    ContainerData containerData = c.getContainerData();
    long containerId = containerData.getContainerID();
    logScanStart(containerData);
    DataScanResult result = c.scanData(throttler, canceler);

    if (result.isDeleted()) {
      LOG.debug("Container [{}] has been deleted during the data scan.", containerId);
    } else {
      if (!result.isHealthy()) {
        logUnhealthyScanResult(containerId, result, LOG);

        // Only increment the number of unhealthy containers if the container was not already unhealthy.
        // TODO HDDS-11593 (to be merged in to the feature branch from master): Scanner counters will start from zero
        //  at the beginning of each run, so this will need to be incremented for every unhealthy container seen
        //  regardless of its previous state.
        if (controller.markContainerUnhealthy(containerId, result)) {
          metrics.incNumUnHealthyContainers();
        }
      }
      metrics.incNumContainersScanned();
      checksumManager.writeContainerDataTree(containerData, result.getDataTree());
    }

    Instant now = Instant.now();
    if (!result.isDeleted()) {
      controller.updateDataScanTimestamp(containerId, now);
    }
    // Even if the container was deleted, mark the scan as completed since we already logged it as starting.
    logScanCompleted(containerData, now);
  }

  @Override
  public Iterator<Container<?>> getContainerIterator() {
    return controller.getContainers(volume);
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

  @Override
  public synchronized void shutdown() {
    shutdown("");
  }

  private synchronized void shutdown(String reason) {
    String shutdownMessage = String.format(NAME_FORMAT, volume) + " is " +
        "shutting down. " + reason;
    LOG.info(shutdownMessage);
    this.canceler.cancel(shutdownMessage);
    super.shutdown();
  }

  @VisibleForTesting
  @Override
  public ContainerDataScannerMetrics getMetrics() {
    return this.metrics;
  }

  @Override
  public String toString() {
    return String.format(NAME_FORMAT, volume + ", " + volume.getStorageID());
  }

  private class HddsDataTransferThrottler extends DataTransferThrottler {
    HddsDataTransferThrottler(long bandwidthPerSec) {
      super(bandwidthPerSec);
    }

    @Override
    public synchronized void throttle(long numOfBytes) {
      BackgroundContainerDataScanner.this.metrics.incNumBytesScanned(
          numOfBytes);
      super.throttle(numOfBytes);
    }

    @Override
    public synchronized void throttle(long numOfBytes, Canceler c) {
      BackgroundContainerDataScanner.this.metrics.incNumBytesScanned(
          numOfBytes);
      super.throttle(numOfBytes, c);
    }
  }
}
