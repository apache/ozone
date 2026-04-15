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

package org.apache.hadoop.ozone.container.common.volume;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;

/**
 * Filter for selecting volumes with enough space for a new container.
 * Uses the <em>hard</em> min-free spare (same as write checks), not the SCM-reported spare in
 * {@link StorageLocationReport#getFreeSpaceToSpare()}. The gap between reported and hard is the
 * soft band (e.g. 40GB − 30GB on a 2000GB disk with 2% vs 1.5%).
 * Keeps track of ineligible volumes for logging/debug purposes.
 */
public class AvailableSpaceFilter implements Predicate<HddsVolume> {

  private final long requiredSpace;
  private final List<StorageLocationReport> fullVolumes = new LinkedList<>();
  private long mostAvailableSpace = Long.MIN_VALUE;

  public AvailableSpaceFilter(long requiredSpace) {
    this.requiredSpace = requiredSpace;
  }

  @Override
  public boolean test(HddsVolume vol) {
    StorageLocationReport report = vol.getReport();
    long capacity = report.getCapacity();
    long spareAtHardLimit = vol.getFreeSpaceToSpare(capacity);
    long spareReported = vol.getReportedFreeSpaceToSpare(capacity);
    long available =
        report.getRemaining() - report.getCommitted() - spareAtHardLimit;
    long availableAtReportedSpare =
        report.getRemaining() - report.getCommitted() - spareReported;

    boolean hasEnoughSpace = available > requiredSpace;

    VolumeInfoMetrics stats = vol.getVolumeInfoStats();
    if (stats != null) {
      if (!hasEnoughSpace) {
        stats.incNumContainerCreateRequestsRejectedHardMinFreeSpace();
      } else if (availableAtReportedSpare <= requiredSpace) {
        stats.incNumContainerCreateRequestsInSoftBandMinFreeSpace();
      }
    }

    mostAvailableSpace = Math.max(available, mostAvailableSpace);

    if (!hasEnoughSpace) {
      fullVolumes.add(report);
    }

    return hasEnoughSpace;
  }

  boolean foundFullVolumes() {
    return !fullVolumes.isEmpty();
  }

  long mostAvailableSpace() {
    return mostAvailableSpace;
  }

  @Override
  public String toString() {
    return "required space: " + requiredSpace +
        ", volumes: " + fullVolumes;
  }
}
