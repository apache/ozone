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

package org.apache.hadoop.ozone.container.diskbalancer.policy;

import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.getIdealUsage;
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.newVolumeFixedUsage;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.VolumeFixedUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Choose a random volume for disk balancing.
 *
 * Source volumes use deltaMap to simulate space that will be freed (pre-deleted).
 * Destination volumes use committedBytes to account for space already reserved.
 * Both deltaMap and committedBytes are considered to calculate usage.
 */
public class DefaultVolumeChoosingPolicy implements DiskBalancerVolumeChoosingPolicy {

  public static final Logger LOG = LoggerFactory.getLogger(
      DefaultVolumeChoosingPolicy.class);
  private final ReentrantLock lock;

  public DefaultVolumeChoosingPolicy(ReentrantLock globalLock) {
    lock = globalLock;
  }

  @Override
  public Pair<HddsVolume, HddsVolume> chooseVolume(MutableVolumeSet volumeSet,
      double thresholdPercentage, Map<HddsVolume, Long> deltaMap, long containerSize) {
    lock.lock();
    try {
      // Create truly immutable snapshot of volumes to ensure consistency
      final List<StorageVolume> allVolumes = volumeSet.getVolumesList();
      if (allVolumes.size() < 2) {
        return null; // Can't balance with less than 2 volumes.
      }

      // Calculate usages and sort in ascending order of utilization
      final List<VolumeFixedUsage> volumeUsages = allVolumes.stream()
          .map(v -> newVolumeFixedUsage(v, deltaMap))
          .sorted(Comparator.comparingDouble(VolumeFixedUsage::getUtilization))
          .collect(Collectors.toList());

      // Calculate ideal usage and threshold range
      final double idealUsage = getIdealUsage(volumeUsages);
      final double actualThreshold = thresholdPercentage / 100.0;
      final double lowerThreshold = idealUsage - actualThreshold;
      final double upperThreshold = idealUsage + actualThreshold;

      // Get highest and lowest utilization volumes
      final VolumeFixedUsage highestUsage = volumeUsages.get(volumeUsages.size() - 1);
      final VolumeFixedUsage lowestUsage = volumeUsages.get(0);

      // Only return null if highest is below upper threshold AND lowest is above lower threshold
      // This means all volumes are strictly within the range (not at boundaries)
      if (highestUsage.getUtilization() < upperThreshold && 
          lowestUsage.getUtilization() > lowerThreshold) {
        // All volumes are strictly within threshold range, no balancing needed
        return null;
      }

      // Determine source volume: highest utilization volume (if above threshold) 
      final VolumeFixedUsage src = highestUsage;

      // Find destination volume: lowest utilization volume that has enough space
      // Prefer volumes below threshold, but accept any volume with lower utilization than source
      for (int i = 0; i < volumeUsages.size() - 1; i++) {
        final VolumeFixedUsage dstUsage = volumeUsages.get(i);
        final HddsVolume dst = dstUsage.getVolume();

        // Check if destination has enough space and has lower utilization than source
        if (dstUsage.getUtilization() < src.getUtilization() &&
            containerSize < dstUsage.computeUsableSpace()) {
          // Found dst, reserve space and return
          dst.incCommittedBytes(containerSize);
          return Pair.of(src.getVolume(), dst);
        }
        LOG.debug("Destination volume {} does not have enough space or utilization, trying next volume.",
            dst.getStorageID());
      }
      LOG.debug("Failed to find appropriate destination volume.");
      return null;
    } finally {
      lock.unlock();
    }
  }
}
