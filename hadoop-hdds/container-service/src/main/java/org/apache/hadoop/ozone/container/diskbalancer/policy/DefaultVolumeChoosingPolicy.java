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

      // Try source candidates from highest to second-highest utilization
      // For each source, try destinations from the lowest utilization up
      for (int s = volumeUsages.size() - 1; s > 0; s--) {
        for (int d = 0; d < s; d++) {
          final VolumeFixedUsage srcUsage = volumeUsages.get(s);
          final VolumeFixedUsage dstUsage = volumeUsages.get(d);
          
          // If volume[s] was already below the Upper Threshold, then volume[s-1] is definitely below it too.
          // So technically, if we hit this condition, we are done with all balancing for the node.
          if (srcUsage.getUtilization() < upperThreshold && dstUsage.getUtilization() > lowerThreshold) {
            return null; //within threshold
          }

          final HddsVolume dst = dstUsage.getVolume();
          if (containerSize < dstUsage.computeUsableSpace()) {
            // Found dst, reserve space and return
            dst.incCommittedBytes(containerSize);
            return Pair.of(srcUsage.getVolume(), dst);
          }
          LOG.debug("Destination volume {} does not have enough space, trying next volume.",
              dst.getStorageID());
        }
      }
      LOG.debug("Failed to find appropriate destination volume.");
      return null;
    } finally {
      lock.unlock();
    }
  }
}
