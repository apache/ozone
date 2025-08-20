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

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.AvailableSpaceFilter;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
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
      double threshold, Map<HddsVolume, Long> deltaMap, long containerSize) {
    lock.lock();
    try {
      double idealUsage = volumeSet.getIdealUsage();

      // Threshold is given as a percentage
      double normalizedThreshold = threshold / 100;
      List<HddsVolume> volumes = StorageVolumeUtil
          .getHddsVolumesList(volumeSet.getVolumesList())
          .stream()
          .filter(volume -> {
            SpaceUsageSource usage = volume.getCurrentUsage();

            return Math.abs(
                  ((double)((usage.getCapacity() - usage.getAvailable())
                      + deltaMap.getOrDefault(volume, 0L) + volume.getCommittedBytes()))
                      / usage.getCapacity() - idealUsage) >= normalizedThreshold;

          }).sorted((v1, v2) -> {
            SpaceUsageSource usage1 = v1.getCurrentUsage();
            SpaceUsageSource usage2 = v2.getCurrentUsage();

            return Double.compare(
                  (double) ((usage2.getCapacity() - usage2.getAvailable())
                      + deltaMap.getOrDefault(v2, 0L) + v2.getCommittedBytes()) /
                      usage2.getCapacity(),
                  (double) ((usage1.getCapacity() - usage1.getAvailable())
                      + deltaMap.getOrDefault(v1, 0L) + v1.getCommittedBytes()) /
                      usage1.getCapacity());
          }).collect(Collectors.toList());

      // Can not generate DiskBalancerTask if we have less than 2 results
      if (volumes.size() <= 1) {
        LOG.debug("Can not find appropriate Source volume and Dest Volume.");
        return null;
      }
      AvailableSpaceFilter filter = new AvailableSpaceFilter(containerSize);
      HddsVolume srcVolume = volumes.get(0);
      HddsVolume destVolume = volumes.get(volumes.size() - 1);
      while (!filter.test(destVolume)) {
        // If the destination volume does not have enough space, try the next
        // one in the list.
        LOG.debug("Destination volume {} does not have enough space, trying next volume.",
            destVolume.getStorageID());
        volumes.remove(destVolume);
        if (volumes.size() <= 1) {
          LOG.debug("Can not find appropriate Source volume and Dest Volume.");
          return null;
        }
        destVolume = volumes.get(volumes.size() - 1);
      }
      // reserve space for the dest volume
      destVolume.incCommittedBytes(containerSize);
      return Pair.of(srcVolume, destVolume);
    } finally {
      lock.unlock();
    }
  }
}
