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

package org.apache.hadoop.ozone.container.diskbalancer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for disk balancer volume calculations.
 * 
 * This class provides shared functionality for
 * creating immutable volume snapshots,
 * calculating ideal usage, and
 * volume data density
 * ensuring consistency across all disk balancing components and preventing race conditions.
 */
public final class DiskBalancerVolumeCalculation {
  
  private static final Logger LOG = LoggerFactory.getLogger(DiskBalancerVolumeCalculation.class);
  
  private DiskBalancerVolumeCalculation() {
    // Utility class - prevent instantiation
  }
  
  /**
   * Get an immutable snapshot of volumes from a MutableVolumeSet.
   * 
   * @param volumeSet The MutableVolumeSet to create a snapshot from
   * @return Immutable list of HddsVolume objects
   */
  public static ImmutableList<HddsVolume> getImmutableVolumeSet(MutableVolumeSet volumeSet) {
    // Create an immutable copy of the volume list at this point in time
    List<HddsVolume> volumes = StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList());
    return ImmutableList.copyOf(volumes);
  }
  
  /**
   * Get ideal usage from an immutable list of volumes.
   * 
   * @param volumes Immutable list of volumes
   * @param deltaMap A map that tracks the total bytes which will be freed
   * from each source volume during container moves
   * @return Ideal usage as a ratio (used space / total capacity)
   * @throws IllegalArgumentException if total capacity is zero
   */
  public static double getIdealUsage(ImmutableList<HddsVolume> volumes,
      Map<HddsVolume, Long> deltaMap) {
    long totalCapacity = 0L, totalEffectiveUsed = 0L;
    
    for (HddsVolume volume : volumes) {
      SpaceUsageSource usage = volume.getCurrentUsage();
      totalCapacity += usage.getCapacity();
      long currentUsed = usage.getCapacity() - usage.getAvailable();
      long delta = (deltaMap != null) ? deltaMap.getOrDefault(volume, 0L) : 0L;
      long committed = volume.getCommittedBytes();
      totalEffectiveUsed += (currentUsed + delta + committed);
    }
    
    Preconditions.checkArgument(totalCapacity != 0);
    return ((double) (totalEffectiveUsed)) / totalCapacity;
  }
  
  /**
   * Calculate VolumeDataDensity.
   * 
   * @param volumeSet The MutableVolumeSet containing all volumes
   * @param deltaMap Map of volume to delta sizes (ongoing operations), can be null
   * @return VolumeDataDensity sum across all volumes
   */
  public static double calculateVolumeDataDensity(ImmutableList<HddsVolume> volumeSet,
      Map<HddsVolume, Long> deltaMap) {
    if (volumeSet == null) {
      LOG.warn("VolumeSet is null, returning 0.0 for VolumeDataDensity");
      return 0.0;
    }
    
    try {
      // If there is only one volume, return 0.0 as there's no imbalance to measure
      if (volumeSet.size() <= 1) {
        return 0.0;
      }

      // Calculate ideal usage using the same immutable volume snapshot
      double idealUsage = getIdealUsage(volumeSet, deltaMap);
      double volumeDensitySum = 0.0;

      // Calculate density for each volume using the same snapshot
      for (HddsVolume volume : volumeSet) {
        SpaceUsageSource usage = volume.getCurrentUsage();
        Preconditions.checkArgument(usage.getCapacity() != 0);

        long deltaSize = (deltaMap != null) ? deltaMap.getOrDefault(volume, 0L) : 0L;
        double currentUsage = (double)((usage.getCapacity() - usage.getAvailable())
            + deltaSize + volume.getCommittedBytes()) / usage.getCapacity();
        
        // Calculate density as absolute difference from ideal usage
        double volumeDensity = Math.abs(currentUsage - idealUsage);
        volumeDensitySum += volumeDensity;
      }
      return volumeDensitySum;
    } catch (Exception e) {
      LOG.error("Error calculating VolumeDataDensity", e);
      return -1.0;
    }
  }
}
