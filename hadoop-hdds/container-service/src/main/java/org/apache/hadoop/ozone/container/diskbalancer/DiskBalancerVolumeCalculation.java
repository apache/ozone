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

import static org.apache.ratis.util.Preconditions.assertInstanceOf;
import static org.apache.ratis.util.Preconditions.assertTrue;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
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
   * @return a list of volumes and usages
   */
  public static List<VolumeFixedUsage> getVolumeUsages(MutableVolumeSet volumeSet) {
    return volumeSet.getVolumesList().stream()
        .map(VolumeFixedUsage::new)
        .collect(Collectors.toList());
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
  public static double getIdealUsage(List<VolumeFixedUsage> volumes, Map<HddsVolume, Long> deltaMap) {
    long totalCapacity = 0L, totalEffectiveUsed = 0L;
    
    for (VolumeFixedUsage volumeUsage : volumes) {
      final HddsVolume volume = volumeUsage.getVolume();
      final SpaceUsageSource.Fixed usage = volumeUsage.getUsage();
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
  public static double calculateVolumeDataDensity(List<VolumeFixedUsage> volumeSet,
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
      for (VolumeFixedUsage volumeUsage : volumeSet) {
        final double currentUsage = volumeUsage.computeUtilization(deltaMap);

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

  public static double computeUtilization(HddsVolume volume, long delta) {
    return computeUtilization(volume.getCurrentUsage(), volume.getCommittedBytes(), delta);
  }

  private static double computeUtilization(SpaceUsageSource.Fixed usage, long committed, long delta) {
    assertTrue(usage.getCapacity() > 0);
    return (usage.getCapacity() - usage.getAvailable() + committed + delta) / (double) usage.getCapacity();
  }

  /** {@link HddsVolume} with a {@link SpaceUsageSource.Fixed} usage. */
  public static class VolumeFixedUsage {
    private final HddsVolume volume;
    private final SpaceUsageSource.Fixed usage;

    public VolumeFixedUsage(HddsVolume volume) {
      this.volume = volume;
      this.usage = volume.getCurrentUsage();
    }

    public VolumeFixedUsage(StorageVolume volume) {
      this(assertInstanceOf(volume, HddsVolume.class));
    }

    public HddsVolume getVolume() {
      return volume;
    }

    public SpaceUsageSource.Fixed getUsage() {
      return usage;
    }

    public double computeUtilization(Map<HddsVolume, Long> deltas) {
      final long delta = deltas == null ? 0L : deltas.getOrDefault(volume, 0L);
      return DiskBalancerVolumeCalculation.computeUtilization(usage, volume.getCommittedBytes(), delta);
    }
  }
}
