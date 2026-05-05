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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeUsage;
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
  public static List<VolumeFixedUsage> getVolumeUsages(MutableVolumeSet volumeSet, Map<HddsVolume, Long> deltas) {
    return volumeSet.getVolumesList().stream()
        .map(v -> newVolumeFixedUsage(v, deltas))
        .collect(Collectors.toList());
  }
  
  /**
   * Get ideal usage from an immutable list of volumes.
   * 
   * @param volumes Immutable list of volumes
   * from each source volume during container moves
   * @return Ideal usage as a ratio (used space / total capacity)
   * @throws IllegalArgumentException if total capacity is zero
   */
  public static double getIdealUsage(List<VolumeFixedUsage> volumes) {
    long totalCapacity = 0L, totalEffectiveUsed = 0L;
    
    for (VolumeFixedUsage volumeUsage : volumes) {
      totalCapacity += volumeUsage.getUsage().getCapacity();
      totalEffectiveUsed += volumeUsage.getEffectiveUsed();
    }
    
    return ((double) (totalEffectiveUsed)) / totalCapacity;
  }
  
  /**
   * Calculate VolumeDataDensity.
   * 
   * @param volumeSet The MutableVolumeSet containing all volumes
   * @return VolumeDataDensity sum across all volumes
   */
  public static double calculateVolumeDataDensity(List<VolumeFixedUsage> volumeSet) {
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
      final double idealUsage = getIdealUsage(volumeSet);
      double volumeDensitySum = 0.0;

      // Calculate density for each volume using the same snapshot
      for (VolumeFixedUsage volumeUsage : volumeSet) {
        final double currentUsage = volumeUsage.getUtilization();

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

  public static double computeUtilization(SpaceUsageSource.Fixed usage, long committed, long required) {
    final long capacity = usage.getCapacity();
    assertTrue(capacity > 0, () -> "capacity = " + capacity + " <= 0");
    return computeEffectiveUsage(usage, committed, required) / (double) capacity;
  }

  private static long computeEffectiveUsage(SpaceUsageSource.Fixed usage, long committed, long required) {
    return usage.getCapacity() - usage.getAvailable() + committed + required;
  }

  public static VolumeFixedUsage newVolumeFixedUsage(StorageVolume volume, Map<HddsVolume, Long> deltaMap) {
    final HddsVolume v = assertInstanceOf(volume, HddsVolume.class);
    final long delta = deltaMap == null ? 0 : deltaMap.getOrDefault(v, 0L);
    return new VolumeFixedUsage(v, delta);
  }

  /** {@link HddsVolume} with a {@link SpaceUsageSource.Fixed} usage. */
  public static final class VolumeFixedUsage {
    private final HddsVolume volume;
    private final SpaceUsageSource.Fixed usage;
    private final long effectiveUsed;
    private final Double utilization;

    private VolumeFixedUsage(HddsVolume volume, long delta) {
      this.volume = volume;
      this.usage = volume.getCurrentUsage();
      this.effectiveUsed = computeEffectiveUsage(usage, volume.getCommittedBytes(), delta);
      this.utilization = usage.getCapacity() > 0 ? computeUtilization(usage, volume.getCommittedBytes(), delta) : null;
    }

    public HddsVolume getVolume() {
      return volume;
    }

    public SpaceUsageSource.Fixed getUsage() {
      return usage;
    }

    public long getEffectiveUsed() {
      return effectiveUsed;
    }

    public double getUtilization() {
      return Objects.requireNonNull(utilization,  "utilization == null");
    }

    public long computeUsableSpace() {
      final long spared = volume.getFreeSpaceToSpare(usage.getCapacity());
      return VolumeUsage.getUsableSpace(usage.getAvailable(), volume.getCommittedBytes(), spared);
    }
  }
}
