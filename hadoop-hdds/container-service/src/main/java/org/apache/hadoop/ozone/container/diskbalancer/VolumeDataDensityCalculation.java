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
import java.util.Map;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone utility class for calculating VolumeDataDensity.
 * This class uses the same logic as DefaultVolumeChoosingPolicy to ensure
 * consistency between operational balancing decisions and reporting.
 * 
 * This calculation is independent and can be used by any component that needs
 * to calculate volume density without depending on DiskBalancerService.
 */
public final class VolumeDataDensityCalculation {
  
  private static final Logger LOG = LoggerFactory.getLogger(VolumeDataDensityCalculation.class);
  
  private VolumeDataDensityCalculation() {
  }
  
  /**
   * Calculate VolumeDataDensity using the same logic as DefaultVolumeChoosingPolicy.
   * This ensures consistency between operational balancing decisions and reporting.
   * 
   * @param volumeSet The MutableVolumeSet containing all volumes
   * @param deltaMap Map of volume to delta sizes (ongoing operations), can be null
   * @return VolumeDataDensity sum across all volumes
   */
  public static double calculate(MutableVolumeSet volumeSet, Map<HddsVolume, Long> deltaMap) {
    if (volumeSet == null) {
      LOG.warn("VolumeSet is null, returning 0.0 for VolumeDataDensity");
      return 0.0;
    }
    
    try {
      double volumeDensitySum = 0.0;

      // Get ideal usage
      double idealUsage = volumeSet.getIdealUsage();
      
      // Calculate density for each volume
      for (HddsVolume volume : StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())) {
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
      return 0.0;
    }
  }
}
