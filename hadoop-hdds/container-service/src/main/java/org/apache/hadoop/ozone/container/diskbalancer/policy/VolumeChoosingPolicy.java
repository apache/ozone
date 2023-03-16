package org.apache.hadoop.ozone.container.diskbalancer.policy;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;

import java.util.Map;

/**
 * This interface specifies the policy for choosing volumes to balance.
 */
public interface VolumeChoosingPolicy {
  /**
   * Choose a pair of volumes for balancing.
   *
   * @param volumeSet - volumes to choose from.
   * @param threshold - the threshold to choose source and dest volumes.
   * @param deltaSizes - the sizes changes of inProgress balancing jobs.
   * @return Source volume and Dest volume.
   */
  Pair<HddsVolume, HddsVolume> chooseVolume(MutableVolumeSet volumeSet,
      double threshold, Map<HddsVolume, Long> deltaSizes);
}
