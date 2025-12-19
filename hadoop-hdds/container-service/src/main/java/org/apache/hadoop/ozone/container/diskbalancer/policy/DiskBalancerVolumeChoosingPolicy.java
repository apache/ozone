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

import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;

/**
 * This interface specifies the policy for choosing volumes to balance.
 */
public interface DiskBalancerVolumeChoosingPolicy {
  /**
   * Choose a pair of volumes for balancing.
   *
   * @param volumeSet - volumes to choose from.
   * @param thresholdPercentage the threshold percentage in range [0, 100] to choose the source volume.
   * @param deltaSizes - the sizes changes of inProgress balancing jobs.
   * @param containerSize - the estimated size of container to be moved.
   * @return Source volume and Dest volume.
   */
  Pair<HddsVolume, HddsVolume> chooseVolume(MutableVolumeSet volumeSet,
      double thresholdPercentage, Map<HddsVolume, Long> deltaSizes, long containerSize);
}
