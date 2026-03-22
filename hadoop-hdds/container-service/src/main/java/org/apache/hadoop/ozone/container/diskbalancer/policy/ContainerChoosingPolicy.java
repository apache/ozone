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
import java.util.Set;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;

/**
 * This interface specifies the policy for choosing volumes and containers to balance.
 * It provides consolidated volume selection (source/destination pair) and container selection
 * into a single operation to avoid recalculating ideal utilization and disk usage.
 */
public interface ContainerChoosingPolicy {
  /**
   * Choose a container and its source/destination volumes for balancing.
   * Performs both volume pair selection and container selection in one call,
   * computing ideal usage and volume utilizations only once.
   * Space is reserved on the destination only when a container is chosen,
   * using the actual container size.
   *
   * @param ozoneContainer the OzoneContainer instance to get all containers
   * @param volumeSet the volumeSet instance
   * @param deltaMap the deltaMap for in-progress balancing jobs (negative = space to be freed)
   * @param inProgressContainerIDs containerIDs to avoid (already under move)
   * @param thresholdPercentage the threshold percentage in range (0, 100)
   * @return a DiskBalancerVolumeContainerCandidate with container and volumes, or null if none found
   */
  ContainerCandidate chooseVolumesAndContainer(OzoneContainer ozoneContainer,
      MutableVolumeSet volumeSet,
      Map<HddsVolume, Long> deltaMap,
      Set<ContainerID> inProgressContainerIDs,
      double thresholdPercentage);
}
