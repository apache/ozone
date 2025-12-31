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
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;

/**
 * This interface specifies the policy for choosing containers to balance.
 */
public interface ContainerChoosingPolicy {
  /**
   * Choose a container for balancing.
   * @param ozoneContainer the OzoneContainer instance to get all containers of a particular volume.
   * @param srcVolume the HddsVolume instance to choose containers from.
   * @param destVolume the destination volume to which container is being moved.
   * @param inProgressContainerIDs containerIDs present in this set should be
   - avoided as these containers are already under move by diskBalancer.
   * @param thresholdPercentage the threshold percentage in range (0, 100)
   * @param volumeSet the volumeSet instance
   * @param deltaMap the deltaMap instance of source volume
   * @return a Container
   */
  ContainerData chooseContainer(OzoneContainer ozoneContainer,
      HddsVolume srcVolume, HddsVolume destVolume,
      Set<ContainerID> inProgressContainerIDs,
      double thresholdPercentage, MutableVolumeSet volumeSet,
      Map<HddsVolume, Long> deltaMap);
}
