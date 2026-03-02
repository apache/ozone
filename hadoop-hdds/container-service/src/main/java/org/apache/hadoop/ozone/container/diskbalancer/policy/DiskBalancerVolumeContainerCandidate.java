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

import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;

/**
 * Result of consolidated volume and container selection for disk balancing.
 * Contains the container to move and its source and destination volumes.
 */
public final class DiskBalancerVolumeContainerCandidate {
  private final ContainerData containerData;
  private final HddsVolume sourceVolume;
  private final HddsVolume destVolume;

  public DiskBalancerVolumeContainerCandidate(ContainerData containerData,
      HddsVolume sourceVolume, HddsVolume destVolume) {
    this.containerData = containerData;
    this.sourceVolume = sourceVolume;
    this.destVolume = destVolume;
  }

  public ContainerData getContainerData() {
    return containerData;
  }

  public HddsVolume getSourceVolume() {
    return sourceVolume;
  }

  public HddsVolume getDestVolume() {
    return destVolume;
  }
}
