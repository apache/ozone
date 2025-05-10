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

package org.apache.hadoop.ozone.container.common.volume;

import static org.apache.hadoop.ozone.container.common.volume.VolumeChoosingUtil.logIfSomeVolumesOutOfSpace;
import static org.apache.hadoop.ozone.container.common.volume.VolumeChoosingUtil.throwDiskOutOfSpace;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Choose volumes in round-robin order.
 * The caller should synchronize access to the list of volumes.
 */
public class RoundRobinVolumeChoosingPolicy implements VolumeChoosingPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(
      RoundRobinVolumeChoosingPolicy.class);

  // Stores the index of the next volume to be returned.
  private int nextVolumeIndex = 0;

  @Override
  public synchronized HddsVolume chooseVolume(List<HddsVolume> volumes,
      long maxContainerSize) throws IOException {

    // No volumes available to choose from
    if (volumes.isEmpty()) {
      throw new DiskOutOfSpaceException("No more available volumes");
    }

    AvailableSpaceFilter filter = new AvailableSpaceFilter(maxContainerSize);

    // since volumes could've been removed because of the failure
    // make sure we are not out of bounds
    int currentVolumeIndex = nextVolumeIndex < volumes.size() ? nextVolumeIndex : 0;

    int startVolumeIndex = currentVolumeIndex;

    while (true) {
      final HddsVolume volume = volumes.get(currentVolumeIndex);
      // adjust for remaining capacity in Open containers
      boolean hasEnoughSpace = filter.test(volume);

      currentVolumeIndex = (currentVolumeIndex + 1) % volumes.size();

      if (hasEnoughSpace) {
        logIfSomeVolumesOutOfSpace(filter, LOG);
        nextVolumeIndex = currentVolumeIndex;
        volume.incCommittedBytes(maxContainerSize);
        return volume;
      }

      if (currentVolumeIndex == startVolumeIndex) {
        throwDiskOutOfSpace(filter, LOG);
      }
    }
  }
}
