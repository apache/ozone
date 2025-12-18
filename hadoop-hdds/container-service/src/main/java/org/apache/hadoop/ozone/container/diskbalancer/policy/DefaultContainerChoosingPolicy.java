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

import static java.util.concurrent.TimeUnit.HOURS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Choose a container from specified volume, make sure it's not being balancing.
 */
public class DefaultContainerChoosingPolicy implements ContainerChoosingPolicy {
  public static final Logger LOG = LoggerFactory.getLogger(
      DefaultContainerChoosingPolicy.class);

  private static final ThreadLocal<Cache<HddsVolume, Iterator<Container<?>>>> CACHE =
      ThreadLocal.withInitial(
          () -> CacheBuilder.newBuilder().recordStats().expireAfterAccess(1, HOURS).build());

  // for test
  private static boolean test = false;

  @Override
  public ContainerData chooseContainer(OzoneContainer ozoneContainer,
      HddsVolume srcVolume, HddsVolume destVolume,
      Set<ContainerID> inProgressContainerIDs,
      Double threshold, MutableVolumeSet volumeSet,
      Map<HddsVolume, Long> deltaMap) {
    Iterator<Container<?>> itr;
    try {
      itr = CACHE.get().get(srcVolume,
          () -> ozoneContainer.getController().getContainers(srcVolume));
    } catch (ExecutionException e) {
      LOG.warn("Failed to get container iterator for volume {}", srcVolume, e);
      return null;
    }

    // Calculate maxAllowedUtilization
    ImmutableList<HddsVolume> immutableVolumeSet = DiskBalancerVolumeCalculation.getImmutableVolumeSet(volumeSet);
    double idealUsage = DiskBalancerVolumeCalculation.getIdealUsage(immutableVolumeSet, deltaMap);
    double maxAllowedUtilization = idealUsage + (threshold / 100.0);

    while (itr.hasNext()) {
      ContainerData containerData = itr.next().getContainerData();
      if (containerData.getBytesUsed() > 0 &&
          !inProgressContainerIDs.contains(ContainerID.valueOf(containerData.getContainerID())) &&
          (containerData.isClosed() || (test && containerData.isQuasiClosed()))) {

        // This is a candidate container. Now, check if moving it would be productive.
        if (isMoveProductive(containerData, destVolume, maxAllowedUtilization)) {
          return containerData;
        }
      }
    }

    if (!itr.hasNext()) {
      CACHE.get().invalidate(srcVolume);
    }
    return null;
  }

  /**
   * Checks if moving the given container from source to destination would
   * result in the destination's utilization being less than or equal to the
   * averageUtilization + threshold. This prevents "thrashing" where a move
   * immediately makes the destination a candidate for a source volume.
   *
   * @param containerData The container to be moved.
   * @param destVolume The destination volume.
   * @param maxAllowedUtilization The maximum allowed utilization
   * for the destination volume.
   * @return true if the move is productive, false otherwise.
   */
  private boolean isMoveProductive(ContainerData containerData, HddsVolume destVolume,
      Double maxAllowedUtilization) {
    long containerSize = containerData.getBytesUsed();
    SpaceUsageSource usage = destVolume.getCurrentUsage();

    double newDestUtilization =
        (double) ((usage.getCapacity() - usage.getAvailable()) + destVolume.getCommittedBytes() + containerSize)
            / usage.getCapacity();

    if (newDestUtilization <= maxAllowedUtilization) {
      // The move is productive.
      return true;
    }

    return false;
  }

  @VisibleForTesting
  public static void setTest(boolean isTest) {
    test = isTest;
  }
}
