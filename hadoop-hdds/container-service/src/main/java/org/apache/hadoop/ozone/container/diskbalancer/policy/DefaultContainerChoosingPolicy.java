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
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.computeUtilization;
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.getIdealUsage;
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.getVolumeUsages;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.VolumeFixedUsage;
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
      HddsVolume src, HddsVolume dst,
      Set<ContainerID> inProgressContainerIDs,
      double thresholdPercentage, MutableVolumeSet volumeSet,
      Map<HddsVolume, Long> deltaMap) {
    final Iterator<Container<?>> itr;
    try {
      itr = CACHE.get().get(src, () -> ozoneContainer.getController().getContainers(src));
    } catch (ExecutionException e) {
      LOG.warn("Failed to get container iterator for volume {}", src, e);
      return null;
    }

    // Calculate the actual threshold
    final List<VolumeFixedUsage> volumeUsages = getVolumeUsages(volumeSet, deltaMap);
    final double actualThreshold = getIdealUsage(volumeUsages) + thresholdPercentage / 100.0;

    // Find container
    final SpaceUsageSource.Fixed dstUsage = dst.getCurrentUsage();
    final long dstCommittedBytes = dst.getCommittedBytes();
    while (itr.hasNext()) {
      ContainerData containerData = itr.next().getContainerData();
      if (containerData.getBytesUsed() > 0 &&
          !inProgressContainerIDs.contains(ContainerID.valueOf(containerData.getContainerID())) &&
          (containerData.isClosed() || (test && containerData.isQuasiClosed()))) {

        // Check if dst can accept the candidate container.
        if (computeUtilization(dstUsage, dstCommittedBytes, containerData.getBytesUsed()) < actualThreshold) {
          return containerData;
        }
      }
    }

    CACHE.get().invalidate(src);
    return null;
  }

  @VisibleForTesting
  public static void setTest(boolean isTest) {
    test = isTest;
  }
}
