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
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.newVolumeFixedUsage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.VolumeFixedUsage;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Volumes are grouped by {@link StorageType} and each group is balanced on its own, so a container
 * never moves between volumes of different storage types. Within a group, this first chooses a
 * source volume and destination volume pair based on ideal utilization and threshold, then chooses
 * a container from the source volume that can be moved to the destination without exceeding the
 * upper threshold. Space is reserved on the destination only when a container is chosen, using the
 * actual container size.
 *
 * A storage type with fewer than two usable volumes is skipped rather than paired with a volume of
 * another type, because moving a container across types would break the storage policy its data was
 * placed for.
 *
 * Which container states may move is determined by {@code DiskBalancerConfiguration#getMovableContainerStates()}.
 */
public class DefaultContainerChoosingPolicy implements ContainerChoosingPolicy {
  public static final Logger LOG = LoggerFactory.getLogger(
      DefaultContainerChoosingPolicy.class);

  private static final ThreadLocal<Cache<HddsVolume, Iterator<Container<?>>>> CACHE =
      ThreadLocal.withInitial(
          () -> CacheBuilder.newBuilder().recordStats().expireAfterAccess(1, HOURS).build());

  private final ReentrantLock lock;

  public DefaultContainerChoosingPolicy(ReentrantLock globalLock) {
    this.lock = globalLock;
  }

  @Override
  public ContainerCandidate chooseVolumesAndContainer(OzoneContainer ozoneContainer,
      MutableVolumeSet volumeSet, Map<HddsVolume, Long> deltaMap, Set<ContainerID> inProgressContainerIDs,
      double thresholdPercentage, Set<State> movableContainerStates) {
    lock.lock();
    try {
      // Create truly immutable snapshot of volumes to ensure consistency
      final List<StorageVolume> allVolumes = volumeSet.getVolumesList();
      if (allVolumes.size() < 2) {
        return null; // Can't balance with less than 2 volumes.
      }

      // Calculate usages and sort in ascending order of utilization (once)
      // Use storage ID as secondary sort for deterministic ordering when utilizations are equal
      final List<VolumeFixedUsage> volumeUsages = allVolumes.stream()
          .map(v -> newVolumeFixedUsage(v, deltaMap))
          .filter(DefaultContainerChoosingPolicy::hasPositiveCapacity)
          .sorted(Comparator.comparingDouble(VolumeFixedUsage::getUtilization)
              .thenComparing(v -> v.getVolume().getStorageID()))
          .collect(Collectors.toList());
      if (volumeUsages.size() < 2) {
        return null;
      }

      // Balance each storage type independently so a container never crosses types. Iterate
      // StorageType.values() rather than the grouping keys to keep the order stable across runs.
      final Map<StorageType, List<VolumeFixedUsage>> usagesByStorageType = volumeUsages.stream()
          .collect(Collectors.groupingBy(v -> v.getVolume().getStorageType()));
      for (StorageType storageType : StorageType.values()) {
        final List<VolumeFixedUsage> sameTypeUsages = usagesByStorageType.get(storageType);
        if (sameTypeUsages == null || sameTypeUsages.size() < 2) {
          if (sameTypeUsages != null) {
            LOG.debug("Skipping storage type {} for disk balancing: only {} usable volume(s)",
                storageType, sameTypeUsages.size());
          }
          continue;
        }
        final ContainerCandidate candidate = chooseWithinStorageType(ozoneContainer, sameTypeUsages,
            deltaMap, inProgressContainerIDs, thresholdPercentage, movableContainerStates, storageType);
        if (candidate != null) {
          return candidate;
        }
      }
      LOG.debug("Failed to find appropriate destination volume and container in any storage type.");
      return null;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Chooses a source volume, destination volume and container among volumes that all share
   * {@code storageType}.
   *
   * @param volumeUsages usable volumes of a single storage type, sorted ascending by utilization
   * @return a candidate, or null if this storage type has nothing to move
   */
  @SuppressWarnings("checkstyle:parameternumber")
  private ContainerCandidate chooseWithinStorageType(OzoneContainer ozoneContainer,
      List<VolumeFixedUsage> volumeUsages, Map<HddsVolume, Long> deltaMap,
      Set<ContainerID> inProgressContainerIDs, double thresholdPercentage,
      Set<State> movableContainerStates, StorageType storageType) {
    // Calculate ideal usage and threshold range (once)
    final double idealUsage = getIdealUsage(volumeUsages);
    final double actualThreshold = thresholdPercentage / 100.0;
    final double lowerThreshold = idealUsage - actualThreshold;
    final double upperThreshold = idealUsage + actualThreshold;

    if (LOG.isDebugEnabled()) {
      logVolumeBalancingState(volumeUsages, idealUsage, thresholdPercentage,
          lowerThreshold, upperThreshold, deltaMap);
    }

    // Get highest and lowest utilization volumes
    final VolumeFixedUsage highestUsage = volumeUsages.get(volumeUsages.size() - 1);
    final VolumeFixedUsage lowestUsage = volumeUsages.get(0);

    // Only return null if highest is below upper threshold AND lowest is above lower threshold
    if (highestUsage.getUtilization() < upperThreshold &&
        lowestUsage.getUtilization() > lowerThreshold) {
      return null;
    }

    // Determine source volume: highest utilization volume
    final VolumeFixedUsage srcUsage = highestUsage;
    final HddsVolume src = srcUsage.getVolume();

    // Find destination volume and container: try each dest with lower utilization than source
    for (int i = 0; i < volumeUsages.size() - 1; i++) {
      final VolumeFixedUsage dstUsage = volumeUsages.get(i);
      final HddsVolume dst = dstUsage.getVolume();

      // Check if destination has lower utilization than source and some usable space
      if (dstUsage.getUtilization() < srcUsage.getUtilization() &&
          dstUsage.computeUsableSpace() > 0) {
        ContainerData containerData = chooseContainer(ozoneContainer,
            src, dst, dstUsage, inProgressContainerIDs, upperThreshold, movableContainerStates);
        if (containerData != null) {
          long containerSize = containerData.getBytesUsed();
          dst.incCommittedBytes(containerSize);
          LOG.debug("Chosen volume pair for disk balancing on storage type {}: source={} "
                  + "(utilization={}), destination={} (utilization={})",
              storageType, src.getStorageDir().getPath(), srcUsage.getUtilization(),
              dst.getStorageDir().getPath(), dstUsage.getUtilization());
          return new ContainerCandidate(containerData, src, dst);
        }
        LOG.debug("No container to move for destination {}, trying next volume.",
            dst.getStorageDir().getPath());
      } else {
        LOG.debug("Destination volume {} does not have enough space, trying next volume.",
            dst.getStorageDir().getPath());
      }
    }
    LOG.debug("Failed to find appropriate destination volume and container on storage type {}.",
        storageType);
    return null;
  }

  private static boolean hasPositiveCapacity(VolumeFixedUsage volumeUsage) {
    long capacity = volumeUsage.getUsage().getCapacity();
    if (capacity > 0) {
      return true;
    }
    LOG.debug("Skipping volume {} for disk balancing because capacity is {}",
        volumeUsage.getVolume(), capacity);
    return false;
  }

  /**
   * Finds a container on {@code src} that can move to {@code dst}.
   */
  private ContainerData chooseContainer(OzoneContainer ozoneContainer,
      HddsVolume src, HddsVolume dst, VolumeFixedUsage dstUsage,
      Set<ContainerID> inProgressContainerIDs, double upperThreshold, Set<State> movableContainerStates) {
    final Iterator<Container<?>> itr;
    try {
      itr = CACHE.get().get(src, () -> ozoneContainer.getController().getContainers(src));
    } catch (ExecutionException e) {
      LOG.warn("Failed to get container iterator for volume {}", src, e);
      return null;
    }

    final SpaceUsageSource.Fixed dstSpaceUsage = dstUsage.getUsage();
    final long dstCommittedBytes = dst.getCommittedBytes();
    final long usableSpace = dstUsage.computeUsableSpace();

    while (itr.hasNext()) {
      ContainerData containerData = itr.next().getContainerData();
      long containerId = containerData.getContainerID();

      if (ozoneContainer.getContainerSet().getContainer(containerId) == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping container {} from volume {}: not in container set "
                  + "(removed after iterator was cached)",
              containerId, src.getStorageDir().getPath());
        }
        continue;
      }

      if (inProgressContainerIDs.contains(ContainerID.valueOf(containerId))) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping container {} from volume {}: disk balancer move already in progress",
              containerId, src.getStorageDir().getPath());
        }
        continue;
      }

      long containerSize = containerData.getBytesUsed();
      if (containerSize <= 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping container {} from volume {}: bytes used is {}",
              containerId, src.getStorageDir().getPath(), containerData.getBytesUsed());
        }
        continue;
      }

      if (!movableContainerStates.contains(containerData.getState())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping container {} from volume {}: state is {}. Allowed container states: {}",
              containerId, src.getStorageDir().getPath(), containerData.getState(), movableContainerStates);
        }
        continue;
      }

      if (containerSize >= usableSpace) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping container {} ({}B) from volume {}: exceeds destination {} "
                  + "usable space {}B", containerId, containerSize, src.getStorageDir().getPath(),
              dst.getStorageDir().getPath(), usableSpace);
        }
        continue;
      }

      double newUtilization = computeUtilization(dstSpaceUsage, dstCommittedBytes, containerSize);
      if (newUtilization >= upperThreshold) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping container {} ({}B) from volume {}: moving to {} would "
                  + "result in utilization {} exceeding upper threshold {}",
              containerId, containerSize, src.getStorageDir().getPath(), dst.getStorageDir().getPath(),
              newUtilization, upperThreshold);
        }
        continue;
      }

      return containerData;
    }

    CACHE.get().invalidate(src);
    return null;
  }

  /**
   * Logs all volume information for disk balancing investigation.
   */
  private void logVolumeBalancingState(List<VolumeFixedUsage> volumeUsages,
      double idealUsage, double thresholdPercentage, double lowerThreshold,
      double upperThreshold, Map<HddsVolume, Long> deltaMap) {
    LOG.debug("Disk balancing state - idealUsage={}, thresholdPercentage={}%, "
            + "thresholdRange=({}, {})",
        String.format("%.10f", idealUsage), thresholdPercentage,
        String.format("%.10f", lowerThreshold), String.format("%.10f", upperThreshold));
    for (int i = 0; i < volumeUsages.size(); i++) {
      VolumeFixedUsage vfu = volumeUsages.get(i);
      HddsVolume vol = vfu.getVolume();
      SpaceUsageSource.Fixed usage = vfu.getUsage();
      long usableSpace = vfu.computeUsableSpace();
      LOG.debug("Volume[{}] - disk={}, utilization={}, capacity={}, "
              + "effectiveUsed={}, available={}, usableSpace={}, committedBytes={}, delta={}",
          i, vol.getStorageDir().getPath(), String.format("%.10f", vfu.getUtilization()),
          usage.getCapacity(), vfu.getEffectiveUsed(), usage.getAvailable(),
          usableSpace, vol.getCommittedBytes(), deltaMap.getOrDefault(vol, 0L));
    }
  }
}
