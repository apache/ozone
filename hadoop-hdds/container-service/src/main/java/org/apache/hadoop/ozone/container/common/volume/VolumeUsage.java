/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.fs.CachingSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckParams;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT;

/**
 * Class that wraps the space df of the Datanode Volumes used by SCM
 * containers.
 */
public class VolumeUsage implements SpaceUsageSource {

  private final CachingSpaceUsageSource source;
  private boolean shutdownComplete;
  private long reservedInBytes;

  private static final Logger LOG = LoggerFactory.getLogger(VolumeUsage.class);

  VolumeUsage(SpaceUsageCheckParams checkParams) {
    source = new CachingSpaceUsageSource(checkParams);
    start(); // TODO should start only on demand
  }

  @Override
  public long getCapacity() {
    return Math.max(source.getCapacity(), 0);
  }

  /**
   * Calculate available space use method B.
   * |----used----|   (avail)   |++++++++reserved++++++++|
   *              |     fsAvail      |-------other-------|
   *                          ->|~~~~|<-
   *                      remainingReserved
   * B) avail = fsAvail - Max(reserved - other, 0);
   */
  @Override
  public long getAvailable() {
    return source.getAvailable() - getRemainingReserved();
  }

  public long getAvailable(SpaceUsageSource precomputedVolumeSpace) {
    long available = precomputedVolumeSpace.getAvailable();
    return available - getRemainingReserved(precomputedVolumeSpace);
  }

  @Override
  public long getUsedSpace() {
    return source.getUsedSpace();
  }

  @Override
  public SpaceUsageSource snapshot() {
    return source.snapshot();
  }

  public void incrementUsedSpace(long usedSpace) {
    source.incrementUsedSpace(usedSpace);
  }

  public void decrementUsedSpace(long reclaimedSpace) {
    source.decrementUsedSpace(reclaimedSpace);
  }

  /**
   * Get the space used by others except hdds.
   * DU is refreshed periodically and could be not exact,
   * so there could be that DU value > totalUsed when there are deletes.
   * @return other used space
   */
  private long getOtherUsed() {
    long totalUsed = source.getCapacity() - source.getAvailable();
    return Math.max(totalUsed - source.getUsedSpace(), 0L);
  }

  private long getOtherUsed(SpaceUsageSource precomputedVolumeSpace) {
    long totalUsed = precomputedVolumeSpace.getCapacity() -
        precomputedVolumeSpace.getAvailable();
    return Math.max(totalUsed - source.getUsedSpace(), 0L);
  }

  private long getRemainingReserved() {
    return Math.max(reservedInBytes - getOtherUsed(), 0L);
  }

  private long getRemainingReserved(
      SpaceUsageSource precomputedVolumeSpace) {
    return Math.max(reservedInBytes - getOtherUsed(precomputedVolumeSpace), 0L);
  }

  public synchronized void start() {
    source.start();
  }

  public synchronized void shutdown() {
    if (!shutdownComplete) {
      source.shutdown();
      shutdownComplete = true;
    }
  }

  public void refreshNow() {
    source.refreshNow();
  }

  public void setReserved(long reserved) {
    this.reservedInBytes = reserved;
  }

  /**
   * If 'hdds.datanode.volume.min.free.space' is defined,
   * it will be honored first. If it is not defined and
   * 'hdds.datanode.volume.min.free.space.' is defined,it will honor this
   * else it will fall back to 'hdds.datanode.volume.min.free.space.default'
   */
  public static long getMinVolumeFreeSpace(ConfigurationSource conf,
      long capacity) {
    if (conf.isConfigured(
        HDDS_DATANODE_VOLUME_MIN_FREE_SPACE) && conf.isConfigured(
        HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT)) {
      LOG.error(
          "Both {} and {} are set. Set either one, not both. If both are set,"
              + "it will use default value which is {} as min free space",
          HDDS_DATANODE_VOLUME_MIN_FREE_SPACE,
          HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT,
          HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_DEFAULT);
    }

    if (conf.isConfigured(HDDS_DATANODE_VOLUME_MIN_FREE_SPACE)) {
      return (long) conf.getStorageSize(HDDS_DATANODE_VOLUME_MIN_FREE_SPACE,
          HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_DEFAULT, StorageUnit.BYTES);
    } else if (conf.isConfigured(HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT)) {
      float volumeMinFreeSpacePercent = Float.parseFloat(
          conf.get(HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT));
      return (long) (capacity * volumeMinFreeSpacePercent);
    }
    // either properties are not configured,then return
    // HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_DEFAULT,
    return (long) conf.getStorageSize(HDDS_DATANODE_VOLUME_MIN_FREE_SPACE,
        HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_DEFAULT, StorageUnit.BYTES);

  }

  public static boolean hasVolumeEnoughSpace(long volumeAvailableSpace,
                                             long volumeCommittedBytesCount,
                                             long requiredSpace,
                                             long volumeFreeSpaceToSpare) {
    return (volumeAvailableSpace - volumeCommittedBytesCount) >
        Math.max(requiredSpace, volumeFreeSpaceToSpare);
  }
}
