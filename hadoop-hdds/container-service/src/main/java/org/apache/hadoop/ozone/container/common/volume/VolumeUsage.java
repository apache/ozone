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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.fs.CachingSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckParams;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED_PERCENT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED_PERCENT_DEFAULT;

/**
 * Class that wraps the space df of the Datanode Volumes used by SCM
 * containers.
 */
public class VolumeUsage {

  private final CachingSpaceUsageSource source;
  private boolean shutdownComplete;
  private final long reservedInBytes;

  private static final Logger LOG = LoggerFactory.getLogger(VolumeUsage.class);

  VolumeUsage(SpaceUsageCheckParams checkParams, ConfigurationSource conf) {
    source = new CachingSpaceUsageSource(checkParams);
    reservedInBytes = getReserved(conf, checkParams.getPath(), source.getCapacity());
    Preconditions.assertTrue(reservedInBytes >= 0, reservedInBytes + " < 0");
    start(); // TODO should start only on demand
  }

  @VisibleForTesting
  SpaceUsageSource realUsage() {
    return source.snapshot();
  }

  public long getCapacity() {
    return getCurrentUsage().getCapacity();
  }

  public long getAvailable() {
    return getCurrentUsage().getAvailable();
  }

  public long getUsedSpace() {
    return getCurrentUsage().getUsedSpace();
  }

  /**
   * Calculate available space use method B.
   * |----used----|   (avail)   |++++++++reserved++++++++|
   *              |     fsAvail      |-------other-------|
   *                          ->|~~~~|<-
   *                      remainingReserved
   * B) avail = fsAvail - Max(reserved - other, 0);
   */
  public SpaceUsageSource getCurrentUsage() {
    SpaceUsageSource real = realUsage();

    return reservedInBytes == 0
        ? real
        : new SpaceUsageSource.Fixed(
            Math.max(real.getCapacity() - reservedInBytes, 0),
            Math.max(real.getAvailable() - getRemainingReserved(real), 0),
            real.getUsedSpace());
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
  private static long getOtherUsed(SpaceUsageSource precomputedVolumeSpace) {
    long totalUsed = precomputedVolumeSpace.getCapacity() -
        precomputedVolumeSpace.getAvailable();
    return Math.max(totalUsed - precomputedVolumeSpace.getUsedSpace(), 0L);
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

  public long getReservedBytes() {
    return reservedInBytes;
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

  private static long getReserved(ConfigurationSource conf, String rootDir,
      long capacity) {
    if (conf.isConfigured(HDDS_DATANODE_DIR_DU_RESERVED_PERCENT)
        && conf.isConfigured(HDDS_DATANODE_DIR_DU_RESERVED)) {
      LOG.error("Both {} and {} are set. Set either one, not both. If the " +
          "volume matches with volume parameter in former config, it is set " +
          "as reserved space. If not it fall backs to the latter config.",
          HDDS_DATANODE_DIR_DU_RESERVED, HDDS_DATANODE_DIR_DU_RESERVED_PERCENT);
    }

    // 1. If hdds.datanode.dir.du.reserved is set for a volume then make it
    // as the reserved bytes.
    Collection<String> reserveList = conf.getTrimmedStringCollection(
        HDDS_DATANODE_DIR_DU_RESERVED);
    for (String reserve : reserveList) {
      String[] words = reserve.split(":");
      if (words.length < 2) {
        LOG.error("Reserved space should be configured in a pair, but current value is {}",
            reserve);
        continue;
      }

      try {
        String path = new File(words[0]).getCanonicalPath();
        if (path.equals(rootDir)) {
          StorageSize size = StorageSize.parse(words[1].trim());
          return (long) size.getUnit().toBytes(size.getValue());
        }
      } catch (IllegalArgumentException e) {
        LOG.error("Failed to parse StorageSize {} from config {}", words[1].trim(), HDDS_DATANODE_DIR_DU_RESERVED, e);
      } catch (IOException e) {
        LOG.error("Failed to read storage path {} from config {}", words[1].trim(), HDDS_DATANODE_DIR_DU_RESERVED, e);
      }
    }

    // 2. If hdds.datanode.dir.du.reserved not set then fall back to hdds.datanode.dir.du.reserved.percent, using
    // either its set value or default value if it has not been set.
    float percentage = conf.getFloat(HDDS_DATANODE_DIR_DU_RESERVED_PERCENT,
        HDDS_DATANODE_DIR_DU_RESERVED_PERCENT_DEFAULT);
    if (percentage < 0 || percentage > 1) {
      LOG.error("The value of {} should be between 0 to 1. Falling back to default value {}",
          HDDS_DATANODE_DIR_DU_RESERVED_PERCENT, HDDS_DATANODE_DIR_DU_RESERVED_PERCENT_DEFAULT);
      percentage = HDDS_DATANODE_DIR_DU_RESERVED_PERCENT_DEFAULT;
    }

    return (long) Math.ceil(capacity * percentage);
  }
}
