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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED_PERCENT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED_PERCENT_DEFAULT;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.hdds.fs.CachingSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckParams;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores information about a disk/volume.
 *
 * Since we have a reserved space for each volume for other usage,
 * let's clarify the space values a bit here:
 * - used: hdds actual usage.
 * - avail: remaining space for hdds usage.
 * - reserved: total space for other usage.
 * - capacity: total space for hdds usage.
 * - other: space used by other service consuming the same volume.
 * - fsAvail: reported remaining space from local fs.
 * - fsUsed: reported total used space from local fs.
 * - fsCapacity: reported total capacity from local fs.
 * - minVolumeFreeSpace (mvfs) : determines the free space for closing
     containers.This is like adding a few reserved bytes to reserved space.
     Dn's will send close container action to SCM at this limit, and it is
     configurable.

 *
 * <pre>
 * {@code
 * |----used----|   (avail)   |++mvfs++|++++reserved+++++++|
 * |<-     capacity                  ->|
 *              |     fsAvail      |-------other-----------|
 * |<-                   fsCapacity                      ->|
 * }</pre>
 * <pre>
 * What we could directly get from local fs:
 *     fsCapacity, fsAvail, (fsUsed = fsCapacity - fsAvail)
 * We could get from config:
 *     reserved
 * Get from cmd line:
 *     used: from cmd 'du' (by default)
 * Get from calculation:
 *     capacity = fsCapacity - reserved
 *     other = fsUsed - used
 *
 * The avail is the result we want from calculation.
 * So, it could be:
 * A) avail = capacity - used
 * B) avail = fsAvail - Max(reserved - other, 0);
 *
 * To be Conservative, we could get min
 *     avail = Max(Min(A, B), 0);
 *
 * If we have a dedicated disk for hdds and are not using the reserved space,
 * then we should use DedicatedDiskSpaceUsage for
 * `hdds.datanode.du.factory.classname`,
 * Then it is much simpler, since we don't care about other usage:
 * {@code
 *  |----used----|             (avail)/fsAvail              |
 *  |<-              capacity/fsCapacity                  ->|
 * }
 *
 *  We have avail == fsAvail.
 *  </pre>
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
  }

  SpaceUsageSource.Fixed realUsage() {
    return source.snapshot();
  }

  /**
   * <pre>
   * {@code
   * Calculate available space use method B.
   * |----used----|   (avail)   |++++++++reserved++++++++|
   *              |     fsAvail      |-------other-------|
   *                          ->|~~~~|<-
   *                      remainingReserved
   * }
   * </pre>
   * B) avail = fsAvail - Max(reserved - other, 0);
   */
  public SpaceUsageSource.Fixed getCurrentUsage() {
    final SpaceUsageSource.Fixed real = realUsage();
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
  static long getOtherUsed(SpaceUsageSource usage) {
    long totalUsed = usage.getCapacity() - usage.getAvailable();
    return Math.max(totalUsed - usage.getUsedSpace(), 0L);
  }

  private long getRemainingReserved(SpaceUsageSource usage) {
    return Math.max(reservedInBytes - getOtherUsed(usage), 0L);
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

  public long getReservedInBytes() {
    return reservedInBytes;
  }

  public static long getUsableSpace(long available, long committed, long spared) {
    return available - committed - spared;
  }

  public static long getUsableSpace(StorageReportProto report) {
    return getUsableSpace(report.getRemaining(), report.getCommitted(), report.getFreeSpaceToSpare());
  }

  public static long getUsableSpace(StorageLocationReport report) {
    return getUsableSpace(report.getRemaining(), report.getCommitted(), report.getFreeSpaceToSpare());
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
        throw new ConfigurationException("hdds.datanode.dir.du.reserved - " +
                "Reserved space should be configured in a pair, but current value is " + reserve);
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
