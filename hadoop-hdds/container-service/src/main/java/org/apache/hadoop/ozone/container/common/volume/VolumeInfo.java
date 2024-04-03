/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckParams;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
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
     Dn's will send close container action to SCM at this limit & it is
     configurable.

 *
 *
 * |----used----|   (avail)   |++mvfs++|++++reserved+++++++|
 * |<-     capacity                  ->|
 *              |     fsAvail      |-------other-----------|
 * |<-                   fsCapacity                      ->|
 *
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
 *
 *  |----used----|             (avail)/fsAvail              |
 *  |<-              capacity/fsCapacity                  ->|
 *
 *  We have avail == fsAvail.
 */
public final class VolumeInfo {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeInfo.class);

  private final String rootDir;
  private final StorageType storageType;

  // Space usage calculator
  private final VolumeUsage usage;

  /**
   * Builder for VolumeInfo.
   */
  public static class Builder {
    private final ConfigurationSource conf;
    private final String rootDir;
    private SpaceUsageCheckFactory usageCheckFactory;
    private StorageType storageType;

    public Builder(String root, ConfigurationSource config) {
      this.rootDir = root;
      this.conf = config;
    }

    public Builder storageType(StorageType st) {
      this.storageType = st;
      return this;
    }

    public Builder usageCheckFactory(SpaceUsageCheckFactory factory) {
      this.usageCheckFactory = factory;
      return this;
    }

    public VolumeInfo build() throws IOException {
      return new VolumeInfo(this);
    }
  }

  private VolumeInfo(Builder b) throws IOException {

    this.rootDir = b.rootDir;
    File root = new File(this.rootDir);

    boolean succeeded = root.isDirectory() || root.mkdirs();

    if (!succeeded) {
      LOG.error("Unable to create the volume root dir at : {}", root);
      throw new IOException("Unable to create the volume root dir at " + root);
    }

    this.storageType = (b.storageType != null ?
        b.storageType : StorageType.DEFAULT);

    SpaceUsageCheckFactory usageCheckFactory = b.usageCheckFactory;
    if (usageCheckFactory == null) {
      usageCheckFactory = SpaceUsageCheckFactory.create(b.conf);
    }
    SpaceUsageCheckParams checkParams =
        usageCheckFactory.paramsFor(root);

    usage = new VolumeUsage(checkParams, b.conf);
  }

  public long getCapacity() {
    return usage.getCapacity();
  }

  /**
   * Calculate available space use method A.
   * |----used----|   (avail)   |++++++++reserved++++++++|
   * |<-     capacity         ->|
   *
   * A) avail = capacity - used
   */
  public long getAvailable() {
    return usage.getAvailable();
  }

  public SpaceUsageSource getCurrentUsage() {
    return usage.getCurrentUsage();
  }

  public void incrementUsedSpace(long usedSpace) {
    usage.incrementUsedSpace(usedSpace);
  }

  public void decrementUsedSpace(long reclaimedSpace) {
    usage.decrementUsedSpace(reclaimedSpace);
  }

  public void refreshNow() {
    usage.refreshNow();
  }

  public long getScmUsed() {
    return usage.getUsedSpace();
  }

  void shutdownUsageThread() {
    usage.shutdown();
  }

  public String getRootDir() {
    return this.rootDir;
  }

  public StorageType getStorageType() {
    return this.storageType;
  }

  /**
   * Only for testing. Do not use otherwise.
   */
  @VisibleForTesting
  public VolumeUsage getUsageForTesting() {
    return usage;
  }

  public long getReservedInBytes() {
    return usage.getReservedBytes();
  }
}
