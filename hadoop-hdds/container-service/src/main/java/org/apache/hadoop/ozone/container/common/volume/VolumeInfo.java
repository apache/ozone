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
import java.util.Collection;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckParams;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED;

/**
 * Stores information about a disk/volume.
 */
public final class VolumeInfo {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeInfo.class);

  private final String rootDir;
  private final StorageType storageType;

  // Space usage calculator
  private final VolumeUsage usage;

  // Capacity configured. This is useful when we want to
  // limit the visible capacity for tests. If negative, then we just
  // query from the filesystem.
  private long configuredCapacity;

  private long reservedInBytes;

  /**
   * Builder for VolumeInfo.
   */
  public static class Builder {
    private final ConfigurationSource conf;
    private final String rootDir;
    private SpaceUsageCheckFactory usageCheckFactory;
    private StorageType storageType;
    private long configuredCapacity;

    public Builder(String root, ConfigurationSource config) {
      this.rootDir = root;
      this.conf = config;
    }

    public Builder storageType(StorageType st) {
      this.storageType = st;
      return this;
    }

    public Builder configuredCapacity(long capacity) {
      this.configuredCapacity = capacity;
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

  private long getReserved(ConfigurationSource conf) {
    Collection<String> reserveList = conf.getTrimmedStringCollection(
        HDDS_DATANODE_DIR_DU_RESERVED);
    for (String reserve : reserveList) {
      String[] words = reserve.split(":");
      if (words.length < 2) {
        LOG.error("Reserved space should config in pair, but current is {}",
            reserve);
        continue;
      }

      if (words[0].trim().equals(rootDir)) {
        try {
          StorageSize size = StorageSize.parse(words[1].trim());
          return (long) size.getUnit().toBytes(size.getValue());
        } catch (Exception e) {
          LOG.error("Failed to parse StorageSize:{}", words[1].trim(), e);
          return 0;
        }
      }
    }

    return 0;
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

    this.configuredCapacity = (b.configuredCapacity != 0 ?
        b.configuredCapacity : -1);

    SpaceUsageCheckFactory usageCheckFactory = b.usageCheckFactory;
    if (usageCheckFactory == null) {
      usageCheckFactory = SpaceUsageCheckFactory.create(b.conf);
    }
    SpaceUsageCheckParams checkParams =
        usageCheckFactory.paramsFor(root);

    this.reservedInBytes = getReserved(b.conf);
    this.usage = new VolumeUsage(checkParams);
  }

  public long getCapacity() {
    if (configuredCapacity < 0) {
      return Math.max(usage.getCapacity() - reservedInBytes, 0);
    }
    return configuredCapacity;
  }

  public long getAvailable() {
    return Math.max(usage.getAvailable() - reservedInBytes, 0);
  }

  public long getScmUsed() {
    return usage.getUsedSpace();
  }

  void start() {
    usage.start();
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
}
