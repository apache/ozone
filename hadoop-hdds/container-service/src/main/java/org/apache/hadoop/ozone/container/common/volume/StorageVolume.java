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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.checker.Checkable;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.util.DiskChecker;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * StorageVolume represents a generic Volume in datanode, could be
 * 1. HddsVolume for container storage.
 * 2. MetadataVolume for metadata(ratis) storage.
 */
public abstract class StorageVolume
    implements Checkable<Boolean, VolumeCheckResult> {

  /**
   * Type for StorageVolume.
   */
  public enum VolumeType {
    DATA_VOLUME,
    META_VOLUME
  }

  private final File storageDir;

  private final VolumeInfo volumeInfo;

  private final VolumeSet volumeSet;

  protected StorageVolume(Builder<?> b) throws IOException {
    if (!b.failedVolume) {
      StorageLocation location = StorageLocation.parse(b.volumeRootStr);
      storageDir = new File(location.getUri().getPath(), b.storageDirStr);
      this.volumeInfo = new VolumeInfo.Builder(b.volumeRootStr, b.conf)
          .storageType(b.storageType)
          .usageCheckFactory(b.usageCheckFactory)
          .build();
      this.volumeSet = b.volumeSet;
    } else {
      storageDir = new File(b.volumeRootStr);
      this.volumeInfo = null;
      this.volumeSet = null;
    }
  }

  /**
   * Builder class for StorageVolume.
   * @param <T> subclass Builder
   */
  public abstract static class Builder<T extends Builder<T>> {
    private final String volumeRootStr;
    private String storageDirStr;
    private ConfigurationSource conf;
    private StorageType storageType;
    private SpaceUsageCheckFactory usageCheckFactory;
    private VolumeSet volumeSet;
    private boolean failedVolume = false;

    public Builder(String volumeRootStr, String storageDirStr) {
      this.volumeRootStr = volumeRootStr;
      this.storageDirStr = storageDirStr;
    }

    public abstract T getThis();

    public T conf(ConfigurationSource config) {
      this.conf = config;
      return this.getThis();
    }

    public T storageType(StorageType st) {
      this.storageType = st;
      return this.getThis();
    }

    public T usageCheckFactory(SpaceUsageCheckFactory factory) {
      this.usageCheckFactory = factory;
      return this.getThis();
    }

    public T volumeSet(VolumeSet volSet) {
      this.volumeSet = volSet;
      return this.getThis();
    }

    // This is added just to create failed volume objects, which will be used
    // to create failed StorageVolume objects in the case of any exceptions
    // caused during creating StorageVolume object.
    public T failedVolume(boolean failed) {
      this.failedVolume = failed;
      return this.getThis();
    }

    public abstract StorageVolume build() throws IOException;

    public String getVolumeRootStr() {
      return this.volumeRootStr;
    }

    public boolean getFailedVolume() {
      return this.failedVolume;
    }

    public StorageType getStorageType() {
      return this.storageType;
    }
  }

  public long getCapacity() {
    return volumeInfo != null ? volumeInfo.getCapacity() : 0;
  }

  public long getAvailable() {
    return volumeInfo != null ? volumeInfo.getAvailable() : 0;
  }

  public long getUsedSpace() {
    return volumeInfo != null ? volumeInfo.getScmUsed() : 0;
  }

  public File getStorageDir() {
    return this.storageDir;
  }

  public void refreshVolumeInfo() {
    volumeInfo.refreshNow();
  }

  public VolumeInfo getVolumeInfo() {
    return this.volumeInfo;
  }

  public VolumeSet getVolumeSet() {
    return this.volumeSet;
  }

  public StorageType getStorageType() {
    if(this.volumeInfo != null) {
      return this.volumeInfo.getStorageType();
    }
    return StorageType.DEFAULT;
  }

  public String getStorageID() {
    return "";
  }

  public void failVolume() {
    if (volumeInfo != null) {
      volumeInfo.shutdownUsageThread();
    }
  }

  public void shutdown() {
    if (volumeInfo != null) {
      volumeInfo.shutdownUsageThread();
    }
  }

  /**
   * Run a check on the current volume to determine if it is healthy.
   * @param unused context for the check, ignored.
   * @return result of checking the volume.
   * @throws Exception if an exception was encountered while running
   *            the volume check.
   */
  @Override
  public VolumeCheckResult check(@Nullable Boolean unused) throws Exception {
    if (!storageDir.exists()) {
      return VolumeCheckResult.FAILED;
    }
    DiskChecker.checkDir(storageDir);
    return VolumeCheckResult.HEALTHY;
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageDir);
  }

  @Override
  public boolean equals(Object other) {
    return this == other
        || other instanceof StorageVolume
        && ((StorageVolume) other).storageDir.equals(this.storageDir);
  }

  @Override
  public String toString() {
    return getStorageDir().toString();
  }
}
