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

import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_NAME;

/**
 * DbVolume represents a volume in datanode holding db instances
 * for multiple HddsVolumes. One HddsVolume will have one subdirectory
 * for its db instance under a DbVolume.
 *
 * For example:
 *   Say we have an SSD device mounted at /ssd1, then the DbVolume
 *   root directory is /ssd1/db, and we have a subdirectory
 *   for db instance like
 *   /ssd1/db/<clusterID>/<storageID>/container.db.
 */
public class DbVolume extends StorageVolume {

  private static final Logger LOG = LoggerFactory.getLogger(DbVolume.class);

  public static final String DB_VOLUME_DIR = "db";

  /**
   * Records all HddsVolumes that put its db instance under this DbVolume.
   */
  private final Set<String> hddsVolumeIDs;

  protected DbVolume(Builder b) throws IOException {
    super(b);

    this.hddsVolumeIDs = new LinkedHashSet<>();
    if (!b.getFailedVolume()) {
      LOG.info("Creating DbVolume: {} of storage type : {} capacity : {}",
          getStorageDir(), b.getStorageType(), getVolumeInfo().getCapacity());
      initialize();
    }
  }

  @Override
  protected void initialize() throws IOException {
    super.initialize();
    scanForHddsVolumeIDs();
  }

  @Override
  public void failVolume() {
    super.failVolume();
    closeAllDbStore();
  }

  @Override
  public void shutdown() {
    super.shutdown();
    closeAllDbStore();
  }

  public void addHddsVolumeID(String id) {
    hddsVolumeIDs.add(id);
  }

  public Set<String> getHddsVolumeIDs() {
    return this.hddsVolumeIDs;
  }

  /**
   * Builder class for DbVolume.
   */
  public static class Builder extends StorageVolume.Builder<Builder> {

    public Builder(String volumeRootStr) {
      super(volumeRootStr, DB_VOLUME_DIR);
    }

    @Override
    public Builder getThis() {
      return this;
    }

    public DbVolume build() throws IOException {
      return new DbVolume(this);
    }
  }

  private void scanForHddsVolumeIDs() throws IOException {
    // Not formatted yet
    if (getClusterID() == null) {
      return;
    }

    // scan subdirectories for db instances mapped to HddsVolumes
    File clusterIdDir = new File(getStorageDir(), getClusterID());
    // Working dir not prepared yet
    if (!clusterIdDir.exists()) {
      return;
    }

    File[] subdirs = clusterIdDir.listFiles(File::isDirectory);
    if (subdirs == null) {
      throw new IOException("Failed to do listFiles for " +
          clusterIdDir.getAbsolutePath());
    }
    hddsVolumeIDs.clear();

    for (File subdir : subdirs) {
      String storageID = subdir.getName();
      hddsVolumeIDs.add(storageID);
    }
  }

  private void closeAllDbStore() {
    if (getClusterID() == null) {
      return;
    }

    File clusterIdDir = new File(getStorageDir(), getClusterID());
    if (clusterIdDir.exists()) {
      for (String storageID : hddsVolumeIDs) {
        File storageIdDir = new File(clusterIdDir, storageID);
        String containerDBPath = new File(storageIdDir, CONTAINER_DB_NAME)
            .getAbsolutePath();
        DatanodeStoreCache.getInstance().removeDB(containerDBPath);
      }
    }
  }
}
