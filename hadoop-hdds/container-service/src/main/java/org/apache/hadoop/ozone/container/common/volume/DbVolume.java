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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

  private String clusterID;

  /**
   * Records all HddsVolumes that put its db instance under this DbVolume.
   */
  private final List<String> hddsVolumeIDs;

  protected DbVolume(Builder b) throws IOException {
    super(b);
    this.clusterID = b.getClusterID();
    this.hddsVolumeIDs = new ArrayList<>();
    if (!b.getFailedVolume()) {
      initialize();
    }
  }

  public boolean format(String cid) {
    Preconditions.checkNotNull(cid, "clusterID cannot be null while " +
        "formatting db volume");
    this.clusterID = cid;

    // create clusterID dir /ssd1/db/<CID-clusterID>
    File volumeRootDir = getStorageDir();
    File clusterIdDir = new File(volumeRootDir, clusterID);
    if (!clusterIdDir.mkdirs() && !clusterIdDir.exists()) {
      LOG.error("Unable to create ID directory {} for db volume {}",
          clusterIdDir, volumeRootDir);
      return false;
    }
    return true;
  }

  public boolean initialize() {
    // This should be on a test path, normally we should get
    // the clusterID from SCM, and it should not be available
    // while restarting.
    if (clusterID != null) {
      return format(clusterID);
    }

    if (!getStorageDir().exists()) {
      // Not formatted yet
      return true;
    }

    File[] storageDirs = getStorageDir().listFiles(File::isDirectory);
    if (storageDirs == null) {
      LOG.error("IO error for the db volume {}, skipped loading",
          getStorageDir());
      return false;
    }

    if (storageDirs.length == 0) {
      // Not formatted completely
      return true;
    }

    if (storageDirs.length > 1) {
      LOG.error("DB volume {} is in an Inconsistent state", getStorageDir());
      return false;
    }

    // scan subdirectories for db instances mapped to HddsVolumes
    File clusterIdDir = storageDirs[0];
    File[] subdirs = clusterIdDir.listFiles(File::isDirectory);
    // Not used yet
    if (subdirs == null) {
      return true;
    }

    for (File subdir : subdirs) {
      String storageID = subdir.getName();
      hddsVolumeIDs.add(storageID);
    }

    return true;
  }

  public List<String> getHddsVolumeIDs() {
    return this.hddsVolumeIDs;
  }

  /**
   * Builder class for DbVolume.
   */
  public static class Builder extends StorageVolume.Builder<Builder> {

    private String clusterID;

    public Builder(String volumeRootStr) {
      super(volumeRootStr, DB_VOLUME_DIR);
    }

    @Override
    public Builder getThis() {
      return this;
    }

    public Builder clusterID(String cid) {
      this.clusterID = cid;
      return this;
    }

    public DbVolume build() throws IOException {
      return new DbVolume(this);
    }

    public String getClusterID() {
      return this.clusterID;
    }
  }
}
