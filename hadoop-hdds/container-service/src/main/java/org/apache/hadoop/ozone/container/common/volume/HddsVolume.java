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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.utils.RawDB;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures.SchemaV3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_NAME;
import static org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil.initPerDiskDBStore;

/**
 * HddsVolume represents volume in a datanode. {@link MutableVolumeSet}
 * maintains a list of HddsVolumes, one for each volume in the Datanode.
 * {@link VolumeInfo} in encompassed by this class.
 * <p>
 * The disk layout per volume is as follows:
 * <p>../hdds/VERSION
 * <p>{@literal ../hdds/<<clusterUuid>>/current/<<containerDir>>/<<containerID
 * >>/metadata}
 * <p>{@literal ../hdds/<<clusterUuid>>/current/<<containerDir>>/<<containerID
 * >>/<<dataDir>>}
 * <p>
 * Each hdds volume has its own VERSION file. The hdds volume will have one
 * clusterUuid directory for each SCM it is a part of (currently only one SCM is
 * supported).
 * <p>
 * During DN startup, if the VERSION file exists, we verify that the
 * clusterID in the version file matches the clusterID from SCM.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@SuppressWarnings("finalclass")
public class HddsVolume extends StorageVolume {

  private static final Logger LOG = LoggerFactory.getLogger(HddsVolume.class);

  public static final String HDDS_VOLUME_DIR = "hdds";
  public static final String TMP_CONTAINER_DELETE_DIR_NAME =
      "deleted-containers";

  private final VolumeIOStats volumeIOStats;
  private final VolumeInfoMetrics volumeInfoMetrics;

  private final AtomicLong committedBytes; // till Open containers become full

  // Mentions the type of volume
  private final VolumeType type = VolumeType.DATA_VOLUME;
  // The dedicated DbVolume that the db instance of this HddsVolume resides.
  // This is optional, if null then the db instance resides on this HddsVolume.
  private DbVolume dbVolume;
  // The subdirectory with storageID as its name, used to build the
  // container db path. This is initialized only once together with dbVolume,
  // and stored as a member to prevent spawning lots of File objects.
  private File dbParentDir;
  private File deletedContainerDir;
  private AtomicBoolean dbLoaded = new AtomicBoolean(false);

  /**
   * Builder for HddsVolume.
   */
  public static class Builder extends StorageVolume.Builder<Builder> {

    public Builder(String volumeRootStr) {
      super(volumeRootStr, HDDS_VOLUME_DIR);
    }

    @Override
    public Builder getThis() {
      return this;
    }

    public HddsVolume build() throws IOException {
      return new HddsVolume(this);
    }
  }

  private HddsVolume(Builder b) throws IOException {
    super(b);

    if (!b.getFailedVolume() && getVolumeInfo().isPresent()) {
      this.setState(VolumeState.NOT_INITIALIZED);
      this.volumeIOStats = new VolumeIOStats(b.getVolumeRootStr(),
          this.getStorageDir().toString());
      this.volumeInfoMetrics =
          new VolumeInfoMetrics(b.getVolumeRootStr(), this);
      this.committedBytes = new AtomicLong(0);

      LOG.info("Creating HddsVolume: {} of storage type : {} capacity : {}",
          getStorageDir(), b.getStorageType(),
              getVolumeInfo().get().getCapacity());

      initialize();
    } else {
      // Builder is called with failedVolume set, so create a failed volume
      // HddsVolume Object.
      this.setState(VolumeState.FAILED);
      volumeIOStats = null;
      volumeInfoMetrics = new VolumeInfoMetrics(b.getVolumeRootStr(), this);
      committedBytes = null;
    }

  }

  @Override
  public void createWorkingDir(String dirName, MutableVolumeSet dbVolumeSet)
      throws IOException {
    super.createWorkingDir(dirName, dbVolumeSet);

    // Create DB store for a newly formatted volume
    if (VersionedDatanodeFeatures.isFinalized(
        HDDSLayoutFeature.DATANODE_SCHEMA_V3)) {
      createDbStore(dbVolumeSet);
    }
  }

  @Override
  public void createTmpDirs(String workDirName) throws IOException {
    super.createTmpDirs(workDirName);
    deletedContainerDir =
        createTmpSubdirIfNeeded(TMP_CONTAINER_DELETE_DIR_NAME);
    cleanDeletedContainerDir();
  }

  public File getHddsRootDir() {
    return super.getStorageDir();
  }

  public VolumeType getType() {
    return type;
  }

  public VolumeIOStats getVolumeIOStats() {
    return volumeIOStats;
  }

  public VolumeInfoMetrics getVolumeInfoStats() {
    return volumeInfoMetrics;
  }

  @Override
  public void failVolume() {
    super.failVolume();
    if (volumeIOStats != null) {
      volumeIOStats.unregister();
    }
    closeDbStore();
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (volumeIOStats != null) {
      volumeIOStats.unregister();
    }
    if (volumeInfoMetrics != null) {
      volumeInfoMetrics.unregister();
    }
    closeDbStore();
    cleanDeletedContainerDir();
  }

  /**
   * Delete all files under
   * <volume>/hdds/<cluster-id>/tmp/deleted-containers.
   * This is the directory where containers are moved when they are deleted
   * from the system, but before being removed from the filesystem. This
   * makes the deletion atomic.
   */
  public void cleanDeletedContainerDir() {
    // If the volume was shut down before initialization completed, skip
    // emptying the directory.
    if (deletedContainerDir == null) {
      return;
    }

    if (!deletedContainerDir.exists()) {
      LOG.warn("Unable to clear deleted containers from {}. Directory does " +
          "not exist.", deletedContainerDir);
      return;
    }

    if (!deletedContainerDir.isDirectory()) {
      LOG.warn("Unable to clear deleted containers from {}. Location is not a" +
          " directory", deletedContainerDir);
      return;
    }

    File[] containerDirs = deletedContainerDir.listFiles(File::isDirectory);
    if (containerDirs == null) {
      // Either directory does not exist or IO error. Either way we cannot
      // proceed with deletion.
      LOG.warn("Failed to clear container delete directory {}. Directory " +
          "could not be accessed.", deletedContainerDir);
      return;
    }

    for (File containerDir: containerDirs) {
      // Check the case where we have Schema V3 and
      // removing the container's entries from RocksDB fails.
      // --------------------------------------------
      // On datanode restart, we populate the container set
      // based on the available datanode volumes and
      // populate the container metadata based on the values in RocksDB.
      // The container is in the tmp directory,
      // so it won't be loaded in the container set
      // but there will be orphaned entries in the volume's RocksDB.
      // --------------------------------------------
      // For every .container file we find under /tmp,
      // we use it to get the RocksDB entries and delete them.
      // If the .container file doesn't exist then the contents of the
      // directory are probably leftovers of a failed delete and
      // the RocksDB entries must have already been removed.
      // In any case we can proceed with deleting the directory's contents.
      // --------------------------------------------
      // Get container file and check Schema version. If Schema is V3
      // then remove the container from RocksDB.
      File containerFile = ContainerUtils.getContainerFile(containerDir);

      if (containerFile.exists()) {
        KeyValueContainerData keyValueContainerData;
        try {
          ContainerData containerData =
              ContainerDataYaml.readContainerFile(containerFile);
          keyValueContainerData = (KeyValueContainerData) containerData;
        } catch (IOException ex) {
          LOG.warn("Failed to read container file {}. Container cannot be " +
              "removed from the delete directory.", containerFile, ex);
          continue;
        }

        if (keyValueContainerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
          // Container file doesn't include the volume
          // so we need to set it here in order to get the DB location.
          keyValueContainerData.setVolume(this);
          File dbFile = KeyValueContainerLocationUtil
              .getContainerDBFile(keyValueContainerData);
          keyValueContainerData.setDbFile(dbFile);
          try {
            // Remove container from Rocks DB
            BlockUtils.removeContainerFromDB(keyValueContainerData, getConf());
          } catch (IOException ex) {
            LOG.warn("Failed to remove container data from DB while" +
                    "deleting container {}. Container cannot be removed from" +
                    "the delete directory {}.",
                keyValueContainerData.getContainerID(),
                deletedContainerDir, ex);
            continue;
          }
        }
      }

      // If the container file was already deleted, the RocksDB entries were
      // cleared.
      try {
        if (containerDir.isDirectory()) {
          FileUtils.deleteDirectory(containerDir);
        } else {
          FileUtils.delete(containerDir);
        }
      } catch (IOException ex) {
        LOG.warn("Failed to remove container directory {}.",
            deletedContainerDir, ex);
      }
    }
  }

  @Override
  public VolumeCheckResult check(@Nullable Boolean unused) throws Exception {
    VolumeCheckResult result = super.check(unused);
    if (!isDbLoaded()) {
      return result;
    }
    DatanodeConfiguration df = getConf().getObject(DatanodeConfiguration.class);
    if (result != VolumeCheckResult.HEALTHY ||
        !df.getContainerSchemaV3Enabled() || !df.autoCompactionSmallSstFile()) {
      return result;
    }
    // Calculate number of files per level and size per level
    RawDB rawDB = DatanodeStoreCache.getInstance().getDB(
        new File(dbParentDir, CONTAINER_DB_NAME).getAbsolutePath(), getConf());
    rawDB.getStore().compactionIfNeeded();

    return VolumeCheckResult.HEALTHY;
  }

  /**
   * add "delta" bytes to committed space in the volume.
   *
   * @param delta bytes to add to committed space counter
   * @return bytes of committed space
   */
  public long incCommittedBytes(long delta) {
    return committedBytes.addAndGet(delta);
  }

  /**
   * return the committed space in the volume.
   *
   * @return bytes of committed space
   */
  public long getCommittedBytes() {
    return committedBytes.get();
  }

  public void setDbVolume(DbVolume dbVolume) {
    this.dbVolume = dbVolume;
  }

  public DbVolume getDbVolume() {
    return this.dbVolume;
  }

  public File getDbParentDir() {
    return this.dbParentDir;
  }

  public File getDeletedContainerDir() {
    return this.deletedContainerDir;
  }

  public boolean isDbLoaded() {
    return dbLoaded.get();
  }

  public void loadDbStore(boolean readOnly) throws IOException {
    // DN startup for the first time, not registered yet,
    // so the DbVolume is not formatted.
    if (!getStorageState().equals(VolumeState.NORMAL)) {
      return;
    }

    // DB is already loaded
    if (dbLoaded.get()) {
      LOG.warn("Schema V3 db is already loaded from {} for volume {}",
          getDbParentDir(), getStorageID());
      return;
    }

    File clusterIdDir = new File(dbVolume == null ?
        getStorageDir() : dbVolume.getStorageDir(),
        getClusterID());
    if (!clusterIdDir.exists()) {
      throw new IOException("Working dir " + clusterIdDir.getAbsolutePath() +
          " not created for HddsVolume: " + getStorageDir().getAbsolutePath());
    }

    File storageIdDir = new File(clusterIdDir, getStorageID());
    if (!storageIdDir.exists()) {
      throw new IOException("Db parent dir " + storageIdDir.getAbsolutePath() +
          " not found for HddsVolume: " + getStorageDir().getAbsolutePath());
    }

    File containerDBFile = new File(storageIdDir, CONTAINER_DB_NAME);
    if (!containerDBFile.exists()) {
      throw new IOException("Db dir " + storageIdDir.getAbsolutePath() +
          " not found for HddsVolume: " + getStorageDir().getAbsolutePath());
    }

    String containerDBPath = containerDBFile.getAbsolutePath();
    try {
      initPerDiskDBStore(containerDBPath, getConf(), readOnly);
    } catch (IOException e) {
      throw new IOException("Can't init db instance under path "
          + containerDBPath + " for volume " + getStorageID(), e);
    }

    dbParentDir = storageIdDir;
    dbLoaded.set(true);
    LOG.info("SchemaV3 db is loaded at {} for volume {}", containerDBPath,
        getStorageID());
  }

  /**
   * Pick a DbVolume for HddsVolume and init db instance.
   * Use the HddsVolume directly if no DbVolume found.
   * @param dbVolumeSet
   */
  public void createDbStore(MutableVolumeSet dbVolumeSet) throws IOException {
    DbVolume chosenDbVolume = null;
    File clusterIdDir;
    String workingDirName = getWorkingDirName() == null ? getClusterID() :
        getWorkingDirName();

    if (dbVolumeSet == null || dbVolumeSet.getVolumesList().isEmpty()) {
      // No extra db volumes specified, just create db under the HddsVolume.
      clusterIdDir = new File(getStorageDir(), workingDirName);
    } else {
      // Randomly choose a DbVolume for simplicity.
      List<DbVolume> dbVolumeList = StorageVolumeUtil.getDbVolumesList(
          dbVolumeSet.getVolumesList());
      chosenDbVolume = dbVolumeList.get(
          ThreadLocalRandom.current().nextInt(dbVolumeList.size()));
      clusterIdDir = new File(chosenDbVolume.getStorageDir(), workingDirName);
    }

    if (!clusterIdDir.exists()) {
      throw new IOException("The working dir "
          + clusterIdDir.getAbsolutePath() + " is missing for volume "
          + getStorageID());
    }

    // Init subdir with the storageID of HddsVolume.
    File storageIdDir = new File(clusterIdDir, getStorageID());
    if (!storageIdDir.mkdirs() && !storageIdDir.exists()) {
      throw new IOException("Can't make subdir under "
          + clusterIdDir.getAbsolutePath() + " for volume "
          + getStorageID());
    }

    // Create the db instance for HddsVolume under the subdir above.
    String containerDBPath = new File(storageIdDir, CONTAINER_DB_NAME)
        .getAbsolutePath();
    try {
      HddsVolumeUtil.initPerDiskDBStore(containerDBPath, getConf(), false);
      dbLoaded.set(true);
      LOG.info("SchemaV3 db is created and loaded at {} for volume {}",
          containerDBPath, getStorageID());
    } catch (IOException e) {
      String errMsg = "Can't create db instance under path "
          + containerDBPath + " for volume " + getStorageID();
      LOG.error(errMsg, e);
      throw new IOException(errMsg);
    }

    // Set the dbVolume and dbParentDir of the HddsVolume for db path lookup.
    dbVolume = chosenDbVolume;
    dbParentDir = storageIdDir;
    if (chosenDbVolume != null) {
      chosenDbVolume.addHddsDbStorePath(getStorageID(), containerDBPath);
    }

    // If SchemaV3 is disabled, close the DB instance
    if (!SchemaV3.isFinalizedAndEnabled(getConf())) {
      closeDbStore();
    }
  }

  private void closeDbStore() {
    if (!dbLoaded.get()) {
      return;
    }

    String containerDBPath = new File(dbParentDir, CONTAINER_DB_NAME)
        .getAbsolutePath();
    DatanodeStoreCache.getInstance().removeDB(containerDBPath);
    dbLoaded.set(false);
    LOG.info("SchemaV3 db is stopped at {} for volume {}", containerDBPath,
        getStorageID());
  }
}
